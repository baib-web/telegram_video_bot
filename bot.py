import logging
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler
import yt_dlp
import yt_dlp.utils
import os
import asyncio
from dotenv import load_dotenv
import subprocess
import ffmpeg
import re
import json
import uuid # Import the uuid library

# --- Load environment variables ---
load_dotenv()

# --- Configure logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Get Telegram Bot Token and download directory ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DOWNLOAD_DESTINATION_DIR = os.getenv("DOWNLOAD_DESTINATION_DIR")
DELETE_DOWNLOADED_FILES_AFTER_UPLOAD = os.getenv("DELETE_DOWNLOADED_FILES_AFTER_UPLOAD", "true").lower()
SHOULD_DELETE_FILE = not (DELETE_DOWNLOADED_FILES_AFTER_UPLOAD == "false" or DELETE_DOWNLOADED_FILES_AFTER_UPLOAD == "0")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID") # New: Get channel ID

# --- Define Telegram file upload maximum limits (2GB = 2000 MB = 2,000,000,000 bytes) ---
TELEGRAM_MAX_FILE_SIZE_BYTES = 1.95 * 1000 * 1000 * 1000
# Define maximum limit for video files to be sent as video directly (50MB)
TELEGRAM_VIDEO_FILE_SIZE_LIMIT_BYTES = 50 * 1024 * 1024

# Define video parsing timeout (seconds)
VIDEO_PARSE_TIMEOUT_SECONDS = 15 # Set to 15 seconds as requested

# Check if environment variables are loaded successfully
if not TELEGRAM_BOT_TOKEN:
    logger.error("Error: TELEGRAM_BOT_TOKEN is not set. Please check your .env file or environment variables.")
    exit(1)
if not DOWNLOAD_DESTINATION_DIR:
    logger.error("Error: DOWNLOAD_DESTINATION_DIR is not set. Please check your .env file or environment variables.")
    exit(1)

# Define user data storage directory
USER_DATA_DIR = os.path.join(DOWNLOAD_DESTINATION_DIR, "user_data")
os.makedirs(USER_DATA_DIR, exist_ok=True)
logger.info(f"Ensuring user data directory '{USER_DATA_DIR}' exists.")

# Global dictionary to store user download session information, including queue and current active download
user_download_sessions = {}

# --- Persistence functions ---
def get_user_data_filepath(chat_id):
    """Gets the full path for the user's data file."""
    return os.path.join(USER_DATA_DIR, f"{chat_id}.json")

def load_user_session(chat_id):
    """Loads session data for a specific user from file."""
    filepath = get_user_data_filepath(chat_id)
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                session_data = json.load(f)
                # Ensure essential keys exist, initialize if not
                session_data.setdefault('active_download', None)
                session_data.setdefault('queue', [])
                session_data.setdefault('last_user_message_id', None)
                session_data.setdefault('selection_buttons_message_id', None)
                return session_data
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error while loading session data for user {chat_id}: {e}")
            return None # Return None if loading failed, indicating re-initialization is needed
        except Exception as e:
            logger.error(f"Error while loading session data for user {chat_id}: {e}")
            return None
    return None

def save_user_session(chat_id, session_data):
    """Saves session data for a specific user to file."""
    filepath = get_user_data_filepath(chat_id)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(session_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Error while saving session data for user {chat_id}: {e}")

# --- Helper to extract video titles ---
async def get_video_title(url):
    """Asynchronously extracts video title using yt-dlp simulate mode with a timeout."""
    ydl_opts_title = {
        'noplaylist': True,
        'restrictfilenames': True,
        'nocheckcertificate': True,
        'quiet': True,
        'no_warnings': True,
        'simulate': True,
        'forcetitle': True,
        'skip_download': True, # Ensure no download
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts_title) as ydl_title:
            info_dict = await asyncio.wait_for(
                asyncio.to_thread(ydl_title.extract_info, url, download=False),
                timeout=VIDEO_PARSE_TIMEOUT_SECONDS # Use the defined timeout
            )
            return info_dict.get('title', '[解析失败]'), None # Return title and no error
    except asyncio.TimeoutError:
        logger.warning(f"Failed to get title for {url} due to timeout ({VIDEO_PARSE_TIMEOUT_SECONDS}s)")
        return "[解析失败]", "timeout"
    except Exception as e:
        logger.warning(f"Failed to get title for {url}: {e}")
        return "[解析失败]", "failed"

# --- Function to extract thumbnail from video's first frame ---
async def extract_thumbnail(video_path, output_thumbnail_path):
    """
    Extracts the first frame of a video as a thumbnail using ffmpeg.
    Now extracts original size thumbnail.
    """
    try:
        logger.info(f"Extracting thumbnail for {video_path} to {output_thumbnail_path}")
        await asyncio.to_thread(
            ffmpeg
            .input(video_path, ss='00:00:01') # Start from 1 second
            .output(output_thumbnail_path, vframes=1, q='2') # Set quality, no forced scaling
            .run, overwrite_output=True, capture_stdout=True, capture_stderr=True
        )
        logger.info(f"Thumbnail extraction successful: {output_thumbnail_path}")
        return True
    except ffmpeg.Error as e:
        logger.error(f"FFmpeg error occurred during thumbnail extraction: {e.stderr.decode()}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unknown error occurred during thumbnail extraction: {e}", exc_info=True)
        return False

# --- yt-dlp progress hook function ---
def yt_dlp_progress_hook(d):
    """
    yt-dlp download progress hook.
    Called by yt-dlp during download.
    """
    if d['status'] == 'downloading':
        total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate')
        downloaded_bytes = d.get('downloaded_bytes')
        if total_bytes and downloaded_bytes:
            percent = downloaded_bytes / total_bytes * 100
            # logger.debug(f"Download progress: {percent:.2f}%") # Too verbose for regular logging

# --- Telegram Bot Command Handlers ---

async def start(update: Update, context):
    """Triggers when the user sends the /start command"""
    await update.message.reply_text('你好！请发送一个或多个视频链接给我，我会尝试解析并添加到队列。您可以使用 /list 查看和选择要处理的项目。')

async def list_downloads(chat_id, context, update_obj=None):
    """
    Displays current tasks and queue, and provides selection buttons.
    chat_id is now explicitly passed.
    update_obj is the original Update object (can be Message or CallbackQuery)
    """
    session = user_download_sessions.get(chat_id)
    
    # Load session data if not already loaded (e.g., bot restart)
    if not session:
        session = load_user_session(chat_id)
        if session:
            user_download_sessions[chat_id] = session
        else: # No session data found or corrupted
            user_download_sessions[chat_id] = {
                'active_download': None,
                'queue': [],
                'last_user_message_id': None,
                'selection_buttons_message_id': None
            }
            session = user_download_sessions[chat_id]
            save_user_session(chat_id, session) # Save initialized session

    # Delete previous selection buttons message if it exists
    if session['selection_buttons_message_id']:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=session['selection_buttons_message_id'])
            session['selection_buttons_message_id'] = None
            save_user_session(chat_id, session)
        except Exception as e:
            logger.warning(f"[{chat_id}] Failed to delete old selection buttons message (list_downloads): {e}")

    response_text = "当前视频处理队列：\n\n"
    keyboard = []
    
    # Prepare items for display, adding active_download at the beginning if present
    display_items = []
    
    # IMPORTANT FIX: If active_download is in a failed state (parse_failed, failed_last_attempt),
    # it means it's no longer 'active' in terms of processing, but rather awaiting user action.
    # So, we should clear active_download and ensure it's only in the queue for display/removal.
    if session.get('active_download'):
        if session['active_download']['status'] in ['parse_failed', 'failed_last_attempt']:
            # Find this item in the queue and update its status if it exists, otherwise add it.
            found_in_queue = False
            for idx, q_item in enumerate(session['queue']):
                if q_item.get('unique_id') == session['active_download'].get('unique_id'):
                    session['queue'][idx]['status'] = session['active_download']['status']
                    found_in_queue = True
                    break
            if not found_in_queue:
                session['queue'].append(session['active_download'])
            session['active_download'] = None # Clear active_download, as it's now 'managed' by the queue
            save_user_session(chat_id, session) # Save this state change
            # After this, the item will be picked up by the filtered_queue logic below.
        else: # It's genuinely active (downloading, sending, awaiting_quality_selection)
            active_item = session['active_download'].copy() # Make a copy
            display_items.append(active_item)

    # Filter out 'completed', 'cancelled', and 'permanently failed' items from the queue for display
    # The fix for "重复显示在列表" is primarily ensuring that parse_failed items that are *not* the active download
    # are handled correctly (i.e., can be re-parsed or removed).
    filtered_queue = [
        item for item in session['queue'] 
        if item['status'] not in ['completed', 'cancelled', 'failed', 'failed_sending', 'failed_internal']
    ]
    display_items.extend(filtered_queue)


    if not display_items:
        # Use update_obj to reply if it's a new message, otherwise send a new message
        if update_obj and hasattr(update_obj, 'message') and update_obj.message:
            await update_obj.message.reply_text("当前没有正在处理或排队的视频。")
        else:
            await context.bot.send_message(chat_id=chat_id, text="当前没有正在处理或排队的视频。")
        return

    # Create rows of three number buttons, now with `start_download` or `reparse_item` callback
    current_row = []
    for i, item in enumerate(display_items):
        display_title = item.get('title', '未知视频')
        
        # Prepend emojis based on status
        if item.get('status') == 'parse_failed':
            display_title = f"❌ [解析失败] {display_title}" # X emoji for parse failed
        elif item.get('status') == 'pending':
            display_title = f"✅ {display_title}" # Checkmark emoji for pending
        elif item.get('status') == 'failed_last_attempt':
            display_title = f"⚠️ [下载失败] {display_title}" # Warning emoji for failed last attempt
        elif item.get('status') == 'downloading':
            display_title = f"⬇️ [下载中] {display_title}" # Down arrow for downloading
        elif item.get('status') == 'sending':
            display_title = f"⬆️ [发送中] {display_title}" # Up arrow for sending
        elif display_title == '未知视频' or not display_title or display_title == '[解析中]':
            display_title = f"🔍 [解析中] {item.get('url', '链接')}"
        
        status_info = f" ({item.get('status', '待处理')})" # Keep status for debug, but emoji makes it clear
        response_text += f"**{i+1}.** `{display_title}`\n" # Removed status_info here

        # Determine callback data based on item status
        callback_data_for_button = f'_no_op_dummy_{i}' # Default to dummy

        # Only allow starting download if pending or failed_last_attempt
        # Add a remove option for failed items
        if item['status'] in ['pending', 'failed_last_attempt']:
            callback_data_for_button = f'start_download_{item["unique_id"]}'
            current_row.append(InlineKeyboardButton(f"下载 {i+1}", callback_data=callback_data_for_button))
        elif item['status'] == 'parse_failed':
            callback_data_for_button = f'reparse_item_{item["unique_id"]}' # New callback for re-parsing
            current_row.append(InlineKeyboardButton(f"重解析 {i+1}", callback_data=callback_data_for_button))
        else: # For downloading, sending, etc. display just a number button that does nothing
            current_row.append(InlineKeyboardButton(f"查看 {i+1}", callback_data=callback_data_for_button))
        
        # Add a "Remove" button for failed or parse_failed items (regardless of active_download status, as it's now in queue)
        if item['status'] in ['parse_failed', 'failed_last_attempt']:
            current_row.append(InlineKeyboardButton(f"移除 {i+1}", callback_data=f'remove_item_{item["unique_id"]}'))


        if len(current_row) >= 2 and i == len(display_items) -1: # Ensure we have at least 2 buttons per row before adding a new row
             keyboard.append(current_row)
             current_row = []
        elif len(current_row) == 2: # Keep 2 columns if not mixed, or adjust to 3 for number + remove
            keyboard.append(current_row)
            current_row = []
        elif len(current_row) == 3: # 3 columns per row
            keyboard.append(current_row)
            current_row = []
    
    # Pad the last row with empty buttons if it's not full, to maintain layout
    while len(current_row) > 0 and len(current_row) < 3: # Only pad if there are buttons in the row
        current_row.append(InlineKeyboardButton(" ", callback_data='_no_op'))
    if current_row: # Add any remaining buttons (including padded ones) in the last row
        keyboard.append(current_row)


    # Add clear all button if there are any items
    if display_items:
        keyboard.append([InlineKeyboardButton("清空列表", callback_data='clear_all')])

    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None

    # Send a new message with the updated list and buttons
    message_sent = await context.bot.send_message(
        chat_id=chat_id,
        text=response_text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    session['selection_buttons_message_id'] = message_sent.message_id
    save_user_session(chat_id, session)
    logger.info(f"[{chat_id}] Displayed '/list' command queue and selection buttons.")


async def _send_media_file(chat_id, file_path, video_title, send_as_video, context, thumbnail_path=None, video_width=None, video_height=None, caption_prefix=''):
    """
    Generic helper function to send video or file to a specified chat_id.
    Does not include message editing or deletion logic.
    """
    try:
        with open(file_path, 'rb') as media_file:
            logger.info(f"[{chat_id}] Opening file {file_path} for sending to target chatinien...")
            
            caption = f'{caption_prefix}视频：{video_title}' if send_as_video else f'{caption_prefix}文件：{video_title}'

            if send_as_video:
                thumbnail_file = None
                if thumbnail_path and os.path.exists(thumbnail_path):
                    thumbnail_file = open(thumbnail_path, 'rb')
                    logger.info(f"[{chat_id}] Using thumbnail: {thumbnail_path}")

                await context.bot.send_video(
                    chat_id=chat_id,
                    video=media_file,
                    caption=caption,
                    thumbnail=thumbnail_file,
                    supports_streaming=True,
                    width=video_width,
                    height=video_height
                )
                if thumbnail_file:
                    thumbnail_file.close()
                logger.info(f"[{chat_id}] Video sent via Telegram API send_video to {chat_id}.")
            else:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=media_file,
                    filename=os.path.basename(file_path),
                    caption=caption,
                )
                logger.info(f"[{chat_id}] Video sent via Telegram API send_document to {chat_id}.")
            
        return True

    except Exception as e:
        logger.error(f"[{chat_id}] Error sending file to Telegram: {e}", exc_info=True)
        return False

async def download_and_send_video(chat_id, download_item: dict, context):
    """
    Core logic for downloading and sending videos.
    download_item contains 'url', 'title', 'initial_message_id', 'format_string', 'queue_index'
    """
    url = download_item['url']
    initial_message_id = download_item['initial_message_id']
    format_string = download_item['format_string']
    video_title = download_item['title']
    queue_unique_id = download_item.get('unique_id') # Use unique_id for consistency

    file_path = None
    send_as_video = False
    thumbnail_path = None
    
    session = user_download_sessions.get(chat_id)
    if not session: # This should ideally not happen if session is loaded properly
        logger.error(f"[{chat_id}] download_and_send_video: Session lost. Aborting.")
        return False

    # IMPORTANT: Ensure active_download matches the item being processed
    # This prevents issues if a new request comes in and replaces active_download
    # or if the user clicks cancel while a download is in progress.
    if session.get('active_download') and session.get('active_download').get('unique_id') != download_item.get('unique_id'):
        logger.warning(f"[{chat_id}] download_and_send_video called, but download item is not current active item or session updated. Aborting.")
        # Attempt to set the item's status in the queue to failed if it can be found
        # MODIFICATION: Ensure if active_download doesn't match, it means it was superseded, so mark this one as failed_internal and remove from queue
        # Find and remove the mismatched item if it exists in the queue
        session['queue'] = [q_item for q_item in session['queue'] if q_item.get('unique_id') != download_item.get('unique_id')]
        save_user_session(chat_id, session)
        return False

    session['active_download']['status'] = 'downloading'
    save_user_session(chat_id, session) # Save status update

    try:
        os.makedirs(DOWNLOAD_DESTINATION_DIR, exist_ok=True)
        logger.info(f"[{chat_id}] Ensuring '{DOWNLOAD_DESTINATION_DIR}' folder exists.")

        # First, try to get info without downloading to check size and set initial message
        ydl_opts_info = {
            'format': format_string,
            'noplaylist': True,
            'restrictfilenames': True,
            'nocheckcertificate': True,
            'quiet': True,
            'no_warnings': True,
            'simulate': True,
            'getfilename': True,
            'geturl': True,
            'forcetitle': True,
            'forcefilename': True,
        }

        with yt_dlp.YoutubeDL(ydl_opts_info) as ydl_info:
            logger.info(f"[{chat_id}] Attempting to get video info (format: {format_string})..")
            
            info_dict = ydl_info.extract_info(url, download=False)
            # Update video_title in case the initial one was '[解析中]' or less accurate
            video_title = info_dict.get('title', video_title)
            file_size_initial_estimate = info_dict.get('filesize') or info_dict.get('filesize_approx')
            
            # Update session's active_download title
            session['active_download']['title'] = video_title
            save_user_session(chat_id, session) # Save updated title

            logger.info(f"[{chat_id}] Video title: {video_title}, Estimated file size: {file_size_initial_estimate} bytes (format: {format_string})")

            message_to_edit = ""
            action_needed = False # Flag to indicate if quality selection is needed

            if file_size_initial_estimate is None:
                message_to_edit = f'找到视频：**{video_title}**，但无法预估文件大小。将尝试下载，并根据实际大小决定是否提供清晰度选项，请稍候...'
                logger.warning(f"[{chat_id}] Failed to get file size for video {video_title}. Attempting download and will decide sending method based on actual size.")
            elif file_size_initial_estimate > TELEGRAM_MAX_FILE_SIZE_BYTES:
                message_to_edit = f'视频 **{video_title}** (当前选择的清晰度) 预估大小约为 {file_size_initial_estimate / (1000 * 1000 * 1000):.2f}GB，超出 Telegram {TELEGRAM_MAX_FILE_SIZE_BYTES / (1000 * 1000 * 1000):.2f}GB Upload limit, cannot process.'
                logger.info(f"[{chat_id}] Video {video_title} (size: {file_size_initial_estimate}) exceeds Telegram limit, not downloading.")
                action_needed = True # Still offer options if possible
            elif file_size_initial_estimate <= TELEGRAM_VIDEO_FILE_SIZE_LIMIT_BYTES:
                send_as_video = True
                message_to_edit = f'找到视频：**{video_title}** (大小: {file_size_initial_estimate / (1024 * 1024):.2f}MB)，开始下载...'
                logger.info(f"[{chat_id}] Video {video_title} (size: {file_size_initial_estimate}) is expected to be sent as video.")
            else: # Greater than 50MB and less than 2GB, needs selection unless already trying specific format
                if format_string == 'best': # Only offer selection if 'best' was originally attempted and it's too large
                    message_to_edit = f'视频 **{video_title}** 预估大小约为 {file_size_initial_estimate / (1024 * 1024):.2f}MB。文件较大，请选择清晰度以尝试下载。'
                    action_needed = True # Flag to show quality selection buttons
                    logger.info(f"[{chat_id}] Video {video_title} (size: {file_size_initial_estimate}) is estimated to be larger than 50MB, will offer quality selection.")
                else: # User already selected a quality, just download
                    message_to_edit = f'找到视频：**{video_title}** (大小: {file_size_initial_estimate / (1024 * 1024):.2f}MB)，开始下载...'
                    logger.info(f"[{chat_id}] Video {video_title} (size: {file_size_initial_estimate}) is estimated to be larger than 50MB, but specific quality selected, continuing download.")

            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=initial_message_id,
                text=message_to_edit,
                parse_mode='Markdown'
            )

            if action_needed: # If quality selection is needed
                keyboard = [
                    [InlineKeyboardButton("尝试中等质量 (720p/480p)", callback_data='quality_medium')],
                    [InlineKeyboardButton("尝试最低质量 (144p)", callback_data='quality_lowest')],
                    [InlineKeyboardButton("保存到列表", callback_data='save_to_list')], # Add save to list
                    [InlineKeyboardButton("取消", callback_data='cancel_download')]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=initial_message_id,
                    text=message_to_edit, # Re-use or refine message
                    parse_mode='Markdown',
                    reply_markup=reply_markup
                )
                session['active_download']['status'] = 'awaiting_quality_selection'
                save_user_session(chat_id, session) # Save status update
                return False # Indicate that further action is needed from user

            # Proceed with actual download
            ydl_opts_download = {
                'format': format_string,
                'outtmpl': os.path.join(DOWNLOAD_DESTINATION_DIR, '%(title)s.%(ext)s'),
                'noplaylist': True,
                'restrictfilenames': True,
                'nocheckcertificate': True,
                'quiet': True,
                'no_warnings': True,
                'progress_hooks': [yt_dlp_progress_hook],
            }

            with yt_dlp.YoutubeDL(ydl_opts_download) as ydl_download:
                logger.info(f"[{chat_id}] Preparing to download (format: {format_string}).")
                
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(ydl_download.download, [url]),
                        timeout=300 # 5 minutes timeout for download
                    )
                    logger.info(f"[{chat_id}] yt-dlp download completed.")
                except asyncio.TimeoutError:
                    error_msg = f"下载超时 (5分钟)。"
                    logger.error(f"[{chat_id}] Download of {url} timed out.", exc_info=True)
                    session['active_download']['status'] = 'failed_last_attempt' # Mark as failed_last_attempt
                    save_user_session(chat_id, session)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=initial_message_id,
                        text=error_msg,
                        parse_mode='Markdown',
                        reply_markup=None
                    )
                    return False
                except yt_dlp.utils.DownloadError as de:
                    error_msg = f"视频下载失败：`{de}`\n请检查链接是否有效、视频是否存在，或稍后再试。"
                    logger.error(f"[{chat_id}] yt-dlp download error: {de}", exc_info=True)
                    session['active_download']['status'] = 'failed_last_attempt' # Mark as failed_last_attempt
                    save_user_session(chat_id, session)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=initial_message_id,
                        text=error_msg,
                        parse_mode='Markdown',
                        reply_markup=None
                    )
                    return False
                except Exception as e:
                    error_msg = f'下载时发生未知错误：`{e}`\n请联系管理员或稍后再试。'
                    logger.error(f"[{chat_id}] Unknown error during download of {url}: {e}", exc_info=True)
                    session['active_download']['status'] = 'failed_last_attempt' # Mark as failed_last_attempt
                    save_user_session(chat_id, session)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=initial_message_id,
                        text=error_msg,
                        parse_mode='Markdown',
                        reply_markup=None
                    )
                    return False


                info_dict_after_download = ydl_download.extract_info(url, download=False) 
                final_file_path = ydl_download.prepare_filename(info_dict_after_download)
                logger.info(f"[{chat_id}] Final file path: {final_file_path}")
                file_path = final_file_path

            if os.path.exists(file_path):
                actual_file_size = os.path.getsize(file_path)
                logger.info(f"[{chat_id}] Actual downloaded file size: {actual_file_size} bytes")

                if actual_file_size > TELEGRAM_MAX_FILE_SIZE_BYTES:
                    session['active_download']['status'] = 'failed' # Permanently failed due to size
                    save_user_session(chat_id, session)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=initial_message_id,
                        text=f'视频 **{video_title}** 实际大小约为 {actual_file_size / (1000 * 1000 * 1000):.2f}GB，超出 Telegram {TELEGRAM_MAX_FILE_SIZE_BYTES / (1000 * 1000 * 1000):.2f}GB Upload limit, cannot process.', # FIXED: Escaped single quote
                        parse_mode='Markdown',
                        reply_markup=None
                    )
                    return False
                elif actual_file_size > TELEGRAM_VIDEO_FILE_SIZE_LIMIT_BYTES:
                    # Even if it was <=50MB estimate, if actual size >50MB, offer quality selection or save to list
                    session['active_download']['status'] = 'awaiting_quality_selection'
                    save_user_session(chat_id, session) # Save status update
                    keyboard = [
                        [InlineKeyboardButton("尝试中等质量 (720p/480p)", callback_data='quality_medium')],
                        [InlineKeyboardButton("尝试最低质量 (144p)", callback_data='quality_lowest')],
                        [InlineKeyboardButton("保存到列表", callback_data='save_to_list')], # Add save to list
                        [InlineKeyboardButton("取消", callback_data='cancel_download')]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=initial_message_id,
                        text=f'视频 **{video_title}** 实际大小为 {actual_file_size / (1024 * 1024):.2f}MB，超过 50MB，需要您选择其他清晰度，或将其保存到列表后续处理。',
                        parse_mode='Markdown',
                        reply_markup=reply_markup
                    )
                    return False
                else:
                    send_as_video = True # Ensure sending as video if within 50MB limit

                video_width = info_dict_after_download.get('width')
                video_height = info_dict_after_download.get('height')
                
                file_to_send = file_path 
                logger.info(f"[{chat_id}] Skipping ffmpeg re-encoding/muxing, directly using original file: {file_to_send}")

                if send_as_video:
                    thumbnail_path = os.path.join(DOWNLOAD_DESTINATION_DIR, f"{os.path.basename(file_path)}.jpg")
                    success_thumbnail = await extract_thumbnail(file_to_send, thumbnail_path)
                    if not success_thumbnail:
                        thumbnail_path = None

                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=initial_message_id,
                    text='视频下载完成，正在发送到 Telegram...',
                )
                session['active_download']['status'] = 'sending'
                save_user_session(chat_id, session) # Save status update
                
                # Check if the download_item still matches active_download before sending
                if session.get('active_download') and session.get('active_download').get('unique_id') != download_item.get('unique_id'):
                    logger.info(f"[{chat_id}] Download item was replaced or cancelled before sending. Aborting send.")
                    return False

                # --- Send to user first ---
                user_send_success = await _send_media_file(chat_id, file_to_send, video_title, send_as_video, context, thumbnail_path, video_width, video_height)
                
                if user_send_success:
                    logger.info(f"[{chat_id}] File sent successfully. Attempting to delete initial status message ID: {initial_message_id}.")
                    try:
                        # Delete the status message that was being updated during download
                        await context.bot.delete_message(chat_id=chat_id, message_id=initial_message_id)
                        logger.info(f"[{chat_id}] Deleted initial status message (ID: {initial_message_id}).")
                    except Exception as e:
                        logger.warning(f"[{chat_id}] Failed to delete initial status message (ID: {initial_message_id}): {e}")

                    # --- If user send successful, attempt to forward to channel ---
                    if TELEGRAM_CHANNEL_ID:
                        logger.info(f"[{chat_id}] Attempting to forward video to channel: {TELEGRAM_CHANNEL_ID}")
                        try:
                            # Re-open file for channel send, as user send might have consumed it
                            channel_send_success = await _send_media_file(
                                TELEGRAM_CHANNEL_ID,
                                file_to_send,
                                video_title,
                                send_as_video,
                                context,
                                thumbnail_path,
                                video_width,
                                video_height,
                                caption_prefix='[自动转发] ' # Add prefix for channel message
                            )
                            if channel_send_success:
                                logger.info(f"[{chat_id}] Video successfully forwarded to channel: {TELEGRAM_CHANNEL_ID}")
                            else:
                                logger.warning(f"[{chat_id}] Video failed to forward to channel: {TELEGRAM_CHANNEL_ID}")
                        except Exception as channel_e:
                            logger.error(f"[{chat_id}] Error forwarding video to channel {TELEGRAM_CHANNEL_ID}时发生错误: {channel_e}", exc_info=True)
                    else:
                        logger.info(f"[{chat_id}] TELEGRAM_CHANNEL_ID is not set, skipping forwarding to channel.")

                    session['active_download']['status'] = 'completed'
                else: # User send failed
                    session['active_download']['status'] = 'failed_sending'
                    # If sending to user failed, edit message to reflect failure
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=initial_message_id,
                            text=f'发送文件到 Telegram 时发生错误：`{e}`\n您可以尝试重新发送链接，或选择其他清晰度。',
                            parse_mode='Markdown'
                        )
                    except Exception as edit_e:
                        logger.error(f"[{chat_id}] Could not edit message {initial_message_id} to show send failure error: {edit_e}", exc_info=True)
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f'发送文件到 Telegram 时发生错误：`{e}`\n您可以尝试重新发送链接，或选择其他清晰度。',
                            parse_mode='Markdown'
                        )

                save_user_session(chat_id, session) # Save final status
                return user_send_success

            else:
                session['active_download']['status'] = 'failed_last_attempt' # Treat as a retryable failed download
                save_user_session(chat_id, session)
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=initial_message_id,
                    text='下载失败：未能找到下载的视频文件，请重试或检查链接。',
                    reply_markup=None
                )
                return False

    except yt_dlp.utils.DownloadError as de:
        error_msg = f"视频处理失败：`{de}`\n请检查链接是否有效、视频是否存在，或稍后再试。"
        logger.error(f"[{chat_id}] yt-dlp download error: {de}", exc_info=True)
        session['active_download']['status'] = 'failed_last_attempt' # Mark as failed, but still retryable
        save_user_session(chat_id, session)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=initial_message_id,
            text=error_msg,
            parse_mode='Markdown',
            reply_markup=None
        )
        return False
            
    except Exception as e:
        error_msg = f'发生未知错误：`{e}`\n请联系管理员或稍后再试。'
        logger.error(f"[{chat_id}] Unknown error while processing link {url}: {e}", exc_info=True)
        session['active_download']['status'] = 'failed_last_attempt' # Mark as failed, but still retryable
        save_user_session(chat_id, session)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=initial_message_id,
            text=error_msg,
            parse_mode='Markdown',
            reply_markup=None
        )
        return False
    finally:
        logger.info(f"[{chat_id}] Entering finally block.")
        # Cleanup logic
        if file_path and os.path.exists(file_path):
            if SHOULD_DELETE_FILE:
                try:
                    os.remove(file_path)
                    logger.info(f"[{chat_id}] Finally block: Deleted local original file: {file_path}")
                except OSError as os_err:
                    logger.error(f"[{chat_id}] Finally block: OS error deleting original file {file_path}: {os_err}")
            else:
                logger.info(f"[{chat_id}] Finally block: Keeping original file as per config: {file_path}")
        
        if thumbnail_path and os.path.exists(thumbnail_path):
            try:
                os.remove(thumbnail_path)
            except OSError as os_err:
                logger.error(f"[{chat_id}] Finally block: OS error deleting thumbnail file {thumbnail_path}: {os_err}")

        # Update queue status based on active_download result
        if session and session.get('active_download') and session['active_download'].get('unique_id') == download_item.get('unique_id'):
            
            # If the active download was originally from the queue and it completed successfully, remove it.
            if session['active_download']['status'] == 'completed':
                # Remove the completed item from the queue
                session['queue'] = [q_item for q_item in session['queue'] if q_item.get('unique_id') != download_item.get('unique_id')]
                logger.info(f"[{chat_id}] Item {download_item.get('title')} (unique_id: {download_item.get('unique_id')}) successfully completed and removed from queue.")
            elif session['active_download']['status'] == 'cancelled':
                # Remove cancelled item from queue
                session['queue'] = [q_item for q_item in session['queue'] if q_item.get('unique_id') != download_item.get('unique_id')]
                logger.info(f"[{chat_id}] Item {download_item.get('title')} (unique_id: {download_item.get('unique_id')}) cancelled and removed from queue.")
            # For permanent failures (failed, failed_sending, failed_internal), also remove from queue
            elif session['active_download']['status'] in ['failed', 'failed_sending', 'failed_internal']:
                logger.info(f"[{chat_id}] Item {download_item.get('title')} (unique_id: {download_item.get('unique_id')}) permanently failed, removing from queue.")
                session['queue'] = [q_item for q_item in session['queue'] if q_item.get('unique_id') != download_item.get('unique_id')]
            elif session['active_download']['status'] == 'parse_failed' or session['active_download']['status'] == 'failed_last_attempt':
                 # If parse failed or last attempt failed, ensure it's in the queue (or updated) for re-parse/retry
                found_in_queue = False
                for idx, q_item in enumerate(session['queue']):
                    if q_item.get('unique_id') == download_item.get('unique_id'):
                        session['queue'][idx]['status'] = session['active_download']['status']
                        found_in_queue = True
                        break
                if not found_in_queue: # If not found in queue (e.g., direct download failed parsing), add it
                    session['queue'].append(session['active_download'])
                logger.info(f"[{chat_id}] Item {download_item.get('title')} (unique_id: {download_item.get('unique_id')}) parsing/downloading failed, status updated in queue.")
            
            # IMPORTANT FIX: Always clear active_download if it's no longer actively downloading/sending.
            # Only keep it active if it's truly awaiting user input (awaiting_quality_selection)
            if session['active_download']['status'] not in ['awaiting_quality_selection']:
                logger.info(f"[{chat_id}] Clearing active_download. Status was: {session['active_download'].get('status')}")
                session['active_download'] = None # Clear active download if finished or failed permanently
            else:
                logger.info(f"[{chat_id}] Active download is {session['active_download'].get('status')}, keeping it active temporarily.")

        save_user_session(chat_id, session) # Save final session state


async def handle_video_link(update: Update, context):
    """Handles video links sent by the user."""
    message_text = update.message.text
    chat_id = update.message.chat_id
    last_user_message_id = update.message.message_id

    # Load session data
    session = user_download_sessions.get(chat_id)
    if not session:
        session = load_user_session(chat_id)
        if session:
            user_download_sessions[chat_id] = session
        else:
            user_download_sessions[chat_id] = {
                'active_download': None,
                'queue': [],
                'last_user_message_id': None,
                'selection_buttons_message_id': None
            }
            session = user_download_sessions[chat_id]

    session['last_user_message_id'] = last_user_message_id

    # Regex to find all URLs in the message
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', message_text)
    
    if not urls:
        logger.info(f"[{chat_id}] Received message but no URL detected: {message_text}")
        await update.message.reply_text("请发送有效的视频链接。")
        return # No URLs found, do nothing

    logger.info(f"[{chat_id}] Detected {len(urls)} URLs.")

    # Delete previous selection buttons message if it exists
    if session['selection_buttons_message_id']:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=session['selection_buttons_message_id'])
            session['selection_buttons_message_id'] = None
        except Exception as e:
            logger.warning(f"[{chat_id}] Failed to delete old selection buttons message (handle_video_link): {e}")

    # Use a set to track URLs already in the queue or active to avoid true duplicates
    existing_urls = set()
    if session['active_download']:
        existing_urls.add(session['active_download']['url'])
    for item in session['queue']:
        existing_urls.add(item['url'])

    new_items_added_count = 0
    new_urls_to_fetch_titles = []
    new_items_added_indices = [] # To track where new items are added in session['queue']

    for i, url in enumerate(urls):
        if url not in existing_urls:
            # Generate a unique ID for each new item
            item = {'url': url, 'title': '[解析中]', 'status': 'pending', 'unique_id': str(uuid.uuid4())} 
            session['queue'].append(item)
            new_items_added_indices.append(len(session['queue']) - 1)
            new_urls_to_fetch_titles.append(url)
            existing_urls.add(url) # Add to set immediately to avoid duplicates within the same message
            new_items_added_count += 1
        # If the URL *is* existing, but its status is 'parse_failed' or 'failed_last_attempt',
        # we can prompt the user that it's already there and they can re-parse/retry.
        else:
            found_item = None
            if session['active_download'] and session['active_download']['url'] == url:
                found_item = session['active_download']
            else:
                for q_item in session['queue']:
                    if q_item['url'] == url:
                        found_item = q_item
                        break
            
            if found_item and found_item['status'] in ['parse_failed', 'failed_last_attempt']:
                await update.message.reply_text(f"视频 `{found_item.get('title', '未知视频')}` (链接：{url}) 已在列表中，状态为 `{found_item['status']}`。您可以使用 `/list` 重新操作。")
                logger.info(f"[{chat_id}] User sent an existing failed URL. Not adding as new item.")


    save_user_session(chat_id, session) # Save session after adding items with placeholder titles

    if new_items_added_count == 0 and len(session['queue']) > 0:
        pass # Do nothing, messages already sent above if applicable
    
    # Fetch titles concurrently for newly added items
    # fetched_results will be a list of (title, error_type) tuples
    fetched_results = await asyncio.gather(*[get_video_title(url) for url in new_urls_to_fetch_titles])
    for i, (title, error_type) in enumerate(fetched_results):
        queue_idx = new_items_added_indices[i]
        # Only update if the item still exists at that index and its URL matches (not removed by clear_all)
        if queue_idx < len(session['queue']) and session['queue'][queue_idx]['url'] == new_urls_to_fetch_titles[i]:
            session['queue'][queue_idx]['title'] = title
            if error_type: # If error_type is not None, it means parsing failed or timed out
                session['queue'][queue_idx]['status'] = 'parse_failed' # Mark as parse failed
            # If parsing was successful (error_type is None), keep 'pending' status
            
    save_user_session(chat_id, session) # Save session after updating titles and statuses

    logger.info(f"[{chat_id}] Added {new_items_added_count} items to queue.")

    # Re-display the list after adding and potentially updating titles
    await list_downloads(chat_id, context, update_obj=update) # Pass chat_id and original update object


async def button_callback_handler(update: Update, context):
    """Handles inline keyboard button callbacks."""
    query = update.callback_query
    await query.answer() # Always answer the callback query

    chat_id = query.message.chat_id
    data = query.data

    # Always reload session data from file to get the latest state
    session = load_user_session(chat_id) 
    if not session: # Session corrupted or not found, re-initialize
        user_download_sessions[chat_id] = {
            'active_download': None,
            'queue': [],
            'last_user_message_id': None,
            'selection_buttons_message_id': None
        }
        session = user_download_sessions[chat_id]
        await context.bot.send_message(chat_id=chat_id, text="会话信息已过期或不存在，已重置。请重新发送链接。")
        save_user_session(chat_id, session)
        await list_downloads(chat_id, context, update_obj=update) # Refresh list after reset
        return
    user_download_sessions[chat_id] = session # Update in-memory session

    # --- Dummy Button (Number Button that does nothing) ---
    if data.startswith('_no_op_dummy_') or data == '_no_op':
        logger.info(f"[{chat_id}] Clicked on a dummy button: {data}")
        return

    # --- Start Download Button (triggered by numbered buttons in list for pending/failed_last_attempt) ---
    # --- Reparse Item Button (triggered by numbered buttons for parse_failed items) ---
    if data.startswith('start_download_') or data.startswith('reparse_item_'):
        is_reparse_action = data.startswith('reparse_item_')
        item_id_to_process = data.split('_')[2] if not is_reparse_action else data.split('_')[2]

        if session['active_download']:
            await query.edit_message_text(
                text="当前已有下载任务正在进行，请等待或取消当前任务。",
                reply_markup=None # Remove buttons from the selection message
            )
            return

        try:
            selected_item_from_queue = None
            # Find the item by unique_id (safer than index)
            for q_item in session['queue']:
                if q_item.get('unique_id') == item_id_to_process:
                    selected_item_from_queue = q_item
                    break

            if not selected_item_from_queue:
                logger.warning(f"[{chat_id}] Item with ID {item_id_to_process} not found in queue. It might have already been processed or removed.")
                await query.edit_message_text(text="无效的选择，视频可能已被移除或正在处理中。请使用 `/list` 查看最新状态。")
                await list_downloads(chat_id, context, update_obj=update) # Refresh the list
                return

            # Check item status before processing for 'start_download'
            if not is_reparse_action and selected_item_from_queue['status'] not in ['pending', 'failed_last_attempt']:
                await query.edit_message_text(text=f"该项目 `{selected_item_from_queue.get('title', '未知视频')}` 状态为 `{selected_item_from_queue['status']}`，无法开始下载。", parse_mode='Markdown')
                await list_downloads(chat_id, context, update_obj=update) # Refresh the list if status doesn't allow immediate download
                return
            
            # Check item status before processing for 'reparse_item'
            if is_reparse_action and selected_item_from_queue['status'] != 'parse_failed':
                await query.edit_message_text(text=f"该项目 `{selected_item_from_queue.get('title', '未知视频')}` 状态为 `{selected_item_from_queue['status']}`，无需重新解析。", parse_mode='Markdown')
                await list_downloads(chat_id, context, update_obj=update) # Refresh the list
                return

            # 1. Acknowledge button click on the original message and remove its buttons
            await query.edit_message_text(
                text=f"正在处理您的请求，请稍候...",
                parse_mode='Markdown',
                reply_markup=None # Remove selection buttons
            )
            
            # 2. Send a NEW message to be used for ongoing progress updates
            status_message = await context.bot.send_message(
                chat_id=chat_id,
                text=f"开始{'重新解析' if is_reparse_action else '下载'}：**{selected_item_from_queue.get('title', '未知视频')}**...",
                parse_mode='Markdown'
            )
            selected_item_from_queue['initial_message_id'] = status_message.message_id
            
            # Set the item as active download in session
            session['active_download'] = selected_item_from_queue
            # Update status in queue
            for q_item in session['queue']:
                if q_item.get('unique_id') == item_id_to_process:
                    q_item['status'] = 'downloading' if not is_reparse_action else 'pending' # 'pending' for re-parse to re-evaluate after title fetch
                    q_item['initial_message_id'] = status_message.message_id # Ensure queue item also has this updated
                    break

            save_user_session(chat_id, session) # Save state before starting download

            if is_reparse_action:
                logger.info(f"[{chat_id}] User requested re-parse for item ID: {item_id_to_process}")
                # Re-fetch title and then attempt download if parsing is successful
                title, error_type = await get_video_title(selected_item_from_queue['url'])
                selected_item_from_queue['title'] = title
                if error_type: # Parsing failed
                    selected_item_from_queue['status'] = 'parse_failed'
                    save_user_session(chat_id, session) # Save the 'parse_failed' status
                    await context.bot.edit_message_text( # Edit the NEW status message
                        chat_id=chat_id,
                        message_id=status_message.message_id,
                        text=f"重新解析视频 **{selected_item_from_queue.get('title', '未知视频')}** 失败：`{error_type}`。\n请重试或检查链接。",
                        parse_mode='Markdown',
                        reply_markup=None
                    )
                else: # Parsing succeeded, proceed to download logic
                    selected_item_from_queue['status'] = 'pending' # Reset to pending for download check
                    selected_item_from_queue['format_string'] = 'best' # Start with best quality
                    # session['active_download'] is already set above
                    save_user_session(chat_id, session) # Save session after title update
                    await download_and_send_video(chat_id, session['active_download'], context) # Attempt download
            else: # It's a start_download_ click
                logger.info(f"[{chat_id}] User selected item {selected_item_from_queue.get('title')} (ID: {item_id_to_process}) to start downloading.")
                selected_item_from_queue['format_string'] = 'best' # Start with best quality
                # session['active_download'] is already set above
                save_user_session(chat_id, session) # Save session before download
                await download_and_send_video(chat_id, session['active_download'], context)
            
            await list_downloads(chat_id, context, update_obj=update) # Always refresh list at the end
            return
        except Exception as e:
            logger.error(f"[{chat_id}] Error in start_download/reparse logic for data: {data}, error: {e}", exc_info=True)
            # If an error occurs, ensure the status message is updated and active_download is cleared
            if session.get('active_download') and session['active_download'].get('unique_id') == item_id_to_process:
                session['active_download']['status'] = 'failed_internal'
                session['queue'] = [q_item for q_item in session['queue'] if q_item.get('unique_id') != item_id_to_process] # Remove from queue
                session['active_download'] = None
                save_user_session(chat_id, session)
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=status_message.message_id, # Use the new status message ID
                        text=f"处理视频 **{selected_item_from_queue.get('title', '未知视频')}** 时发生错误：`{e}`。请稍后再试。",
                        parse_mode='Markdown',
                        reply_markup=None
                    )
                except Exception as edit_e:
                    logger.error(f"[{chat_id}] Could not edit status message {status_message.message_id} after error: {edit_e}")
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"处理视频 **{selected_item_from_queue.get('title', '未知视频')}** 时发生错误：`{e}`。请稍后再试。",
                        parse_mode='Markdown'
                    )
            else: # Fallback if active_download was already cleared
                 await context.bot.send_message(chat_id=chat_id, text=f"处理请求时发生错误：`{e}`。请使用 `/list` 查看最新状态。")

            await list_downloads(chat_id, context, update_obj=update) # Refresh the list
            return
            
    # Add handler for `remove_item`
    if data.startswith('remove_item_'):
        try:
            item_id_to_remove = data.split('_')[2]
            original_queue_len = len(session['queue'])
            
            # Remove from queue
            session['queue'] = [q_item for q_item in session['queue'] if q_item.get('unique_id') != item_id_to_remove]
            
            # IMPORTANT: If the item to remove is currently in active_download, clear active_download.
            if session.get('active_download') and session['active_download'].get('unique_id') == item_id_to_remove:
                session['active_download'] = None
                logger.info(f"[{chat_id}] Removed active_download item with ID: {item_id_to_remove}")

            if len(session['queue']) < original_queue_len or (session.get('active_download') is None and original_queue_len == len(session['queue'])):
                save_user_session(chat_id, session)
                await query.edit_message_text(text="已从列表中移除。", reply_markup=None) # Remove buttons on the old message
                logger.info(f"[{chat_id}] User removed item with ID: {item_id_to_remove}")
            else:
                await query.edit_message_text(text="该项目不存在或已被处理。请使用 `/list` 查看最新状态。")

            await list_downloads(chat_id, context, update_obj=update) # Refresh the list after removal
            return
        except Exception as e:
            logger.error(f"[{chat_id}] Error in remove_item logic: {data}, error: {e}", exc_info=True)
            await context.bot.send_message(chat_id=chat_id, text="移除项目时发生错误。")
            return

    # --- Clear All Button ---
    if data == 'clear_all':
        # If there's an active download, it needs to be cancelled first (optional, but good practice)
        if session['active_download']:
            session['active_download']['status'] = 'cancelled' # Mark active as cancelled
            logger.info(f"[{chat_id}] Clearing list: active download {session['active_download'].get('title')} marked as cancelled.")
        
        session['active_download'] = None
        session['queue'] = []
        save_user_session(chat_id, session)
        # Delete the current message with buttons
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=query.message.message_id)
        except Exception as e:
            logger.warning(f"[{chat_id}] Failed to delete message after clear_all: {e}")
        
        await context.bot.send_message(
            chat_id=chat_id,
            text="列表已清空。"
        )
        logger.info(f"[{chat_id}] User cleared the list.")
        return

    # --- Quality selection or Cancel/Save for Active Download ---
    # This section is still relevant for a download that is *currently* active and awaiting user input
    # (e.g., after initial size estimation)
    active_dl = session.get('active_download')
    if not active_dl or active_dl.get('initial_message_id') != query.message.message_id:
        # This check ensures that the buttons are for the *current* active download message
        # If not, it means the message is old or the active download has changed.
        try: 
            await query.edit_message_text(text="该操作已过期或不适用于当前任务。请使用 `/list` 查看最新状态。")
        except Exception: 
            await context.bot.send_message(chat_id=chat_id, text="该操作已过期或不适用于当前任务。请使用 `/list` 查看最新状态。")
        await list_downloads(chat_id, context, update_obj=update)
        return
    
    url = active_dl['url']
    initial_message_id = active_dl['initial_message_id']
    video_title = active_dl['title']

    ack_message_text = ""
    should_retry_download = False
    format_to_try = None

    if data == 'quality_medium':
        ack_message_text = f"您选择了：中等质量。正在重新尝试下载视频 **{video_title}**..."
        should_retry_download = True
        format_to_try = 'bestvideo[height<=720]+bestaudio/best[height<=720]/bestvideo[height<=480]+bestaudio/best[height<=480]'
        active_dl['status'] = 'downloading' # Reset status for retry
    elif data == 'quality_lowest':
        ack_message_text = f"您选择了：最低质量。正在重新尝试下载视频 **{video_title}**..."
        should_retry_download = True
        format_to_try = 'worst'
        active_dl['status'] = 'downloading' # Reset status for retry
    elif data == 'save_to_list': # New "Save to List" option
        # Find the item in the queue by unique_id and update its status to pending
        found_in_queue = False
        for idx, q_item in enumerate(session['queue']):
            if q_item.get('unique_id') == active_dl.get('unique_id'):
                session['queue'][idx]['status'] = 'pending'
                found_in_queue = True
                break
        
        if not found_in_queue: # This case should ideally not happen if active_dl was from a parsed URL
            active_dl['status'] = 'pending'
            item_to_save = active_dl.copy() # Make a copy
            # No need for new uuid if it's already in active, it already has one.
            session['queue'].append(item_to_save)
        
        ack_message_text = f"视频 **{video_title}** 已保存回待处理列表。您可以使用 `/list` 查看。"
        session['active_download'] = None # Clear active download
        save_user_session(chat_id, session) # Save state
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=initial_message_id,
            text=ack_message_text,
            parse_mode='Markdown',
            reply_markup=None
        )
        logger.info(f"[{chat_id}] User chose to save to list.")
        # After saving to list, it's good to refresh the list view for the user
        await list_downloads(chat_id, context, update_obj=update)
        return # Important: exit after saving to list

    elif data == 'cancel_download':
        ack_message_text = f"已取消视频 **{video_title}** 的下载。"
        should_retry_download = False # Explicitly do not retry
        
        # If the item was originally from the queue, mark it as cancelled in the queue
        # For simplicity, if cancelled, just remove it from queue.
        # This prevents cancelled items from sticking around if they were from the queue.
        session['queue'] = [q_item for q_item in session['queue'] if q_item.get('unique_id') != active_dl.get('unique_id')]
        session['active_download'] = None # Clear active download
        save_user_session(chat_id, session) # Save state

    # Edit the initial message to show user's choice or cancel status, and remove buttons
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=initial_message_id,
        text=ack_message_text,
        parse_mode='Markdown',
        reply_markup=None
    )
    logger.info(f"[{chat_id}] User choice: {data.replace('quality_', '')} quality.")

    if should_retry_download:
        active_dl['format_string'] = format_to_try # Update format for retry
        await download_and_send_video(chat_id, session['active_download'], context) # Pass active_download
    
    # Always refresh the list after a quality selection or cancellation
    await list_downloads(chat_id, context, update_obj=update)


# --- Main function: Starts the bot ---
def main():
    """Starts the bot."""
    logger.info("Checking and updating yt-dlp...")
    try:
        update_result = subprocess.run(['yt-dlp', '-U'], capture_output=True, text=True, check=True)
        logger.info(f"yt-dlp update successful: {update_result.stdout}")
        if update_result.stderr:
            logger.warning(f"yt-dlp update warnings/errors: {update_result.stderr}")
    except subprocess.CalledProcessError as e:
        logger.error(f"yt-dlp update failed (command returned non-zero exit code): {e.stderr}")
    except FileNotFoundError:
        logger.error("yt-dlp command not found. Please ensure yt-dlp is installed and configured in your system's PATH.")
        exit(1)
    except Exception as e:
        logger.error(f"Unknown error during yt-dlp update: {e}")

    # Load all existing user sessions at startup
    for filename in os.listdir(USER_DATA_DIR):
        if filename.endswith(".json"):
            try:
                chat_id = int(filename.split('.')[0])
                session_data = load_user_session(chat_id)
                if session_data:
                    # For existing items loaded from file, ensure they have a unique_id
                    # This handles sessions saved before unique_id was introduced
                    for item in session_data['queue']:
                        if 'unique_id' not in item:
                            item['unique_id'] = str(uuid.uuid4())
                    if session_data['active_download'] and 'unique_id' not in session_data['active_download']:
                        session_data['active_download']['unique_id'] = str(uuid.uuid4())
                    user_download_sessions[chat_id] = session_data
                    save_user_session(chat_id, session_data) # Save to update with unique_ids
                    logger.info(f"Loaded and updated session data for user {chat_id}.")
                else:
                    logger.warning(f"Could not load session data for user {chat_id}, file might be corrupted.")
            except (ValueError, IndexError):
                logger.warning(f"Skipping non-standard user data file: {filename}")

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).read_timeout(300).write_timeout(300).connect_timeout(300).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("list", lambda update, context: list_downloads(update.effective_chat.id, context, update_obj=update))) # Pass update object
    # Use a more general regex that captures URLs and passes them to handle_video_link
    application.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+') & ~filters.COMMAND,
        handle_video_link
    ))
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    logger.info("Bot is running...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()