# ⚡️Telegram 视频下载转发机器人

这是一个基于 Telegram Bot API 的机器人，能够从各种支持的平台下载视频并转发到指定频道。

## ✨ 特性 (Features)
* 从多种视频网站下载视频。
* **支持视频清晰度选择。**
* **拥有类似“购物清单”的下载任务列表和队列管理功能。**
* 将下载的视频发送给用户。
* 自动转发视频到 Telegram 频道或群组。

## ⚙️ 核心技术 (Under the Hood)

本项目利用了强大的 `yt-dlp` 工具作为核心下载引擎。`yt-dlp` 是一个命令行程序，用于从互联网上下载视频和音频。

## 🙏 鸣谢 (Acknowledgements)

本项目得以实现，离不开 [yt-dlp](https://github.com/yt-dlp/yt-dlp) 团队的杰出工作。`yt-dlp` 是一个功能强大且维护良好的视频下载工具，极大地简化了视频抓取过程。我们非常感谢他们的开源贡献！

## ⚠️ 注意事项 (Important Notes)

1.  **视频大小限制：** 由于本项目使用的是 Telegram Bot API 的 `send_video` 方法，**直接发送视频文件目前存在约 50MB 的限制。**
    * **小于 50MB** 的视频会作为原生的视频文件发送，用户可以直接播放和流式传输。
    * 这个限制是 Telegram Bot API 的特性，而非本项目代码的限制。如果需要突破此限制并直接作为视频文件发送大文件，则需要使用 Telegram 的 MTProto 协议 (例如 Pyrogram 或 Telethon 库)，但这超出了当前项目的范畴。

2.  **下载队列与清晰度选择：**
    * 当您发送视频链接后，机器人会解析可用格式，并以**列表形式**（类似购物清单）展示可供选择的视频清晰度。
    * 您可以点击按钮选择您偏好的清晰度，选中的视频任务将会**添加到下载队列**中。
    * 机器人会按照队列顺序**逐个**下载和发送视频，确保任务有序进行。

3.  **稳定性与支持网站：** `yt-dlp` 支持的网站非常多，但由于网站结构可能随时变化，某些链接可能暂时无法解析或下载。如果遇到问题，通常等待 `yt-dlp` 更新即可。

## 🚀 如何使用 (Usage)

### 1. 前提条件 (Prerequisites)
* Python 3.8+
* `ffmpeg` 和 `ffprobe` (用于视频处理和缩略图提取)。请确保它们已安装并添加到系统 PATH 中。
* 一个 Telegram 机器人 token (从 @BotFather 获取)。

### 2. 克隆项目 (Clone the Repository)
```bash
git clone https://github.com/baib-web/telegram_video_bot.git
cd telegram_video_bot
```
### 3. 设置虚拟环境 (Set up Virtual Environment)

`python3 -m venv venv`
# Windows:
`.\venv\Scripts\activate.bat`
# macOS/Linux:
`source venv/bin/activate`
### 4. 安装依赖 (Install Dependencies)

`pip install -r requirements.txt`
### 5. 配置环境变量 (.env File)
修改 .env 文件

代码段
```
TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN_HERE"
DOWNLOAD_DESTINATION_DIR="./downloads"
DELETE_DOWNLOADED_FILES_AFTER_UPLOAD="true" # 或 "false"
TELEGRAM_CHANNEL_ID="" # 可选，你的频道ID，通常是负数，例如 -1001234567890
```
请替换 YOUR_BOT_TOKEN_HERE 和 TELEGRAM_CHANNEL_ID 为你的实际值。

### 6. 运行机器人 (Run the Bot)
`python bot.py`

---

📜 许可证 (License)
本项目采用 MIT 许可证。