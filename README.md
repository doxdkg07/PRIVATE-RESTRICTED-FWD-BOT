## Features

- 📥 Download media (photos, videos, audio, documents).
- ✅ Supports downloading from both single media posts and media groups.
- 🔄 Progress bar showing real-time downloading progress.
- ✍️ Copy text messages or captions from Telegram posts.

## Configuration

1. Open the `config.env` file in your favorite text editor.
2. Replace the placeholders for `API_ID`, `API_HASH`, `SESSION_STRING`, and `BOT_TOKEN` with your actual values:
   - **`API_ID`**: Your API ID from [my.telegram.org](https://my.telegram.org).
   - **`API_HASH`**: Your API Hash from [my.telegram.org](https://my.telegram.org).
   - **`SESSION_STRING`**: The session string generated using [session-string-generator](https://telegram.tools/session-string-generator)).
   - **`BOT_TOKEN`**: The token you obtained from [@BotFather](https://t.me/BotFather).

## Usage

- **`/start`** – Welcomes you and gives a brief introduction.  
- **`/help`** – Shows detailed instructions and examples.  
- **`/dl <post_URL> <range upto> <channel id>`** or simply paste a Telegram post link – Fetch photos, videos, audio, or documents from that post.  
- **`/killall`** – Cancel any pending downloads if the bot hangs.  
- **`/logs`** – Download the bot’s logs file.  
- **`/stats`** – View current status (uptime, disk, memory, network, CPU, etc.).

### Examples
- `/dl https://t.me/566555/547 530 -1002695709891`  
- `https://t.me/666666/547`

> **Note:** Make sure both this bot and your user session are members of the source chat or channel before downloading.  

## Author

- Name: Dipak Kumar Gupta
- Telegram: [@Dipak_Kumar_Gupta](https://t.me/DIPAK_KUMAR_GUPTA)

> **Note**: If you found this repo helpful, please fork and star it. Also, feel free to share with proper credit!
