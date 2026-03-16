# CineBro

A high-performance Telegram movie search system built with **Pyrogram + MongoDB**.

This project has two runtime components:

- **Main Bot (`bot.py`)**: handles user search requests and directly sends matching files.
- **Userbot (`main.py`)**: handles admin indexing, status, and database maintenance commands.

## Features

- Smart fuzzy search with regex (`space`, `.`, `_` -> `.*`)
- Case-insensitive matching (`$options: "i"`)
- Direct media delivery using `send_cached_media` (no inline menus/buttons)
- Search query capped with `limit(20)` for safer memory usage
- Indexing throttle (`await asyncio.sleep(0.05)` per item) to reduce CPU spikes
- Upsert-based indexing by `file_id` to prevent duplicate records
- Admin flush command to clear collection safely
- Uptime + CPU/RAM reporting in `.status`

## Project Structure

```text
CineBro/
  bot.py                # Telegram bot (user-facing search)
  main.py               # Userbot (index/status/flush)
  database.py           # Mongo connection + query helpers
  indexer.py            # Metadata extraction helpers
  config.py             # Environment loading
  utils.py              # Rate limiting helpers
  requirements.txt
```

## Requirements

- Python 3.10+
- MongoDB (local or remote)
- Telegram API credentials

## Installation

```bash
cd CineBro
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file in `CineBro/`:

```env
API_ID=123456
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
SESSION_STRING=your_userbot_session_string

MONGO_URI=mongodb://localhost:27017
DB_NAME=MoviesBot

ADMIN_ID=123456789
STORAGE_CHANNEL=-1001234567890
BACKUP_CHANNEL=-1001234567890
```

## Run

### 1) Start the main Telegram bot

```bash
python bot.py
```

### 2) Start the userbot (admin operations)

```bash
python main.py
```

You can run both in separate terminals or with a process manager.

## Admin Commands (Userbot)

- `.status` : show CPU, RAM, uptime, and movie count
- `.index <channel_id_or_username>` : index channel media into MongoDB (upsert by `file_id`)
- `.flush` : clear all movie documents from database

## Search Behavior

When a user sends text in private chat to the bot:

1. Bot replies with `🔎 Searching...`
2. Fuzzy regex search runs against title fields (max `20` docs)
3. Matching files are sent directly with caption:
   - Title
   - Size
4. Search message is removed after successful send
5. If no match is found, a friendly error is returned

## Notes

- Keep your `.env`, session strings, and `.session` files private.
- For large databases, ensure Mongo indexes are created on startup (`setup_indexes`).
- If indexing private channels, the userbot account must be joined to that channel.

## Troubleshooting

- **`PeerIdInvalid` on `.index`**: join the target channel manually first.
- **No search results**: verify indexed documents contain expected `title/clean_title` values.
- **FloodWait issues**: indexing loop already throttles; rerun command after wait if needed.

## License

For personal/educational use unless you define another license for your deployment.
