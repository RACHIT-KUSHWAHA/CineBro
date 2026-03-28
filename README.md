# CineBro

CineBro is a Telegram movie/series search bot + an admin userbot.

- The **Bot** serves users: search → buttons → file delivery.
- The **Userbot** is admin-only: index/clone channels into MongoDB, manage `.env`, broadcast, etc.

## Requirements

- Python 3.10+ (works on 3.12 too)
- MongoDB (Atlas is fine)
- Telegram API ID + API HASH
- Telegram Bot Token
- Pyrogram String Session (for the userbot)

## Quick start

### 1) Install

```bash
git clone <your-repo-url>
cd CineBro
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure environment

This project reads config from environment variables (see `config.py`).

Create a `.env` file locally (it is ignored by git):

```bash
cp .env.example .env
```

Fill at minimum:

- `API_ID`
- `API_HASH`
- `BOT_TOKEN`
- `SESSION_STRING`
- `MONGO_URI`
- `DB_NAME`
- `ADMIN_ID`
- `DATABASE_CHANNEL_ID`
- `LOG_CHANNEL_ID` (optional but recommended for audit logs)

Notes:

- `LOG_CHANNEL_ID` should be a group/channel where your bot + userbot can send messages.
- If MongoDB password has special characters, URL-encode it.

### 3) Run

Run the bot (user-facing search):

```bash
python3 bot.py
```

Run the userbot (admin tools):

```bash
python3 main.py
```

## Bot usage (users)

- Send any movie/series name in private chat.
- For series: select a season, then an episode.
- Use `/help` inside the bot for the short usage guide.

## Userbot commands (admin)

Userbot commands use the `.` prefix and only work for `ADMIN_ID`.

- `.help` — full command help
- `.id` — show chat/user/channel IDs
- `.index <chat_id|@username|link>` — index media from a chat/channel into MongoDB
- `.clone <source> <dest>` — copy media source → dest and index
- `.clone_one <source> <dest> <msg_id>` — copy one message and index
- `.env` — view/edit `.env` keys via Telegram (logs changes to `LOG_CHANNEL_ID`)
- `.stats` / `.status` — health and database stats
- `.broadcast <text>` (or reply + `.broadcast`) — send a message to all users
- `.reply <user_id> <text>` — DM a specific user
- `.flush` — delete all indexed movies (danger)

## Project structure

- `bot.py` — Telegram bot (search UI + delivery)
- `main.py` — Telegram userbot (admin tools, indexing, cloning)
- `auto_indexer.py` — channel auto-indexing helper
- `database.py` — MongoDB operations
- `env_manager.py` — `.env` editing helper
- `audit_logger.py` — log-group audit utilities (startup, downloads, env changes)
- `streamer.py` — streaming helper
- `plugins/` — optional handlers

## Safety

This repository does not commit `.env` or session files.
Use CineBro responsibly and respect Telegram limits to avoid FloodWait/account restrictions.
