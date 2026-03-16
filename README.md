# CineBro Userbot & Search Bot

A powerful combination of a Telegram Userbot (for indexing and backing up files) and a highly responsive Search Bot (for serving files via clean inline UI). Developed with performance and scale in mind, handling tens of thousands of media documents elegantly.

## Features
- **Smart Metadata Parsing:** Extracts seasons, episodes, language variants, and quality tags directly from raw filenames using complex Regex heuristics.
- **Stealth Cloning:** Safely duplicates content from source channel to backup channel while instantly bypassing duplicates and pausing to evade Telegram's `FloodWait` (Anti-ban).
- **Responsive Netflix-style UI:** Browse languages, qualities, and seasons with inline cyclic filtering. File buttons are beautifully formatted as `Movie/Show Title - Season (Quality)`.
- **Database Architecture:** Optimized MongoDB schemas serving fast indexed searches using prefix checks and `$regex` queries preventing Out-of-Memory crashes.

## Requirements
- Python 3.9+
- MongoDB Database
- A Telegram API ID & Hash
- A Pyrogram String Session (for indexer/cloner)
- A Bot Token (for serving files)

## Setup
1. Clone the repository natively.
2. Install pip requirements: `pip install -r requirements.txt`.
3. Overwrite or update `config.py` with your database credentials.

## Commands Reference
### Userbot / Indexer Commands (`main.py`)
Run the userbot securely from your Telegram account.
- `.index <chat_id_or_username>`: Crawl and index missing documents from the source chat directly to the MongoDB.
- `.clone <source_chat> <dest_chat>`: Copies every file from the source chat to a destination backup chat securely with limits and indexes the new message IDs to the database.
- `.flush`: Delete all indexed database entries.
- `.status`: Displays active server status, RAM, CPU, and total indexed database entries.

### User Search Bot (`bot.py`)
Run the Telegram bot to respond to ordinary users.
- `/start`: Starts the bot interface for a private chat.
- Text search: Just type any movie name! It handles limits and parses dynamically.

## Project Structure
- `main.py`: The userbot responsible for indexing, parsing metadata, and cloning channels safely.
- `bot.py`: The frontend UI bot responding to user queries with cyclic filters.
- `database.py`: Handles all MongoDB operations, optimization limits, schema setups, and filtering logic.

---
**Disclaimer:** Use strictly within limits to prevent account suspension. CineBro securely uses rate limitations natively.
