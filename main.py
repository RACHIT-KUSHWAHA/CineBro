import asyncio
import re
import time
import psutil
import logging
from html import escape as html_escape
from pyrogram import filters
from pyrogram.client import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, PeerIdInvalid
import config
from database import upsert_movie_document, flush_movies_collection, get_total_movies_count, setup_indexes, movies_col, get_all_users, get_total_users_count
from env_manager import EnvManager
from audit_logger import find_missing_keys, format_env_change, format_startup_report, send_log

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("cinebro.userbot")

# Global start time for uptime
start_time = time.time()

# Validator functions
def _require_int(name: str, value) -> int:
    try: return int(value)
    except: raise ValueError(f"Invalid {name}: {value}")

def _require_str(name: str, value) -> str:
    if not value: raise ValueError(f"Empty {name}")
    return str(value).strip()

API_ID = _require_int("API_ID", config.API_ID)
API_HASH = _require_str("API_HASH", config.API_HASH)
SESSION_STRING = _require_str("SESSION_STRING", config.SESSION_STRING)
ADMIN_ID = _require_int("ADMIN_ID", config.ADMIN_ID)
LOG_CHANNEL_ID = getattr(config, "LOG_CHANNEL_ID", 0) or 0

app = Client("userbot_main", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

# Initialize Environment Manager for admin config editing
env_manager = EnvManager(".env")

# Global start time for uptime tracking
start_time = time.time()

QUALITY_PATTERN = re.compile(r"\b(480p|720p|1080p|2160p|4k)\b", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
LANG_TOKEN_PATTERN = re.compile(
    r"\b(dual(?:\s*audio)?|multi(?:\s*audio)?|hindi|english|tamil|telugu|malayalam|kannada|bengali|punjabi|marathi)\b",
    re.IGNORECASE,
)
SEASON_RANGE_PATTERN = re.compile(
    r"\b(?:s(?:eason)?\s*0?(\d{1,2})\s*(?:to|\-|_)\s*0?(\d{1,2})|s0?(\d{1,2})\s*(?:to|\-|_)\s*0?(\d{1,2}))\b",
    re.IGNORECASE,
)
SEASON_EP_PATTERN = re.compile(r"\bs(?:eason)?\s*0?(\d{1,2})\s*e(?:p(?:isode)?)?\s*0?(\d{1,3})\b", re.IGNORECASE)
SEASON_SINGLE_PATTERN = re.compile(r"\b(?:season\s*0?(\d{1,2})|s\s*0?(\d{1,2}))\b", re.IGNORECASE)
EPISODE_RANGE_PATTERN = re.compile(r"\be(?:p(?:isode)?)?\s*0?(\d{1,3})\s*(?:to|\-|_)\s*0?(\d{1,3})\b", re.IGNORECASE)
EPISODE_SINGLE_PATTERN = re.compile(r"\be(?:p(?:isode)?)?\s*0?(\d{1,3})\b", re.IGNORECASE)

NOISE_PATTERN = re.compile(
    r"(@[a-zA-Z0-9_]+|mkv|mp4|avi|x264|x265|hevc|hdrip|web-?dl|webrip|bluray|aac|10bit|esub|\b\d{4}\b)",
    re.IGNORECASE,
)


def _normalize_lang_token(token: str) -> list[str]:
    t = re.sub(r"\s+", " ", (token or "").strip().lower())
    if t in {"dual", "dual audio"}:
        return ["hindi", "english"]
    if t in {"multi", "multi audio"}:
        return ["multi"]
    return [t] if t else []


def _extract_season_and_ep(normalized_text: str) -> str:
    season = ""
    ep = ""
    
    se_match = SEASON_EP_PATTERN.search(normalized_text)
    if se_match:
        return f"S{int(se_match.group(1))} E{int(se_match.group(2))}"
    
    range_match = SEASON_RANGE_PATTERN.search(normalized_text)
    if range_match:
        start = int(range_match.group(1) or range_match.group(3))
        end = int(range_match.group(2) or range_match.group(4))
        start, end = min(start, end), max(start, end)
        if start == end:
            return f"S{start}"
        return f"S{start}-S{end}"
        
    single_match = SEASON_SINGLE_PATTERN.search(normalized_text)
    if single_match:
        season = f"S{int(single_match.group(1) or single_match.group(2))}"
    
    ep_range_match = EPISODE_RANGE_PATTERN.search(normalized_text)
    if ep_range_match:
        start = int(ep_range_match.group(1))
        end = int(ep_range_match.group(2))
        start, end = min(start, end), max(start, end)
        if start == end:
            ep = f"E{start}"
        else:
            ep = f"E{start}-E{end}"
    else:
        ep_single_match = EPISODE_SINGLE_PATTERN.search(normalized_text)
        if ep_single_match:
            ep = f"E{int(ep_single_match.group(1))}"
            
    if season and ep: return f"{season} {ep}"
    if season: return season
    if ep: return ep
    return ""


def parse_media_metadata(raw_text: str) -> dict:
    text = (raw_text or "").strip()
    normalized = re.sub(r"[._]", " ", text)

    quality_match = QUALITY_PATTERN.search(normalized)
    quality = quality_match.group(1).lower() if quality_match else "unknown"
    if quality == "4k":
        quality = "2160p"

    season = _extract_season_and_ep(normalized)

    langs = []
    for match in LANG_TOKEN_PATTERN.findall(normalized):
        for item in _normalize_lang_token(match):
            if item and item not in langs:
                langs.append(item)

    year_match = YEAR_PATTERN.search(normalized)
    year = int(year_match.group(1)) if year_match else 0

    # generate clean_title using normalized to ensure word boundaries (\b) match correctly
    # especially for cases like "NF_Series_Dual" -> "NF Series Dual"
    clean_title = normalized
    clean_title = YEAR_PATTERN.sub(" ", clean_title)
    clean_title = LANG_TOKEN_PATTERN.sub(" ", clean_title)
    clean_title = NOISE_PATTERN.sub(" ", clean_title)
    clean_title = SEASON_RANGE_PATTERN.sub(" ", clean_title)
    clean_title = SEASON_EP_PATTERN.sub(" ", clean_title)
    clean_title = SEASON_SINGLE_PATTERN.sub(" ", clean_title)
    clean_title = EPISODE_RANGE_PATTERN.sub(" ", clean_title)
    clean_title = EPISODE_SINGLE_PATTERN.sub(" ", clean_title)
    
    clean_title = re.sub(r"[._\[\]\(\)\-]+", " ", clean_title)
    clean_title = re.sub(r"\s+", " ", clean_title).strip().lower()

    return {
        "quality": quality,
        "languages": langs,
        "language": " ".join(langs) if langs else "unknown",
        "season": season,
        "year": year,
        "clean_title": clean_title,
    }


async def resolve_chat(client: Client, raw_chat: str):
    try:
        if raw_chat.startswith("http") or "t.me" in raw_chat:
            return await client.join_chat(raw_chat)
        return await client.get_chat(raw_chat)
    except PeerIdInvalid:
        return await client.join_chat(raw_chat)


async def safe_copy_message(client: Client, dest_chat_id: int, src_chat_id: int, src_msg_id: int):
    while True:
        try:
            return await client.copy_message(dest_chat_id, src_chat_id, src_msg_id)
        except FloodWait as flood:
            await asyncio.sleep(int(getattr(flood, "value", 0)) + 2)

@app.on_message(filters.command("status", prefixes=".") & (filters.user(ADMIN_ID)))
async def status_handler(client, message):
    try:
        import psutil
        cpu, ram = psutil.cpu_percent(), psutil.virtual_memory().percent
        uptime = time.strftime("%Hh %Mm %Ss", time.gmtime(time.time() - start_time))
        total_movies = await get_total_movies_count()
        status_text = (
            "<b>🚀 CineBro Status Report</b>\n\n"
            f"<b>🖥 CPU:</b> {cpu}%\n<b>📊 RAM:</b> {ram}%\n"
            f"<b>⏳ Uptime:</b> {uptime}\n"
            f"<b>🎬 Movies:</b> {total_movies}\n"
            f"<b>✅ Userbot:</b> Online"
        )
        await message.reply_text(status_text)
    except Exception as e:
        await message.reply_text(f"❌ Error: {e}")


@app.on_message(filters.command("help", prefixes=".") & (filters.user(ADMIN_ID)))
async def help_handler(client: Client, message: Message):
    try:
        help_text = (
            "<b>📘 CineBro Userbot Help</b>\n"
            "<i>Prefix:</i> <code>.</code>  |  <i>Admin-only:</i> Yes\n\n"
            "<b>Index / Clone</b>\n"
            "• <code>.index &lt;chat_id|@username|link&gt;</code> — index media from a chat/channel into MongoDB\n"
            "• <code>.clone &lt;source&gt; &lt;dest&gt;</code> — copy media from source → dest and index\n"
            "• <code>.clone_one &lt;source&gt; &lt;dest&gt; &lt;msg_id&gt;</code> — copy one message and index\n\n"
            "<b>Utilities</b>\n"
            "• <code>.id</code> — get chat/user/channel IDs (reply to a message for target ID)\n"
            "• <code>.status</code> — CPU/RAM/uptime + total indexed\n"
            "• <code>.stats</code> — users + movies + system stats\n\n"
            "<b>Admin Messaging</b>\n"
            "• <code>.broadcast &lt;text&gt;</code> — send a message to all users (or reply + <code>.broadcast</code>)\n"
            "• <code>.reply &lt;user_id&gt; &lt;text&gt;</code> — DM a specific user\n\n"
            "<b>Configuration</b>\n"
            "• <code>.env</code> — view/edit .env keys (sends audit logs to log group)\n\n"
            "<b>Danger Zone</b>\n"
            "• <code>.flush</code> — clear movies collection (cannot be undone)"
        )
        await message.reply_text(help_text)
    except Exception as e:
        await message.reply_text(f"❌ Error while showing help: {e}")

@app.on_message(filters.command("id", prefixes=".") & (filters.user(ADMIN_ID)))
async def id_handler(client: Client, message: Message):
    try:
        if message.reply_to_message:
            target = message.reply_to_message
            if target.from_user:
                await message.reply_text(f"<b>User ID:</b> <code>{target.from_user.id}</code>")
            elif target.sender_chat:
                await message.reply_text(f"<b>Channel/Group ID:</b> <code>{target.sender_chat.id}</code>")
            else:
                await message.reply_text("<b>ID:</b> Unknown")
        else:
            await message.reply_text(f"<b>Current Chat ID:</b> <code>{message.chat.id}</code>\n<b>Your ID:</b> <code>{message.from_user.id}</code>")
    except Exception as e:
        await message.reply_text(f"❌ Error getting ID: {e}")


@app.on_message(filters.command("index", prefixes=".") & (filters.user(ADMIN_ID)))
async def index_handler(client, message):
    try:
        if len(message.command) < 2:
            return await message.reply_text("<b>❌ Please provide a Channel ID or Username!</b>")
        
        raw_chat = message.command[1]
        msg = await message.reply_text("<b>🔍 Trying to resolve Peer...</b>")

        try:
            chat = await resolve_chat(client, raw_chat)
        except Exception as e:
            return await msg.edit(f"<b>❌ Failed to resolve source chat:</b> {e}")

        await msg.edit(f"<b>📂 Indexing: {html_escape(chat.title or '')}</b>\n<i>Please wait...</i>")

        processed_count = 0
        upserted_count = 0
        failed_count = 0
        async for user_msg in client.get_chat_history(chat.id):
            try:
                media = user_msg.document or user_msg.video
                if not media:
                    continue

                raw_text = getattr(media, "file_name", "") or getattr(user_msg, "caption", "") or ""
                metadata = parse_media_metadata(raw_text)
                movie_doc = {
                    "file_id": media.file_id,
                    "raw_file_name": getattr(media, "file_name", "") or raw_text,
                    "msg_id": user_msg.id,
                    "source_chat_id": chat.id,
                    "title": raw_text,
                    "clean_title": metadata["clean_title"],
                    "size": getattr(media, "file_size", 0),
                    "quality": metadata["quality"],
                    "language": metadata["language"],
                    "languages": metadata["languages"],
                    "season": metadata["season"],
                    "year": metadata["year"],
                }

                await upsert_movie_document(movie_doc)
                processed_count += 1
                upserted_count += 1

                if processed_count % 200 == 0:
                    try:
                        await msg.edit(
                            f"<b>⏳ Indexed {processed_count} files in {html_escape(chat.title or '')}...</b>"
                        )
                    except FloodWait as e:
                        await asyncio.sleep(int(getattr(e, "value", 0)) + 1)

                await asyncio.sleep(0.05)
            except FloodWait as e:
                await asyncio.sleep(int(getattr(e, "value", 0)) + 1)
            except Exception:
                failed_count += 1
                continue

        await msg.edit(
            "<b>✅ Indexing Complete!</b>\n"
            f"<b>Total Processed:</b> {processed_count}\n"
            f"<b>Total Upserted:</b> {upserted_count}\n"
            f"<b>Failed:</b> {failed_count}"
        )

    except Exception as e:
        await message.reply_text(f"❌ Error during indexing: {e}")


@app.on_message(filters.command("clone", prefixes=".") & (filters.user(ADMIN_ID)))
async def clone_handler(client: Client, message: Message):
    try:
        if len(message.command) < 3:
            return await message.reply_text("❌ Usage: <code>.clone &lt;source_chat_id&gt; &lt;dest_chat_id&gt;</code>")

        raw_source = message.command[1]
        raw_dest = message.command[2]
        progress = await message.reply_text("<b>🔄 Initializing clone process...</b>")

        try:
            source_chat = await resolve_chat(client, raw_source)
            dest_chat = await resolve_chat(client, raw_dest)
        except Exception as e:
            return await progress.edit(f"❌ Error resolving chats: {e}")

        await progress.edit(
            f"<b>📥 Source:</b> {html_escape(source_chat.title or '')}\n"
            f"<b>📤 Destination:</b> {html_escape(dest_chat.title or '')}\n"
            "<i>Cloning started...</i>"
        )

        cloned_count = 0
        skipped_count = 0
        failed_count = 0
        processed_media = 0

        async for src_msg in client.get_chat_history(source_chat.id):
            media = src_msg.document or src_msg.video
            if not media:
                continue

            try:
                processed_media += 1
                raw_text = getattr(media, "file_name", "") or getattr(src_msg, "caption", "") or ""
                raw_file_name = (getattr(media, "file_name", "") or raw_text).strip()
                file_size = int(getattr(media, "file_size", 0) or 0)

                existing = await movies_col.find_one(
                    {
                        "raw_file_name": raw_file_name,
                        "size": file_size,
                    },
                    {"_id": 1},
                )
                if existing:
                    print(f"[SKIP] '{raw_file_name}' already exists in DB.")
                    skipped_count += 1
                    if processed_media % 200 == 0:
                        try:
                            await progress.edit(
                                f"⏳ Processed {processed_media} files...\n"
                                f"✅ Cloned: {cloned_count}\n"
                                f"⏭ Skipped: {skipped_count}"
                            )
                        except FloodWait as e:
                                await asyncio.sleep(int(getattr(e, "value", 0)) + 1)
                    continue

                await asyncio.sleep(2.5)
                copied_msg = await safe_copy_message(client, dest_chat.id, source_chat.id, src_msg.id)

                metadata = parse_media_metadata(raw_text)
                movie_doc = {
                    "file_id": media.file_id,
                    "raw_file_name": raw_file_name,
                    "msg_id": copied_msg.id,
                    "source_chat_id": dest_chat.id,
                    "title": raw_text,
                    "clean_title": metadata["clean_title"],
                    "size": getattr(media, "file_size", 0),
                    "quality": metadata["quality"],
                    "language": metadata["language"],
                    "languages": metadata["languages"],
                    "season": metadata["season"],
                    "year": metadata["year"],
                }
                await upsert_movie_document(movie_doc)
                cloned_count += 1

                if processed_media % 200 == 0:
                    try:
                        await progress.edit(
                            f"⏳ Processed {processed_media} files...\n"
                            f"✅ Cloned: {cloned_count}\n"
                            f"⏭ Skipped: {skipped_count}"
                        )
                    except FloodWait as e:
                        await asyncio.sleep(int(getattr(e, "value", 0)) + 1)
            except Exception:
                failed_count += 1
                continue

        await progress.edit(
            "<b>✅ Clone Complete!</b>\n"
            f"<b>Cloned:</b> {cloned_count}\n"
            f"<b>Skipped:</b> {skipped_count}\n"
            f"<b>Failed:</b> {failed_count}"
        )
    except Exception as e:
        await message.reply_text(f"❌ Error during cloning: {e}")

@app.on_message(filters.command("flush", prefixes=".") & (filters.user(ADMIN_ID)))
async def flush_db(client, message):
    try:
        deleted = await flush_movies_collection()
        await message.reply_text(f"<b>🗑 Database flushed successfully!</b>\n<b>Deleted:</b> {deleted}")
    except Exception as e:
        await message.reply_text(f"❌ Flush Error: {e}")


# ============================================================================
# ADMIN COMMANDS (Userbot - Works with . prefix in any chat)
# ============================================================================

@app.on_message(filters.command("stats", prefixes=".") & (filters.user(ADMIN_ID)))
async def stats_cmd(client: Client, message: Message):
    """Admin command to view system stats and database info."""
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    uptime = time.strftime("%Hh %Mm %Ss", time.gmtime(time.time() - start_time))
    total_users = await get_total_users_count()
    total_movies = await get_total_movies_count()
    
    await message.reply_text(
        f"<b>📊 Admin Dashboard</b>\n\n"
        f"<b>👥 Total Users:</b> {total_users}\n"
        f"<b>🎬 Indexed Movies:</b> {total_movies}\n"
        f"<b>🖥 CPU Usage:</b> {cpu}%\n"
        f"<b>🐏 RAM Usage:</b> {ram}%\n"
        f"<b>⏱️ Uptime:</b> {uptime}"
    )

    await send_log(
        client,
        (
            "📊 <b>Userbot stats requested</b>\n"
            f"<b>By:</b> <a href='tg://user?id={message.from_user.id}'>Admin</a> (<code>{message.from_user.id}</code>)\n"
            f"<b>Users:</b> {total_users} | <b>Movies:</b> {total_movies}\n"
            f"<b>CPU:</b> {cpu}% | <b>RAM:</b> {ram}% | <b>Uptime:</b> {uptime}"
        ),
    )


@app.on_message(filters.command("broadcast", prefixes=".") & (filters.user(ADMIN_ID)))
async def broadcast_cmd(client: Client, message: Message):
    """Broadcast message to all bot users."""
    if len(message.command) < 2 and not message.reply_to_message:
        return await message.reply_text("Usage: .broadcast <message> or reply to a message with .broadcast")
    
    msg = await message.reply_text("Broadcast started...")
    succ = 0
    fail = 0
    users_cursor = await get_all_users()
    
    async for user in users_cursor:
        try:
            if message.reply_to_message:
                await message.reply_to_message.copy(user["user_id"])
            else:
                text = message.text.split(None, 1)[1]
                await client.send_message(user["user_id"], text)
            succ += 1
            await asyncio.sleep(0.1)
        except FloodWait as e:
            await asyncio.sleep(float(getattr(e, "value", 0)))
        except Exception:
            fail += 1

    await msg.edit_text(f"<b>📢 Broadcast complete!</b>\n✅ Success: {succ}\n❌ Failed: {fail}")

    await send_log(
        client,
        (
            "📣 <b>Broadcast finished</b>\n"
            f"<b>By:</b> <a href='tg://user?id={message.from_user.id}'>Admin</a> (<code>{message.from_user.id}</code>)\n"
            f"<b>Success:</b> {succ} | <b>Failed:</b> {fail}"
        ),
    )


@app.on_message(filters.command("reply", prefixes=".") & (filters.user(ADMIN_ID)))
async def reply_cmd(client: Client, message: Message):
    """Send direct message to specific user."""
    if len(message.command) < 3:
        return await message.reply_text("Usage: .reply <user_id> <message>")
    
    try:
        user_id = int(message.command[1])
        msg_text = message.text.split(None, 2)[2]
        safe_text = html_escape(msg_text)
        await client.send_message(user_id, f"<b>📩 Reply from Admin:</b>\n{safe_text}")
        await message.reply_text("✅ Message sent successfully.")

        await send_log(
            client,
            (
                "📩 <b>Admin replied to user</b>\n"
                f"<b>By:</b> <a href='tg://user?id={message.from_user.id}'>Admin</a> (<code>{message.from_user.id}</code>)\n"
                f"<b>To:</b> <code>{user_id}</code>"
            ),
        )
    except Exception as e:
        await message.reply_text(f"❌ Failed to send message: {e}")


@app.on_message(filters.command("env", prefixes=".") & (filters.user(ADMIN_ID)))
async def env_command(client: Client, message: Message):
    """
    View and manage environment variables.
    Shows configuration status and provides options to edit variables.
    """
    text = env_manager.get_env_summary()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Full", callback_data="env_full"),
         InlineKeyboardButton("✏️ Edit", callback_data="env_edit_menu")],
        [InlineKeyboardButton("📦 Backups", callback_data="env_backups")]
    ])
    await message.reply_text(text, reply_markup=keyboard)


@app.on_callback_query(filters.regex(r"^env_") & (filters.user(ADMIN_ID)))
async def env_callback(client: Client, call: CallbackQuery):
    """Handle environment configuration callbacks."""
    raw_data = call.data
    if isinstance(raw_data, (bytes, bytearray, memoryview)):
        data = bytes(raw_data).decode(errors="ignore")
    else:
        data = str(raw_data or "")
    
    if data == "env_full":
        text = env_manager.get_env_display()
        await call.message.edit_text(text)
    
    elif data == "env_edit_menu":
        keys = env_manager.get_editable_keys()
        buttons = [InlineKeyboardButton(key, callback_data=f"env_edit_{key}") for key in keys[:20]]
        rows = [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
        rows.append([InlineKeyboardButton("❌ Cancel", callback_data="env_cancel")])
        keyboard = InlineKeyboardMarkup(rows)
        
        await call.message.edit_text(
            "✏️ <b>Select variable to edit:</b>",
            reply_markup=keyboard
        )
    
    elif data.startswith("env_edit_"):
        key = data.replace("env_edit_", "")
        current_value = env_manager.get_key_value(key)
        current_display = env_manager._mask_value(key, current_value or "")
        
        await call.message.edit_text(
            f"✏️ <b>Edit: {key}</b>\n\n"
            f"<b>Current:</b> <code>{current_display}</code>\n\n"
            f"<b>📝 Reply with new value:</b>",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="env_edit_menu")]
            ])
        )
    
    elif data == "env_backups":
        text = env_manager.get_backup_list()
        await call.message.edit_text(text)
    
    elif data == "env_cancel":
        text = env_manager.get_env_summary()
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Full", callback_data="env_full"),
             InlineKeyboardButton("✏️ Edit", callback_data="env_edit_menu")],
            [InlineKeyboardButton("📦 Backups", callback_data="env_backups")]
        ])
        await call.message.edit_text(text, reply_markup=keyboard)


@app.on_message(filters.reply & (filters.user(ADMIN_ID)))
async def handle_env_edit(client: Client, message: Message):
    """Handle replies to env edit prompts from admin."""
    if not message.reply_to_message:
        return
    
    reply_text = message.reply_to_message.text or ""
    
    # Check if this is an env edit prompt
    if "✏️ <b>Edit:" not in reply_text:
        return
    
    # Extract the key name from the prompt
    match = re.search(r"✏️ <b>Edit: (\w+)</b>", reply_text)
    if not match:
        return
    
    key = match.group(1)
    old_value = env_manager.get_key_value(key)
    new_value = message.text.strip()
    
    # Update the value
    success, result_msg = env_manager.set_value(key, new_value)
    
    response = result_msg
    if success:
        response += "\n\n🔄 <b>Note:</b> Restart the bot to apply changes."
    
    await message.reply_text(response)

    if success:
        await send_log(client, format_env_change(message.from_user, key, old_value, new_value))


async def main():
    logger.info("Starting userbot dispatcher...")
    await setup_indexes()

    required = [
        "API_ID",
        "API_HASH",
        "SESSION_STRING",
        "MONGO_URI",
        "DB_NAME",
        "ADMIN_ID",
        "LOG_CHANNEL_ID",
    ]
    env_snapshot = {
        "API_ID": config.API_ID,
        "API_HASH": config.API_HASH,
        "SESSION_STRING": config.SESSION_STRING,
        "MONGO_URI": config.MONGO_URI,
        "DB_NAME": getattr(config, "DB_NAME", ""),
        "ADMIN_ID": config.ADMIN_ID,
        "LOG_CHANNEL_ID": getattr(config, "LOG_CHANNEL_ID", 0),
    }
    missing = find_missing_keys(env_snapshot, required)

    async with app:
        me = await app.get_me()
        me_username = (me.username or "").strip()
        me_label = f"@{me_username}" if me_username else (me.first_name or "(no-username)")
        logger.info("Online as %s (id=%s)", me_label, me.id)
        if LOG_CHANNEL_ID:
            await send_log(app, format_startup_report("Userbot", missing))

        from pyrogram import idle
        await idle()


@app.on_message(filters.command("clone_one", prefixes=".") & (filters.user(ADMIN_ID)))
async def clone_one_handler(client: Client, message: Message):
    try:
        if len(message.command) < 4:
            return await message.reply_text("❌ Usage: <code>.clone_one &lt;source_chat_id&gt; &lt;dest_chat_id&gt; &lt;msg_id&gt;</code>")

        raw_source = message.command[1]
        raw_dest = message.command[2]
        msg_id = int(message.command[3])
        
        progress = await message.reply_text("<b>🔄 Initializing clone_one process...</b>")

        try:
            source_chat = await resolve_chat(client, raw_source)
            dest_chat = await resolve_chat(client, raw_dest)
        except Exception as e:
            return await progress.edit(f"❌ Error resolving chats: {e}")

        src_msg = await client.get_messages(source_chat.id, msg_id)
        if not src_msg or not (src_msg.document or src_msg.video):
            return await progress.edit("❌ Message not found or does not contain media.")

        media = src_msg.document or src_msg.video
        raw_text = getattr(media, "file_name", "") or getattr(src_msg, "caption", "") or ""
        raw_file_name = (getattr(media, "file_name", "") or raw_text).strip()
        file_size = int(getattr(media, "file_size", 0) or 0)

        existing = await movies_col.find_one(
            {
                "raw_file_name": raw_file_name,
                "size": file_size,
            },
            {"_id": 1},
        )
        if existing:
            return await progress.edit(f"❌ File '{raw_file_name}' already exists in DB (Skipped).")

        copied_msg = await safe_copy_message(client, dest_chat.id, source_chat.id, src_msg.id)

        metadata = parse_media_metadata(raw_text)
        movie_doc = {
            "file_id": media.file_id,
            "raw_file_name": raw_file_name,
            "msg_id": copied_msg.id,
            "source_chat_id": dest_chat.id,
            "title": raw_text,
            "clean_title": metadata["clean_title"],
            "size": getattr(media, "file_size", 0),
            "quality": metadata["quality"],
            "language": metadata["language"],
            "languages": metadata["languages"],
            "season": metadata["season"],
            "year": metadata["year"],
        }
        await upsert_movie_document(movie_doc)
        
        await progress.edit("<b>✅ Clone One Complete!</b>\nFile successfully cloned and indexed.")
    except Exception as e:
        await message.reply_text(f"❌ Error during clone_one: {e}")


if __name__ == "__main__":
    try:
        app.run(main())
    except Exception as e:
        print(f"[FATAL] {e}")

