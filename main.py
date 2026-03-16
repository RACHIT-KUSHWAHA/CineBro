import asyncio
import re
import time
from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.errors import FloodWait, PeerIdInvalid
import config
from database import upsert_movie_document, flush_movies_collection, get_total_movies_count, setup_indexes, movies_col

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

app = Client("userbot_main", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

QUALITY_PATTERN = re.compile(r"\b(480p|720p|1080p|2160p|4k)\b", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
LANG_TOKEN_PATTERN = re.compile(
    r"(dual(?:\s*audio)?|multi(?:\s*audio)?|hindi|english|tamil|telugu|malayalam|kannada|bengali|punjabi|marathi)",
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

    clean_title = text
    clean_title = YEAR_PATTERN.sub(" ", clean_title)
    
    for mat in LANG_TOKEN_PATTERN.finditer(clean_title):
        clean_title = clean_title.replace(mat.group(0), " ")
        
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
            await asyncio.sleep(flood.value + 2)

@app.on_message(filters.all, group=-100)
async def log_every_message(_: Client, message: Message):
    text = message.text or message.caption or "<non-text message>"
    print(f"[LOG] Message received: {text}")

@app.on_message(filters.command("status", prefixes=".") & (filters.me | filters.user(ADMIN_ID)))
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


@app.on_message(filters.command("help", prefixes=".") & (filters.me | filters.user(ADMIN_ID)))
async def help_handler(client: Client, message: Message):
    try:
        help_text = (
            "<b>📘 CineBro Userbot Commands</b>\n\n"
            "<b>1. .index &lt;source_chat_id_or_username&gt;</b>\n"
            "Indexes media from a source chat into MongoDB.\n"
            "<i>Example:</i> <code>.index -1001234567890</code>\n\n"
            "<b>2. .clone &lt;source_chat_id&gt; &lt;dest_chat_id&gt;</b>\n"
            "Stealth-clones media with safe delay and stores new pointers in DB.\n"
            "<i>Example:</i> <code>.clone -1001111111111 -1002222222222</code>\n\n"
            "<b>3. .status</b>\n"
            "Shows CPU, RAM, uptime, and total indexed movies.\n\n"
            "<b>4. .flush</b>\n"
            "Clears the movies collection safely.\n"
            "<i>Example:</i> <code>.flush</code>"
        )
        await message.reply_text(help_text)
    except Exception as e:
        await message.reply_text(f"❌ Error while showing help: {e}")

@app.on_message(filters.command("index", prefixes=".") & (filters.me | filters.user(ADMIN_ID)))
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

        await msg.edit(f"<b>📂 Indexing: {chat.title}</b>\n<i>Please wait...</i>")

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
                            f"<b>⏳ Indexed {processed_count} files in {chat.title}...</b>"
                        )
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 1)

                await asyncio.sleep(0.05)
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
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


@app.on_message(filters.command("clone", prefixes=".") & (filters.me | filters.user(ADMIN_ID)))
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
            f"<b>📥 Source:</b> {source_chat.title}\n"
            f"<b>📤 Destination:</b> {dest_chat.title}\n"
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
                            await asyncio.sleep(e.value + 1)
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
                        await asyncio.sleep(e.value + 1)
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

@app.on_message(filters.command("flush", prefixes=".") & (filters.me | filters.user(ADMIN_ID)))
async def flush_db(client, message):
    try:
        deleted = await flush_movies_collection()
        await message.reply_text(f"<b>🗑 Database flushed successfully!</b>\n<b>Deleted:</b> {deleted}")
    except Exception as e:
        await message.reply_text(f"❌ Flush Error: {e}")

async def main():
    print("[LOG] Starting userbot dispatcher...")
    await setup_indexes()
    await app.start()
    print("[LOG] Online as @BeyondRachit")
    await idle()
    await app.stop()

if __name__ == "__main__":
    try:
        app.run(main())
    except Exception as e:
        print(f"[FATAL] {e}")