import asyncio
import time
from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.errors import FloodWait, PeerIdInvalid, ChannelPrivate, InviteHashInvalid
import config
from indexer import extract_metadata
from database import upsert_movie_document, flush_movies_collection, get_total_movies_count, setup_indexes

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

@app.on_message(filters.command("index", prefixes=".") & (filters.me | filters.user(ADMIN_ID)))
async def index_handler(client, message):
    try:
        if len(message.command) < 2:
            return await message.reply_text("<b>❌ Please provide a Channel ID or Username!</b>")
        
        raw_chat = message.command[1]
        msg = await message.reply_text("<b>🔍 Trying to resolve Peer...</b>")

        # PEER RESOLUTION LOGIC
        try:
            chat = await client.get_chat(raw_chat)
        except PeerIdInvalid:
            await msg.edit("<b>⚠️ Peer unknown. Trying to join...</b>")
            try:
                # Agar private channel hai toh link se join karne ki koshish karega
                chat = await client.join_chat(raw_chat)
            except Exception as e:
                return await msg.edit(f"<b>❌ Failed to join:</b> {e}\nJoin the channel manually first!")

        await msg.edit(f"<b>📂 Indexing: {chat.title}</b>\n<i>Please wait...</i>")

        processed_count = 0
        upserted_count = 0
        async for user_msg in client.get_chat_history(chat.id):
            media = user_msg.document or user_msg.video
            if not media:
                continue

            raw_text = getattr(media, "file_name", "") or getattr(user_msg, "caption", "") or ""
            metadata = extract_metadata(raw_text)
            movie_doc = {
                "file_id": media.file_id,
                "msg_id": user_msg.id,
                "source_chat_id": chat.id,
                "title": raw_text,
                "clean_title": metadata["clean_title"],
                "size": getattr(media, "file_size", 0),
                "quality": metadata["quality"],
                "language": metadata["language"],
                "year": metadata["year"],
            }

            await upsert_movie_document(movie_doc)
            processed_count += 1
            upserted_count += 1

            if processed_count % 100 == 0:
                try:
                    await msg.edit(
                        f"<b>⏳ Processed {processed_count} files in {chat.title}...</b>"
                    )
                except FloodWait as e:
                    await asyncio.sleep(e.value)

            # Critical throttle to keep CPU stable during long indexing loops.
            await asyncio.sleep(0.05)

        await msg.edit(
            "<b>✅ Indexing Complete!</b>\n"
            f"<b>Total Processed:</b> {processed_count}\n"
            f"<b>Total Upserted:</b> {upserted_count}"
        )

    except Exception as e:
        print(f"[ERROR] Indexing failed: {e}")
        await message.reply_text(f"❌ Indexing Error: {e}")

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