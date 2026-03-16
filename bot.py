import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
import config
from database import setup_indexes, get_fuzzy_search_cursor
from utils import is_rate_limited


def format_size(size_bytes: int) -> str:
    size = float(size_bytes or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.2f} {units[idx]}"

app = Client(
    "MovieSearchBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    await message.reply_text(
        "👋 Welcome to CineBro!\n\n"
        "Send any movie name and I will find and deliver matching files directly."
    )


@app.on_message(filters.private & ~filters.command(["start", "help"]))
async def search_and_deliver(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    query_text = (message.text or "").strip()

    if len(query_text) < 2:
        await message.reply_text("Please enter at least 2 characters to search.")
        return

    if is_rate_limited(user_id, 3):
        await message.reply_text("Please wait 3 seconds before searching again.")
        return

    searching_message = await message.reply_text("🔎 Searching...")

    sent_count = 0
    try:
        cursor = get_fuzzy_search_cursor(query_text, limit=20)
        async for item in cursor:
            file_id = item.get("file_id")
            if not file_id:
                continue

            title = item.get("title") or item.get("clean_title") or "Unknown"
            size = format_size(item.get("size", 0))
            caption = f"<b>Title:</b> {title}\n<b>Size:</b> {size}"
            await client.send_cached_media(
                chat_id=message.chat.id,
                file_id=file_id,
                caption=caption,
            )
            sent_count += 1

        if sent_count == 0:
            await searching_message.edit_text("😕 Sorry, I couldn't find anything for that query. Try a slightly different name.")
            return
    except Exception as exc:
        await searching_message.edit_text(f"❌ Search failed: {exc}")
        return

    await searching_message.delete()

async def main():
    print("Initializing Database Indexes...")
    await setup_indexes()
    
    print("Starting Telegram Bot Client...")
    await app.start()
    
    me = await app.get_me()
    print(f"✅ Bot Online as @{me.username}")
    
    # Idle until stopped
    from pyrogram import idle
    await idle()
    
    await app.stop()

if __name__ == "__main__":
    # Setup asyncio event loop manually to ensure DB indices apply properly
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
