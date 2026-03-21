import asyncio
import re
import time
import os
import psutil
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
import config
from bson import ObjectId
from database import build_fuzzy_regex, movies_col, add_user, get_all_users, get_total_users_count, get_total_movies_count
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


PAGE_SIZE = 10
start_time = time.time()

def build_base_query(query_text: str) -> dict:
    pattern = build_fuzzy_regex(query_text)
    return {
        "$or": [
            {"clean_title": {"$regex": pattern, "$options": "i"}},
            {"title": {"$regex": pattern, "$options": "i"}},
        ]
    } if pattern else {"_id": None}


async def safe_copy_message(client: Client, chat_id: int, from_chat_id: int, message_id: int, caption: str, reply_markup=None):
    while True:
        try:
            return await client.copy_message(
                chat_id=chat_id,
                from_chat_id=from_chat_id,
                message_id=message_id,
                caption=caption,
                reply_markup=reply_markup
            )
        except FloodWait as flood:
            await asyncio.sleep(flood.value + 1)


async def fetch_page(query_text: str, page: int):
    base_query = build_base_query(query_text)
    projection = {
        "title": 1,
        "clean_title": 1,
        "source_chat_id": 1,
        "msg_id": 1,
        "size": 1,
        "season": 1,
        "quality": 1,
        "language": 1
    }

    total = await movies_col.count_documents(base_query)
    cursor = movies_col.find(base_query, projection).skip(page * PAGE_SIZE).limit(PAGE_SIZE)
    movies = await cursor.to_list(length=PAGE_SIZE)
    return movies, total


def build_results_keyboard(query_text: str, page: int, movies: list, total: int) -> InlineKeyboardMarkup:
    rows = []

    for movie in movies:
        title = (movie.get("title") or movie.get("clean_title") or "Unknown").strip()
        quality_label = str(movie.get("quality", "unknown")).upper()
        if quality_label == "UNKNOWN": quality_label = "None"
        season_val = movie.get("season", "")
        
        if season_val:
            suffix = f" - {season_val} ({quality_label})"
        else:
            suffix = f" ({quality_label})"
            
        max_title_len = 59 - len(suffix)
        if len(title) > max_title_len:
            title = title[:max_title_len - 3] + "..."
            
        btn_text = f"{title}{suffix}"
        
        rows.append([
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"send_file|{str(movie.get('_id'))}",
            )
        ])

    max_page = (total - 1) // PAGE_SIZE if total else 0
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"page|{query_text[:40]}|{page - 1}")
        )
    if page < max_page:
        nav_buttons.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"page|{query_text[:40]}|{page + 1}")
        )

    if nav_buttons:
        rows.append(nav_buttons)

    # Adding Owner and Support Group buttons to Search Results
    rows.append([
        InlineKeyboardButton("💬 Support Group", url=config.SUPPORT_GROUP_LINK),
        InlineKeyboardButton("👨‍💻 Owner", url=config.OWNER_PROFILE_LINK)
    ])

    return InlineKeyboardMarkup(rows)

app = Client(
    "MovieSearchBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    await message.reply_text(
        "👋 Welcome to CineBro!\n\n"
        "Send any movie name and I will find and deliver matching files directly.\n"
        "Use /help to see more options."
    )

@app.on_message(filters.command("help") & filters.private)
async def help_cmd(client: Client, message: Message):
    help_text = (
        "<b>🎬 CineBro Help Menu</b>\n\n"
        "Just send me any movie or series name and I will find it for you!\n"
    )
    if message.from_user and message.from_user.id == config.ADMIN_ID:
        help_text += (
            "\n<b>👑 Admin Commands:</b>\n"
            "<code>/stats</code> - Dashboard with CPU, RAM, Users, and Movies\n"
            "<code>/broadcast &lt;msg&gt;</code> - Mass message all users (or reply to a msg)\n"
            "<code>/reply &lt;user_id&gt; &lt;msg&gt;</code> - Message a specific user\n"
        )
    await message.reply_text(help_text)

@app.on_message(filters.command("stats") & filters.user(config.ADMIN_ID) & filters.private)
async def stats_cmd(client: Client, message: Message):
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
        f"<b>⏳ Uptime:</b> {uptime}"
    )

@app.on_message(filters.command("broadcast") & filters.user(config.ADMIN_ID) & filters.private)
async def broadcast_cmd(client: Client, message: Message):
    if len(message.command) < 2 and not message.reply_to_message:
        return await message.reply_text("Please provide a message or reply to a message to broadcast.")
    
    msg = await message.reply_text("Broadcast started...")
    succ = 0
    fail = 0
    users_cursor = await get_all_users()
    
    async for user in users_cursor:
        try:
            if message.reply_to_message:
                await message.reply_to_message.copy(user["user_id"])
            else:
                await client.send_message(user["user_id"], message.text.split(None, 1)[1])
            succ += 1
            await asyncio.sleep(0.1)
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception:
            fail += 1

    await msg.edit_text(f"Broadcast complete!\nSuccess: {succ}\nFailed: {fail}")

@app.on_message(filters.command("reply") & filters.user(config.ADMIN_ID) & filters.private)
async def reply_cmd(client: Client, message: Message):
    if len(message.command) < 3:
        return await message.reply_text("Usage: /reply <user_id> <message>")
    
    try:
        user_id = int(message.command[1])
        msg_text = message.text.split(None, 2)[2]
        await client.send_message(user_id, f"<b>📩 Reply from Admin:</b>\n{msg_text}")
        await message.reply_text("✅ Message sent successfully.")
    except Exception as e:
        await message.reply_text(f"❌ Failed to send message: {e}")

@app.on_message(filters.private & ~filters.command(["start", "help", "stats", "broadcast", "reply"]))
async def search_and_deliver(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    query_text = (message.text or "").strip()

    await add_user(user_id)

    if config.LOG_CHANNEL_ID:
        try:
            name = message.from_user.first_name if message.from_user else "Unknown"
            await client.send_message(
                config.LOG_CHANNEL_ID,
                f"<b>🔍 New Search</b>\n<b>User:</b> <a href='tg://user?id={user_id}'>{name}</a> (`{user_id}`)\n<b>Query:</b> {query_text}"
            )
        except Exception as e:
            print(f"Log Error: {e}")

    if len(query_text) < 2:
        await message.reply_text("Please enter at least 2 characters to search.")
        return

    if is_rate_limited(user_id, 3):
        await message.reply_text("Please wait 3 seconds before searching again.")
        return

    searching_message = await message.reply_text("🔎 Searching...")

    try:
        movies, total = await fetch_page(query_text, 0)
        
        # --- IMDB SPELL CHECK FALLBACK ---
        corrected_query = None
        if not movies:
            try:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    url = f"https://v3.sg.media-imdb.com/suggestion/x/{query_text.lower()}.json"
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for item in data.get('d', []):
                                if item.get('qid') in ('movie', 'tvSeries', 'tvMiniSeries'):
                                    suggestion = item.get('l')
                                    if suggestion and suggestion.lower() != query_text.lower():
                                        smovies, stotal = await fetch_page(suggestion, 0)
                                        if smovies:
                                            movies = smovies
                                            total = stotal
                                            corrected_query = suggestion
                                            break
            except Exception as e:
                print(f"IMDB Search Error: {e}")
            
            if corrected_query:
                query_text = corrected_query
        # ---------------------------------

        if not movies:
            await searching_message.edit_text("😕 Sorry, I couldn't find anything for that query. Try a slightly different name.")
            return

        max_page = (total - 1) // PAGE_SIZE if total else 0
        keyboard = build_results_keyboard(query_text, 0, movies, total)
        
        header_text = f"<b>Results for:</b> {query_text} (Auto-corrected)\n" if corrected_query else f"<b>Results for:</b> {query_text}\n"
        
        await searching_message.edit_text(
            f"{header_text}"
            f"<b>Page:</b> 1/{max_page + 1}\n"
            "Select a file to receive:",
            reply_markup=keyboard,
        )
    except Exception as exc:
        await searching_message.edit_text(f"❌ Search failed: {exc}")
        return


@app.on_callback_query()
async def callback_router(client: Client, call: CallbackQuery):
    data = call.data or ""

    if data.startswith("send_file|"):
        parts = data.split("|", 1)
        if len(parts) < 2:
            await call.answer("Invalid file request.", show_alert=True)
            return
        raw_id = parts[1]
        try:
            movie = await movies_col.find_one({"_id": ObjectId(raw_id)})
        except Exception:
            movie = None

        if not movie:
            await call.answer("This file is no longer available.", show_alert=True)
            return

        source_chat_id = movie.get("source_chat_id")
        msg_id = movie.get("msg_id")
        if not source_chat_id or not msg_id:
            await call.answer("Invalid file pointer in database.", show_alert=True)
            return

        title = movie.get("title") or movie.get("clean_title") or "Unknown"
        bot_me = await client.get_me()
        bot_username = bot_me.username
        
        season_val = movie.get("season", "")
        quality_val = movie.get("quality", "unknown")
        size = format_size(movie.get("size", 0))
        
        langs = movie.get("languages", [])
        if not langs:
            lang_val = movie.get("language", "unknown")
            langs = [lang_val] if lang_val else ["unknown"]
            
        language_str = ", ".join(str(l).title() for l in langs if l and str(l).lower() != "unknown")

        caption_lines = [
            f"🎬 <b>Title:</b> {title}"
        ]
        if season_val:
            caption_lines.append(f"📺 <b>Season:</b> {season_val}")
        if quality_val and str(quality_val).lower() not in ["none", "unknown"]:
            caption_lines.append(f"💿 <b>Quality:</b> {str(quality_val).upper()}")
        if language_str:
            caption_lines.append(f"🗣 <b>Language:</b> {language_str}")
        if size and size != "0 B":
            caption_lines.append(f"💾 <b>Size:</b> {size}")
            
        caption_lines.append("")
        caption_lines.append(f"🤖 <b>Downloaded via:</b> @{bot_username}")
        
        caption = "\n".join(caption_lines)

        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("💬 Support Group", url=config.SUPPORT_GROUP_LINK),
                InlineKeyboardButton("👨‍💻 Owner", url=config.OWNER_PROFILE_LINK)
            ]
        ])

        try:
            await safe_copy_message(
                client=client,
                chat_id=call.message.chat.id,
                from_chat_id=source_chat_id,
                message_id=msg_id,
                caption=caption,
                reply_markup=buttons
            )
            await call.answer("File sent successfully!")
        except Exception as exc:
            await call.answer("Failed to send file.", show_alert=True)
            await call.message.reply_text(f"❌ Delivery failed: {exc}")
        return

    if data.startswith("page|"):
        parts = data.split("|", 2)
        if len(parts) != 3:
            await call.answer("Invalid page request.", show_alert=True)
            return

        query_text = parts[1]
        try:
            page = int(parts[2])
        except ValueError:
            await call.answer("Invalid page number.", show_alert=True)
            return
        if page < 0:
            page = 0

        try:
            movies, total = await fetch_page(query_text, page)
            if not movies:
                await call.answer("No more results.", show_alert=True)
                return

            max_page = (total - 1) // PAGE_SIZE if total else 0
            keyboard = build_results_keyboard(query_text, page, movies, total)
            await call.message.edit_text(
                f"<b>Results for:</b> {query_text}\n"
                f"<b>Page:</b> {page + 1}/{max_page + 1}\n"
                "Select a file to receive:",
                reply_markup=keyboard,
            )
            await call.answer()
        except Exception as exc:
            await call.answer("Failed to load page.", show_alert=True)
            await call.message.reply_text(f"❌ Pagination error: {exc}")
        return


async def main():
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
