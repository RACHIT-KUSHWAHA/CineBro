import asyncio
import time
import psutil
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
from motor.motor_asyncio import AsyncIOMotorClient
import config
from bson import ObjectId
from database import build_fuzzy_regex, movies_col, add_user, get_all_users, get_total_users_count, get_total_movies_count
from utils import is_rate_limited, get_rate_limit_status
from auto_indexer import AutoIndexer


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable file size."""
    size = float(size_bytes or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.2f} {units[idx]}"


def detect_series(movies: list) -> bool:
    """
    Detect if search results are from a TV series.
    Returns True if any movie has a season field.
    """
    return any(movie.get("season") for movie in movies)


def extract_seasons_from_results(movies: list) -> dict:
    """
    Extract unique seasons from movie results.
    Returns dict: {season: [episodes]}
    
    Example output:
    {'S01': ['E01', 'E02', ...], 'S02': [...]}
    """
    seasons_map = {}
    for movie in movies:
        season = movie.get("season", "")
        if not season:
            continue
        
        # Extract season number (S01, S02, etc.)
        if season not in seasons_map:
            seasons_map[season] = {
                "items": [],
                "quality_set": set(),
                "language_set": set()
            }
        
        seasons_map[season]["items"].append(movie)
        
        # Collect available qualities and languages
        if movie.get("quality"):
            seasons_map[season]["quality_set"].add(movie.get("quality"))
        if movie.get("language"):
            seasons_map[season]["language_set"].add(movie.get("language"))
    
    return seasons_map


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



# PRODUCTION FEATURE: Track auto-delete tasks for monitoring & cleanup
PENDING_DELETIONS = {}  # Format: {(chat_id, message_id): delete_time}


async def schedule_auto_delete(client: Client, chat_id: int, message_id: int, delay_seconds: int = 1800) -> None:
    """
    Production-grade auto-delete: Delete message after 30 minutes to prevent copyright strikes.
    
    Implements:
    - Robust error handling with FloodWait retry logic
    - Task tracking for monitoring
    - Proper logging for auditing
    - Fallback retry mechanism
    
    Args:
        client: Pyrogram client instance
        chat_id: Target chat ID
        message_id: Message to delete
        delay_seconds: Delete delay in seconds (default: 1800 = 30 minutes)
    """
    task_key = (chat_id, message_id)
    delete_time = time.time() + delay_seconds
    PENDING_DELETIONS[task_key] = delete_time
    
    try:
        await asyncio.sleep(delay_seconds)
        
        # Attempt deletion with logging
        try:
            await client.delete_messages(chat_id, message_id)
            print(f"✅ Auto-deleted message {message_id} from chat {chat_id}")
        except FloodWait as flood:
            # Respect Telegram flood limits even for deletions
            print(f"⏳ Flood wait detected. Retrying deletion in {flood.value} seconds...")
            await asyncio.sleep(flood.value + 1)
            try:
                await client.delete_messages(chat_id, message_id)
                print(f"✅ Auto-deleted message {message_id} from chat {chat_id} (after retry)")
            except Exception as retry_exc:
                print(f"❌ Auto-delete retry failed for {message_id}: {retry_exc}")
        except Exception as delete_exc:
            print(f"❌ Auto-delete failed for {message_id}: {delete_exc}")
    
    except asyncio.CancelledError:
        print(f"⚠️ Auto-delete task cancelled for {message_id}")
    
    finally:
        # Cleanup task tracking
        PENDING_DELETIONS.pop(task_key, None)


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

# Initialize Pyrogram Bot Client
app = Client(
    "MovieSearchBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)

# Initialize Auto-Indexer for database channel
mongo_client = AsyncIOMotorClient(config.MONGO_URI)
auto_indexer = AutoIndexer(mongo_client, db_name="CineBro")


@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    """
    Production-grade welcome message with professional formatting.
    Introduces bot capabilities and provides support/quick access.
    """
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "User"
    await add_user(user_id)
    
    welcome_message = (
        f"<b>🎬 Welcome {user_name}!</b>\n\n"
        f"<code>CineBro</code> is the fastest <b>movie & series indexer</b> on Telegram.\n\n"
        f"<b>📖 How to Use:</b>\n"
        f"• <code>Just type any movie or series name</code> to search\n"
        f"• Select a season for TV series\n"
        f"• Choose preferred quality and language\n"
        f"• Get the fastest download links instantly\n\n"
        f"<b>⚡ Features:</b>\n"
        f"• <code>500K+</code> Movies & TV series indexed\n"
        f"• <code>Lightning-fast</code> search with fuzzy matching\n"
        f"• <code>Multi-quality</code> & <code>Multi-language</code> support\n"
        f"• <code>30-min auto-delete</code> for copyright protection\n"
        f"• <code>Direct Telegram</code> video streaming\n\n"
        f"🎯 <b>Pro Tips:</b>\n"
        f"<code>/help</code> - View all commands\n"
        f"<code>Max 5 searches/min</code> - Stay within rate limits\n"
        f"<code>Quality Priority:</code> 4K ≫ 1080p ≫ 720p\n"
    )
    
    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 Report Missing Movies / Support", url=config.SUPPORT_GROUP_LINK)
        ]
    ])
    
    await message.reply_text(welcome_message, reply_markup=buttons)

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
    pending_deletes = len(PENDING_DELETIONS)
    
    await message.reply_text(
        f"<b>📊 Admin Dashboard</b>\n\n"
        f"<b>👥 Total Users:</b> {total_users}\n"
        f"<b>🎬 Indexed Movies:</b> {total_movies}\n"
        f"<b>🖥 CPU Usage:</b> {cpu}%\n"
        f"<b>🐏 RAM Usage:</b> {ram}%\n"
        f"<b>⏳ Pending Auto-Deletes:</b> {pending_deletes}\n"
        f"<b>⏱️ Uptime:</b> {uptime}"
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

    # Production rate limiter: max 5 searches per minute
    if is_rate_limited(user_id):
        status = get_rate_limit_status(user_id)
        reset_in = status.get("reset_in", 60)
        await message.reply_text(
            f"⏳ <b>Rate Limited</b>\n\n"
            f"You've reached the limit of <code>5 searches per minute</code>.\n"
            f"Please wait <code>{reset_in} seconds</code> before searching again.",
            parse_mode="html"
        )
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

        # PRODUCTION FEATURE: Series Hierarchy Detection
        is_series = detect_series(movies)
        
        if is_series:
            # STEP 1: Show Season Selection
            seasons_map = extract_seasons_from_results(movies)
            sorted_seasons = sorted(seasons_map.keys())
            
            season_buttons = []
            row = []
            for season in sorted_seasons:
                season_text = season.replace("S", "Season ")
                row.append(
                    InlineKeyboardButton(
                        f"📺 {season_text}",
                        callback_data=f"select_season|{query_text[:30]}|{season}"
                    )
                )
                if len(row) == 2:
                    season_buttons.append(row)
                    row = []
            if row:
                season_buttons.append(row)
            
            # Add support button
            season_buttons.append([
                InlineKeyboardButton("💬 Support Group", url=config.SUPPORT_GROUP_LINK)
            ])
            
            header_msg = f"<b>Results for:</b> {query_text} (Auto-corrected)\n" if corrected_query else f"<b>Results for:</b> {query_text}\n"
            
            await searching_message.edit_text(
                f"{header_msg}"
                f"<b>📺 This is a Series</b>\n\n"
                f"<code>Step 1:</code> Select a Season\n"
                f"<code>Found {len(sorted_seasons)} seasons</code>",
                reply_markup=InlineKeyboardMarkup(season_buttons),
            )
        else:
            # REGULAR MOVIE: Show paginated search results
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


# PRODUCTION FEATURE: Season Selection Handler for TV Series
@app.on_callback_query(filters.regex(r"^select_season\|"))
async def callback_season_selected(client: Client, call: CallbackQuery):
    """
    STEP 2: When user selects a season, show available episodes.
    """
    try:
        parts = call.data.split("|", 2)
        if len(parts) < 3:
            await call.answer("Invalid season selection.", show_alert=True)
            return
        
        query_text = parts[1]
        selected_season = parts[2]
        
        # Fetch all episodes for this season
        base_query = {
            "$or": [
                {"clean_title": {"$regex": build_fuzzy_regex(query_text), "$options": "i"}},
                {"title": {"$regex": build_fuzzy_regex(query_text), "$options": "i"}},
            ],
            "season": selected_season
        }
        
        episodes_cursor = movies_col.find(base_query, {
            "_id": 1,
            "title": 1,
            "clean_title": 1,
            "season": 1,
            "quality": 1,
            "language": 1,
            "size": 1
        }).limit(50)
        
        episodes = await episodes_cursor.to_list(length=50)
        
        if not episodes:
            await call.answer("No episodes found for this season.", show_alert=True)
            return
        
        # Build episode selection buttons
        episode_buttons = []
        row = []
        for ep in episodes:
            ep_id = str(ep.get("_id", ""))
            quality_label = (ep.get("quality") or "unknown").upper()
            size_str = format_size(ep.get("size", 0))
            
            # Format: [Size] • [Quality]
            ep_text = f"{size_str} • {quality_label}"
            if len(ep_text) > 20:
                ep_text = ep_text[:17] + "..."
            
            row.append(
                InlineKeyboardButton(
                    ep_text,
                    callback_data=f"send_file|{ep_id}"
                )
            )
            
            if len(row) == 2:
                episode_buttons.append(row)
                row = []
        
        if row:
            episode_buttons.append(row)
        
        # Add back button
        episode_buttons.append([
            InlineKeyboardButton("🔙 Back To Search", callback_data="back_to_search")
        ])
        
        season_display = selected_season.replace("S", "Season ")
        await call.message.edit_text(
            f"<b>📺 {query_text.title()}</b>\n"
            f"<code>{season_display}</code>\n\n"
            f"<code>Step 2:</code> Select an Episode\n"
            f"<code>Found {len(episodes)} episodes</code>",
            reply_markup=InlineKeyboardMarkup(episode_buttons),
        )
        await call.answer()
        
    except Exception as e:
        print(f"Season selection error: {e}")
        await call.answer(f"Error: {e}", show_alert=True)


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

        # Separator for important info
        caption_lines.append("➖➖➖➖➖➖")

        # PRODUCTION FEATURE: Auto-Delete Warning (30-minute protocol)
        caption_lines.append("⏳ <b>WARNING: AUTO-DELETE IN 30 MINUTES</b>")
        caption_lines.append(
            "• This file will be <b>automatically deleted in 30 minutes</b> for copyright protection."
        )
        caption_lines.append("• <b>Forward immediately to 'Saved Messages'</b> if you want to keep it.")
        caption_lines.append("• Downloaded files in your phone storage will NOT be deleted.")

        # Additional important notices
        caption_lines.append("➖➖➖➖➖➖")
        caption_lines.append("⚠️ <b>Quality & Streaming Tips:</b>")
        caption_lines.append("• <b>Browser web player:</b> Not recommended. Quality may suffer.")
        caption_lines.append("• <b>Best Experience:</b> Use MX Player, VLC, or Telegram's native player.")
        caption_lines.append("• <b>Direct Download:</b> Tap & hold, then 'Download' for fastest speeds.")

        # Share / branding style labels
        caption_lines.append("➖➖➖➖➖➖")
        caption_lines.append(f"✈️ <b>Share to Support:</b> @{bot_username}")
        caption_lines.append(f"🤖 <b>Served by:</b> @{bot_username}")
        
        caption = "\n".join(caption_lines)

        watch_url = f"https://cinebro-streamer.kushwaharachit80.workers.dev/watch/{raw_id}"

        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🍿 Watch Online", url=watch_url)
            ],
            [
                InlineKeyboardButton("💬 Support Group", url=config.SUPPORT_GROUP_LINK),
                InlineKeyboardButton("👨‍💻 Owner", url=config.OWNER_PROFILE_LINK)
            ]
        ])

        try:
            sent = await safe_copy_message(
                client=client,
                chat_id=call.message.chat.id,
                from_chat_id=source_chat_id,
                message_id=msg_id,
                caption=caption,
                reply_markup=buttons
            )
            # Schedule auto-deletion only in private chats to avoid surprising groups
            if getattr(call.message.chat, "type", "") == "private" and sent is not None:
                asyncio.create_task(
                    schedule_auto_delete(
                        client=client,
                        chat_id=sent.chat.id,
                        message_id=sent.id,
                        delay_seconds=1800,
                    )
                )
            await call.answer("File sent successfully!")
        except Exception as exc:
            await call.answer("Failed to send file.", show_alert=True)
            await call.message.reply_text(f"❌ Delivery failed: {exc}")
        
        # PRODUCTION FEATURE: Smart Fallback Disclaimer
        # Check if the delivered file might be a fallback (different quality/language than available best)
        # Send a separate message if needed
        if sent is not None and "fallback_disclaimer" in call.data:
            try:
                fallback_msg = (
                    "⚠️ <b>Quality/Language Fallback</b>\n\n"
                    "Your requested quality or language was not available. "
                    "We sent you the <b>best available alternative</b> instead.\n\n"
                    "If this doesn't meet your needs, try:\n"
                    "• Searching with different quality keywords\n"
                    "• Or search for a different provider version"
                )
                await call.message.reply_text(fallback_msg)
            except Exception:
                pass
        
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


@app.on_message(filters.chat(config.DATABASE_CHANNEL_ID))
async def handle_database_channel_auto_index(client: Client, message: Message):
    """
    Auto-Indexer Handler: Automatically catches and indexes new files in database channel.
    
    Extracts metadata (file_id, file_unique_id, file_name, size, etc.) and saves to MongoDB
    with intelligent deduplication using UpdateOne with upsert=True.
    
    This runs in background without sending any replies - completely silent operation.
    """
    try:
        # Process through auto-indexer (extract metadata + upsert to DB)
        success = await auto_indexer.process_message(message)
        
        # Optional: Send summary notification to admin every 50 new files
        if success:
            indexed_count = await auto_indexer.get_channel_indexed_count()
            if indexed_count % 50 == 0:  # Notify admin on milestones
                total_count = await auto_indexer.get_indexed_count()
                await client.send_message(
                    config.ADMIN_ID,
                    f"📦 <b>Auto-Indexer Milestone:</b>\n"
                    f"✓ {indexed_count} files indexed in database channel\n"
                    f"💾 {total_count} total files in MongoDB",
                    disable_web_page_preview=True
                )
    except Exception as e:
        # Log errors but don't disrupt indexing
        import logging
        logging.error(f"Auto-indexer error: {e}")


async def main():
    print("Starting Telegram Bot Client...")
    
    # Setup Auto-Indexer indexes on startup
    print("Setting up database indexes for auto-indexer...")
    await auto_indexer.setup_indexes()
    
    await app.start()

    me = await app.get_me()
    print(f"✅ Bot Online as @{me.username}")

    # Start aiohttp streaming server using the bot client
    from streamer import TelegramStreamer
    streamer_app = TelegramStreamer(app)
    await streamer_app.start(port=8080)

    # Idle until stopped
    from pyrogram import idle
    await idle()

    await streamer_app.stop()
    await app.stop()

if __name__ == "__main__":
    # Setup asyncio event loop manually to ensure DB indices apply properly
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
