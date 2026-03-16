import asyncio
import re
import time
import uuid
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
import config
from bson import ObjectId
from database import setup_indexes, build_fuzzy_regex, movies_col
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
SESSION_TTL_SECONDS = 1800
MAX_SESSION_CACHE = 5000

SEARCH_SESSIONS = {}


def clear_old_sessions() -> None:
    now = time.time()
    if len(SEARCH_SESSIONS) <= MAX_SESSION_CACHE:
        return
    stale_keys = [k for k, v in SEARCH_SESSIONS.items() if now - v.get("ts", now) > SESSION_TTL_SECONDS]
    for key in stale_keys:
        del SEARCH_SESSIONS[key]


def normalize_quality(value: str) -> str:
    q = (value or "").strip().lower()
    return "2160p" if q == "4k" else (q or "unknown")


def normalize_language(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def to_lang_label(value: str) -> str:
    if value == "all":
        return "All"
    if value == "multi":
        return "Multi"
    return value.title()


def to_quality_label(value: str) -> str:
    if value == "all":
        return "All"
    if value == "unknown":
        return "Unknown"
    return value.upper()


def parse_language_string(raw_language: str) -> list:
    parts = [x.strip().lower() for x in re.split(r"[\s,/|&]+", raw_language or "") if x.strip()]
    return parts


async def fetch_available_filters(base_query: dict):
    languages = set()
    qualities = set()

    lang_list = await movies_col.distinct("languages", base_query)
    for lang in lang_list:
        if isinstance(lang, str):
            val = normalize_language(lang)
            if val:
                languages.add(val)

    if not languages:
        language_field_values = await movies_col.distinct("language", base_query)
        for raw in language_field_values:
            if not isinstance(raw, str):
                continue
            for token in parse_language_string(raw):
                languages.add(token)

    quality_values = await movies_col.distinct("quality", base_query)
    for qual in quality_values:
        if isinstance(qual, str):
            qualities.add(normalize_quality(qual))

    lang_options = ["all"] + sorted([x for x in languages if x])
    quality_rank = {"2160p": 5, "1080p": 4, "720p": 3, "480p": 2, "unknown": 1}
    quality_options = ["all"] + sorted([x for x in qualities if x], key=lambda x: quality_rank.get(x, 0), reverse=True)
    return lang_options, quality_options


def build_base_query(query_text: str) -> dict:
    pattern = build_fuzzy_regex(query_text)
    return {
        "$or": [
            {"clean_title": {"$regex": pattern, "$options": "i"}},
            {"title": {"$regex": pattern, "$options": "i"}},
        ]
    } if pattern else {"_id": None}


def build_filter_query(base_query: dict, selected_lang: str, selected_quality: str) -> dict:
    query = dict(base_query)
    filters_list = []

    lang = normalize_language(selected_lang)
    qual = normalize_quality(selected_quality)

    if lang and lang != "all":
        lang_regex = rf"(^|[\s,/|&]){re.escape(lang)}($|[\s,/|&])"
        filters_list.append({
            "$or": [
                {"languages": lang},
                {"language": {"$regex": lang_regex, "$options": "i"}},
            ]
        })

    if qual and qual != "all":
        filters_list.append({"quality": qual})

    if filters_list:
        if "$and" in query:
            query["$and"].extend(filters_list)
        else:
            query["$and"] = filters_list
    return query


def cycle_option(current: str, options: list) -> str:
    if not options:
        return "all"
    if current not in options:
        return options[0]
    idx = options.index(current)
    return options[(idx + 1) % len(options)]


async def safe_copy_message(client: Client, chat_id: int, from_chat_id: int, message_id: int, caption: str):
    while True:
        try:
            return await client.copy_message(
                chat_id=chat_id,
                from_chat_id=from_chat_id,
                message_id=message_id,
                caption=caption,
            )
        except FloodWait as flood:
            await asyncio.sleep(flood.value + 1)


async def fetch_page(query_text: str, page: int, selected_lang: str = "all", selected_quality: str = "all"):
    base_query = build_base_query(query_text)
    mongo_query = build_filter_query(base_query, selected_lang, selected_quality)
    projection = {
        "title": 1,
        "clean_title": 1,
        "source_chat_id": 1,
        "msg_id": 1,
        "size": 1,
    }

    total = await movies_col.count_documents(mongo_query)
    cursor = movies_col.find(mongo_query, projection).skip(page * PAGE_SIZE).limit(PAGE_SIZE)
    movies = await cursor.to_list(length=PAGE_SIZE)
    return movies, total


def build_results_keyboard(session_id: str, page: int, selected_lang: str, selected_quality: str, movies: list, total: int) -> InlineKeyboardMarkup:
    rows = []

    rows.append([
        InlineKeyboardButton(
            text=f"🗣 Lang: {to_lang_label(selected_lang)}",
            callback_data=f"lang|{session_id}|{page}|{selected_lang}|{selected_quality}",
        ),
        InlineKeyboardButton(
            text=f"📺 Qual: {to_quality_label(selected_quality)}",
            callback_data=f"qual|{session_id}|{page}|{selected_lang}|{selected_quality}",
        ),
    ])

    for movie in movies:
        title = (movie.get("title") or movie.get("clean_title") or "Unknown").strip()
        if len(title) > 52:
            title = title[:49] + "..."
        rows.append([
            InlineKeyboardButton(
                text=title,
                callback_data=f"send_file|{str(movie.get('_id'))}|{session_id}",
            )
        ])

    max_page = (total - 1) // PAGE_SIZE if total else 0
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"page|{session_id}|{page - 1}|{selected_lang}|{selected_quality}")
        )
    if page < max_page:
        nav_buttons.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"page|{session_id}|{page + 1}|{selected_lang}|{selected_quality}")
        )

    if nav_buttons:
        rows.append(nav_buttons)

    return InlineKeyboardMarkup(rows)

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

    try:
        base_query = build_base_query(query_text)
        lang_options, quality_options = await fetch_available_filters(base_query)

        session_id = uuid.uuid4().hex[:10]
        clear_old_sessions()
        SEARCH_SESSIONS[session_id] = {
            "query": query_text,
            "lang_options": lang_options,
            "quality_options": quality_options,
            "ts": time.time(),
        }

        selected_lang = "all"
        selected_quality = "all"
        movies, total = await fetch_page(query_text, 0, selected_lang, selected_quality)
        if not movies:
            await searching_message.edit_text("😕 Sorry, I couldn't find anything for that query. Try a slightly different name.")
            return

        max_page = (total - 1) // PAGE_SIZE if total else 0
        keyboard = build_results_keyboard(session_id, 0, selected_lang, selected_quality, movies, total)
        await searching_message.edit_text(
            f"<b>Results for:</b> {query_text}\n"
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
        parts = data.split("|", 2)
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
        size = format_size(movie.get("size", 0))
        caption = f"<b>Title:</b> {title}\n<b>Size:</b> {size}"

        try:
            await safe_copy_message(
                client=client,
                chat_id=call.message.chat.id,
                from_chat_id=source_chat_id,
                message_id=msg_id,
                caption=caption,
            )
            await call.answer("Sending file...")
        except Exception as exc:
            await call.answer("Failed to send file.", show_alert=True)
            await call.message.reply_text(f"❌ Delivery failed: {exc}")
        return

    if data.startswith("page|"):
        parts = data.split("|", 4)
        if len(parts) != 5:
            await call.answer("Invalid page request.", show_alert=True)
            return

        session_id = parts[1]
        try:
            page = int(parts[2])
        except ValueError:
            await call.answer("Invalid page number.", show_alert=True)
            return
        if page < 0:
            page = 0

        selected_lang = normalize_language(parts[3]) or "all"
        selected_quality = normalize_quality(parts[4]) or "all"
        session = SEARCH_SESSIONS.get(session_id)
        if not session:
            await call.answer("Search expired. Please search again.", show_alert=True)
            return

        query_text = session.get("query", "")
        try:
            movies, total = await fetch_page(query_text, page, selected_lang, selected_quality)
            if not movies:
                await call.answer("No more results.", show_alert=True)
                return

            max_page = (total - 1) // PAGE_SIZE if total else 0
            keyboard = build_results_keyboard(session_id, page, selected_lang, selected_quality, movies, total)
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

    if data.startswith("lang|") or data.startswith("qual|"):
        parts = data.split("|", 4)
        if len(parts) != 5:
            await call.answer("Invalid filter request.", show_alert=True)
            return

        action = parts[0]
        session_id = parts[1]
        try:
            page = int(parts[2])
        except ValueError:
            page = 0
        selected_lang = normalize_language(parts[3]) or "all"
        selected_quality = normalize_quality(parts[4]) or "all"

        session = SEARCH_SESSIONS.get(session_id)
        if not session:
            await call.answer("Search expired. Please search again.", show_alert=True)
            return

        lang_options = session.get("lang_options", ["all"])
        quality_options = session.get("quality_options", ["all"])
        if "all" not in lang_options:
            lang_options = ["all"] + lang_options
        if "all" not in quality_options:
            quality_options = ["all"] + quality_options

        if action == "lang":
            selected_lang = cycle_option(selected_lang, lang_options)
            page = 0
        else:
            selected_quality = cycle_option(selected_quality, quality_options)
            page = 0

        query_text = session.get("query", "")

        try:
            movies, total = await fetch_page(query_text, page, selected_lang, selected_quality)
            if not movies:
                await call.answer("❌ Not available in this Language/Quality", show_alert=True)
                return

            max_page = (total - 1) // PAGE_SIZE if total else 0
            keyboard = build_results_keyboard(session_id, page, selected_lang, selected_quality, movies, total)
            await call.message.edit_text(
                f"<b>Results for:</b> {query_text}\n"
                f"<b>Page:</b> {page + 1}/{max_page + 1}\n"
                "Select a file to receive:",
                reply_markup=keyboard,
            )
            await call.answer()
        except Exception as exc:
            await call.answer("Failed to apply filter.", show_alert=True)
            await call.message.reply_text(f"❌ Filter error: {exc}")
        return

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
