import asyncio
import time
import uuid
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import config
from database import search_movies, get_file_by_id
from utils import is_rate_limited

SEARCH_CACHE = {}
QUALITY_ORDER = {"2160p": 5, "1080p": 4, "720p": 3, "480p": 2, "unknown": 1}


def clear_old_cache() -> None:
    now = time.time()
    if len(SEARCH_CACHE) > 5000:
        stale = [k for k, v in SEARCH_CACHE.items() if now - v.get("ts", now) > 3600]
        for key in stale:
            del SEARCH_CACHE[key]


def quality_sort_key(raw_quality: str) -> int:
    q = (raw_quality or "").lower().strip()
    if q == "4k":
        q = "2160p"
    return QUALITY_ORDER.get(q, 0)


def split_languages(raw: str):
    parts = [x.strip().lower() for x in (raw or "").replace(",", " ").replace("/", " ").split()]
    return [p for p in parts if p]


@Client.on_message(filters.private & ~filters.command(["start", "help", "index", "backup", "status", "flush"]))
async def handle_search(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else 0

    if is_rate_limited(user_id, 3):
        await message.reply_text("Please wait 3 seconds between searches.")
        return

    query_text = (message.text or "").strip()
    if len(query_text) < 2:
        await message.reply_text("Please provide a longer search query.")
        return

    status = await message.reply_text("Searching...")
    results = await search_movies(query_text, limit=80)

    if not results:
        await status.edit_text("No movies found for your query.")
        return

    best_title = results[0].get("clean_title", "unknown")
    base_list = [r for r in results if r.get("clean_title") == best_title] or results

    year_found = next((m.get("year") for m in base_list if m.get("year", 0) > 0), "Unknown")

    cache_id = uuid.uuid4().hex[:10]
    clear_old_cache()
    SEARCH_CACHE[cache_id] = {
        "ts": time.time(),
        "title": best_title,
        "query": query_text,
        "base_files": base_list,
    }

    languages = set()
    for item in base_list:
        for lang in split_languages(item.get("language", "unknown")):
            languages.add(lang)

    if not languages:
        languages = {"unknown"}

    language_list = sorted(languages)
    buttons = []
    row = []
    for lang in language_list:
        row.append(InlineKeyboardButton(f"{lang.capitalize()}", callback_data=f"L|{cache_id}|{lang[:12]}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    await status.edit_text(
        f"Movie: `{best_title.title()}`\nYear: {year_found}\n\nSelect language:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


@Client.on_callback_query(filters.regex(r"^L\|"))
async def cb_language_selected(client: Client, query: CallbackQuery):
    _, cache_id, selected_lang = query.data.split("|")

    session = SEARCH_CACHE.get(cache_id)
    if not session:
        await query.answer("Search session expired. Search again.", show_alert=True)
        return

    title = session["title"]
    fallback_results = await search_movies(title, language=selected_lang, limit=80)
    movie_list = [r for r in fallback_results if r.get("clean_title") == title] or fallback_results

    if not movie_list:
        await query.answer("No files found for this title.", show_alert=True)
        return

    quality_to_item = {}
    for item in movie_list:
        quality = (item.get("quality") or "unknown").lower()
        if quality not in quality_to_item:
            quality_to_item[quality] = item

    ordered_qualities = sorted(quality_to_item.keys(), key=quality_sort_key, reverse=True)

    session["selected_lang"] = selected_lang
    buttons = []
    row = []
    for qual in ordered_qualities:
        row.append(InlineKeyboardButton(f"{qual.upper()}", callback_data=f"Q|{cache_id}|{qual[:10]}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton("Back", callback_data=f"B|{cache_id}")])

    await query.message.edit_text(
        f"Movie: `{title.title()}`\nLanguage: {selected_lang.capitalize()}\n\nSelect quality:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


@Client.on_callback_query(filters.regex(r"^B\|"))
async def cb_back_to_language(client: Client, query: CallbackQuery):
    _, cache_id = query.data.split("|")
    session = SEARCH_CACHE.get(cache_id)
    if not session:
        await query.answer("Session expired.", show_alert=True)
        return

    base_list = session.get("base_files", [])
    title = session.get("title", "unknown")
    year_found = next((m.get("year") for m in base_list if m.get("year", 0) > 0), "Unknown")

    languages = set()
    for item in base_list:
        for lang in split_languages(item.get("language", "unknown")):
            languages.add(lang)

    if not languages:
        languages = {"unknown"}

    buttons = []
    row = []
    for lang in sorted(languages):
        row.append(InlineKeyboardButton(f"{lang.capitalize()}", callback_data=f"L|{cache_id}|{lang[:12]}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    await query.message.edit_text(
        f"Movie: `{title.title()}`\nYear: {year_found}\n\nSelect language:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


@Client.on_callback_query(filters.regex(r"^Q\|"))
async def cb_quality_selected(client: Client, query: CallbackQuery):
    _, cache_id, selected_quality = query.data.split("|")

    session = SEARCH_CACHE.get(cache_id)
    if not session:
        await query.answer("Search session expired.", show_alert=True)
        return

    title = session.get("title", "")
    selected_lang = session.get("selected_lang", "")

    candidates = await search_movies(title, language=selected_lang, quality=selected_quality, limit=20)
    candidates = [c for c in candidates if c.get("clean_title") == title] or candidates

    if not candidates:
        await query.answer("No matching file found.", show_alert=True)
        return

    picked = candidates[0]
    msg_id = picked.get("msg_id")
    file_id = picked.get("file_id")
    source_chat_id = picked.get("source_chat_id", config.STORAGE_CHANNEL)

    if not msg_id and file_id:
        doc = await get_file_by_id(file_id)
        if doc:
            msg_id = doc.get("msg_id")
            source_chat_id = doc.get("source_chat_id", source_chat_id)

    if not msg_id:
        await query.answer("File pointer missing in backup storage.", show_alert=True)
        return

    await query.message.reply_text("Sending file...")

    try:
        sent_msg = await client.copy_message(
            chat_id=query.from_user.id,
            from_chat_id=source_chat_id,
            message_id=msg_id,
            caption="⚠️ **Please forward this file to your Saved Messages or another chat.**\n\n_Because this file will be deleted in 30 minutes to avoid copyright bans._\n\nDelivered by Movie Bot"
        )
        if sent_msg:
            async def auto_delete():
                await asyncio.sleep(1800)
                try:
                    await client.delete_messages(chat_id=query.from_user.id, message_ids=sent_msg.id)
                except Exception:
                    pass
            asyncio.create_task(auto_delete())
    except Exception as exc:
        await query.message.reply_text(f"Error sending file: {exc}")
