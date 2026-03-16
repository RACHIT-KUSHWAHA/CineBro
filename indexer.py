import re
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait
import config
from database import insert_movies_batch

# regex compile for optimization
QUALITY_PATTERN = re.compile(r'\b(480p|720p|1080p|2160p|4k)\b', re.IGNORECASE)
YEAR_PATTERN = re.compile(r'\b(19\d{2}|20\d{2})\b')
LANGUAGE_PATTERN = re.compile(
    r'\b(hindi|english|tamil|telugu|malayalam|kannada|bengali|punjabi|marathi|dual\s*audio|multi\s*audio)\b',
    re.IGNORECASE,
)

SPAM_WORDS = re.compile(
    r'(@[a-zA-Z0-9_]+|mkv|mp4|avi|hevc|x264|x265|bluray|web-dl|webrip|hdrip|camrip|predvd|esub|hdtv|line\s*audio|aac|10bit)',
    re.IGNORECASE,
)

def extract_metadata(file_name_or_caption: str):
    """
    Extracts Quality, Language, Year, and Clean Title from raw string.
    """
    raw_text = file_name_or_caption or ""

    quality_match = QUALITY_PATTERN.search(raw_text)
    quality = quality_match.group(1).lower() if quality_match else "unknown"
    if quality == "4k":
        quality = "2160p"

    language_match = LANGUAGE_PATTERN.findall(raw_text)
    normalized_langs = []
    for l in language_match:
        lang = re.sub(r'\s+', ' ', l.lower()).strip()
        if lang == "dual audio":
            lang = "hindi english"
        elif lang == "multi audio":
            lang = "multi"
        if lang not in normalized_langs:
            normalized_langs.append(lang)
    language = " ".join(normalized_langs) if normalized_langs else "unknown"

    year_match = YEAR_PATTERN.search(raw_text)
    year = int(year_match.group(1)) if year_match else 0

    # Clean the title
    # Remove quality, language, year, and spam words
    clean_title = raw_text
    clean_title = QUALITY_PATTERN.sub('', clean_title)
    clean_title = LANGUAGE_PATTERN.sub('', clean_title)
    clean_title = YEAR_PATTERN.sub('', clean_title)
    clean_title = SPAM_WORDS.sub('', clean_title)
    
    # Replace dots, underscores, dashes with space
    clean_title = re.sub(r'[\._\[\]\(\)\-]', ' ', clean_title)
    clean_title = re.sub(r'\b(s\d{1,2}e\d{1,2}|season\s*\d{1,2}|episode\s*\d{1,3})\b', ' ', clean_title, flags=re.IGNORECASE)
    # Remove extra spaces
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()
    
    return {
        "quality": quality,
        "language": language,
        "year": year,
        "clean_title": clean_title.lower()
    }

@Client.on_message(filters.command("index") & filters.user(config.ADMIN_ID))
async def index_channel(client: Client, message: Message):
    """
    Admin command to trigger bulk indexing from the storage channel.
    Usage: /index <channel_id_or_username>
    """
    if len(message.command) < 2:
        await message.reply_text("Usage: /index <channel_id_or_username>")
        return

    channel = message.command[1]
    status_msg = await message.reply_text(f"Starting index map for `{channel}`...")
    
    count = 0
    batch = []
    BATCH_SIZE = 500
    
    try:
        async for msg in client.get_chat_history(channel):
            try:
                # Check if there's a document/video
                media = msg.document or msg.video
                if media:
                    raw_text = getattr(media, "file_name", "") or getattr(msg, "caption", "") or ""
                    metadata = extract_metadata(raw_text)
                    
                    doc = {
                        "file_id": media.file_id,
                        "title": raw_text,
                        "clean_title": metadata["clean_title"],
                        "size": media.file_size,
                        "quality": metadata["quality"],
                        "language": metadata["language"],
                        "year": metadata["year"]
                    }
                    
                    batch.append(doc)
                    count += 1
                    
                    if len(batch) >= BATCH_SIZE:
                        await insert_movies_batch(batch)
                        batch.clear()
                        await asyncio.sleep(0.5) # Prevent flood waits
            except FloodWait as e:
                print(f"FloodWait caught! Sleeping for {e.value + 5} seconds before continuing...")
                await asyncio.sleep(e.value + 5)
                    
        # insert any remaining
        if batch:
            await insert_movies_batch(batch)
            batch.clear()
            
        await status_msg.edit_text(f"✅ Successfully indexed {count} movies into database!")
    except Exception as e:
        await status_msg.edit_text(f"❌ Error during indexing: {str(e)}")
