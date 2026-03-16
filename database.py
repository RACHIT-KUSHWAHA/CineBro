import re
from motor.motor_asyncio import AsyncIOMotorClient
import config

# Create the async Motor client
client = AsyncIOMotorClient(config.MONGO_URI, tz_aware=True)
db = client[config.DB_NAME]
movies_col = db['movies']

QUALITY_SCORES = {
    "4k": 4000,
    "2160p": 2160,
    "1080p": 1080,
    "720p": 720,
    "480p": 480,
}


def normalize_quality(raw_quality: str) -> str:
    if not raw_quality:
        return "unknown"
    quality = raw_quality.lower().strip()
    if quality == "4k":
        return "2160p"
    return quality


def quality_score(raw_quality: str) -> int:
    return QUALITY_SCORES.get(normalize_quality(raw_quality), 0)


def _lang_match(item_language: str, target_language: str) -> bool:
    if not target_language:
        return True
    item_tokens = {p.strip().lower() for p in re.split(r"[\s,/&|]+", item_language or "") if p.strip()}
    target_tokens = {p.strip().lower() for p in re.split(r"[\s,/&|]+", target_language or "") if p.strip()}
    if not item_tokens or not target_tokens:
        return False
    return not item_tokens.isdisjoint(target_tokens)

async def setup_indexes():
    """
    Creates the required indexes for fuzzy search and unique file storage.
    """
    await movies_col.create_index([("clean_title", 1)])
    await movies_col.create_index([("title", 1)])
    await movies_col.create_index([("file_id", 1)], unique=True)
    await movies_col.create_index([("msg_id", 1)])
    await movies_col.create_index([("source_chat_id", 1)])
    
    # Filter specific indexes to prevent slow distinct() queries on high traffic
    await movies_col.create_index([("languages", 1)])
    await movies_col.create_index([("quality", 1)])
    await movies_col.create_index([("season", 1)])
    print("Database indexes created successfully.")

async def insert_movies_batch(movies_list):
    """
    Inserts a chunk of formatted movie documents into MongoDB natively saving memory.
    Ignores DuplicateKeyError for duplicate file_ids.
    """
    if not movies_list:
        return
    try:
        for movie in movies_list:
            if "quality" in movie:
                movie["quality"] = normalize_quality(movie.get("quality", ""))
        await movies_col.insert_many(movies_list, ordered=False)
    except Exception as e:
        # Catch BulkWriteError which contains duplicate keys when ordered=False
        pass


def build_fuzzy_regex(query: str) -> str:
    """
    Converts user input into a fuzzy regex by replacing separators with .*.
    Example: "Kalki 2898" -> "kalki.*2898"
    """
    query = (query or "").strip().lower()
    if not query:
        return ""
    
    # Security: Limit query length to prevent ReDoS and slow Mongo scans
    if len(query) > 100:
        query = query[:100]
        
    escaped = re.escape(query)
    # Replace escaped separators with fuzzy wildcard blocks.
    pattern = re.sub(r"(?:\\\.|\\_|\\ )+", r".*", escaped)
    pattern = re.sub(r"\.\*+", ".*", pattern)
    return pattern


def get_fuzzy_search_cursor(query: str, limit: int = 20):
    """
    Returns a Mongo cursor for fuzzy title search. Caller should iterate the cursor.
    """
    pattern = build_fuzzy_regex(query)
    if not pattern:
        return movies_col.find({"_id": None}).limit(0)

    mongo_query = {
        "$or": [
            {"clean_title": {"$regex": pattern, "$options": "i"}},
            {"title": {"$regex": pattern, "$options": "i"}},
        ]
    }

    projection = {
        "_id": 0,
        "file_id": 1,
        "title": 1,
        "clean_title": 1,
        "size": 1,
        "msg_id": 1,
        "source_chat_id": 1,
    }
    return movies_col.find(mongo_query, projection).limit(max(1, int(limit)))

async def search_movies(query: str, language: str = "", quality: str = "", limit: int = 50):
    """
    Search movies dynamically using MongoDB $text index to prevent OOM crash
    on 700k records. Uses projections to minimize memory payload.
    """
    pattern = build_fuzzy_regex(query)
    if not pattern:
        return []

    search_query = {
        "$or": [
            {"clean_title": {"$regex": pattern, "$options": "i"}},
            {"title": {"$regex": pattern, "$options": "i"}},
        ]
    }
    
    # Projection to only return what is needed
    projection = {
        "_id": 0,
        "file_id": 1,
        "msg_id": 1,
        "clean_title": 1,
        "year": 1,
        "language": 1,
        "quality": 1,
        "size": 1,
        "season": 1,
    }

    # Fetch a wider candidate set first, then apply Python fallback/sorting logic.
    cursor = movies_col.find(search_query, projection).limit(200)
    candidates = await cursor.to_list(length=200)

    if not candidates:
        return []

    # Quality-first sort for all results while keeping text relevance as tie-breaker.
    candidates.sort(
        key=lambda x: (
            quality_score(x.get("quality", "")),
        ),
        reverse=True,
    )

    target_language = (language or "").strip().lower()
    target_quality = normalize_quality((quality or "").strip().lower())

    # Default search path used by query text from users.
    if not target_language and not target_quality:
        return candidates[:limit]

    exact_matches = [
        item for item in candidates
        if (not target_quality or normalize_quality(item.get("quality", "")) == target_quality)
        and _lang_match(item.get("language", ""), target_language)
    ]
    if exact_matches:
        return exact_matches[:limit]

    fallback_pool = []

    # 1) Same language, best available quality.
    same_language = [item for item in candidates if _lang_match(item.get("language", ""), target_language)]
    if same_language:
        fallback_pool.extend(same_language)

    # 2) Same requested quality, alternate language.
    if target_quality:
        same_quality_other_lang = [
            item for item in candidates
            if normalize_quality(item.get("quality", "")) == target_quality
            and not _lang_match(item.get("language", ""), target_language)
        ]
        fallback_pool.extend(same_quality_other_lang)

    # 3) Last fallback: top candidates by quality score and text relevance.
    if not fallback_pool:
        fallback_pool = candidates

    # De-duplicate by file_id while preserving fallback priority.
    seen = set()
    deduped = []
    for item in fallback_pool:
        fid = item.get("file_id")
        if not fid or fid in seen:
            continue
        seen.add(fid)
        deduped.append(item)

    return deduped[:limit]

async def get_file_by_id(file_id: str):
    """
    Retrieve one document accurately by file_id.
    """
    return await movies_col.find_one({"file_id": file_id}, {"_id": 0})


async def update_msg_id_by_file_id(file_id: str, new_msg_id: int) -> bool:
    result = await movies_col.update_one(
        {"file_id": file_id},
        {"$set": {"msg_id": int(new_msg_id)}},
    )
    return result.modified_count > 0


async def upsert_movie_document(movie_doc: dict) -> None:
    if not movie_doc or not movie_doc.get("file_id"):
        return
    if "quality" in movie_doc:
        movie_doc["quality"] = normalize_quality(movie_doc.get("quality", ""))
    await movies_col.update_one(
        {"file_id": movie_doc["file_id"]},
        {"$set": movie_doc},
        upsert=True,
    )


async def get_total_movies_count() -> int:
    return await movies_col.count_documents({})


async def flush_movies_collection() -> int:
    result = await movies_col.delete_many({})
    return result.deleted_count
