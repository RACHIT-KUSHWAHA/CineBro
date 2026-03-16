import asyncio
from pyrogram import Client
from pyrogram.errors import FloodWait
import config
from indexer import extract_metadata
from database import insert_movies_batch, setup_indexes, get_file_by_id

async def run_indexer():
    print("Initializing Database Indexes...")
    await setup_indexes()
    
    if not config.SESSION_STRING:
        print("❌ Error: SESSION_STRING not found in environment.")
        return
        
    app = Client(
        "userbot_scraper",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=config.SESSION_STRING
    )
    
    print("Starting Userbot Client...")
    await app.start()
    me = await app.get_me()
    print(f"✅ Userbot Online as @{me.username or me.first_name}")
    
    channel = config.STORAGE_CHANNEL
    backup_channel = config.BACKUP_CHANNEL
    print(f"Starting to index and backup from channel: {channel} to {backup_channel}...")
    
    count = 0
    copied_count = 0
    batch = []
    BATCH_SIZE = 500
    
    try:
        async for msg in app.get_chat_history(channel):
            try:
                media = msg.document or msg.video
                if media:
                    # Resume logic: Check if movie is already in DB
                    existing_file = await get_file_by_id(media.file_id)
                    if existing_file:
                        continue
                        
                    raw_text = getattr(media, "file_name", "") or getattr(msg, "caption", "") or ""
                    metadata = extract_metadata(raw_text)
                    
                    # Dual Action: Copy message to BACKUP_CHANNEL and get new msg_id
                    copied_msg = await app.copy_message(backup_channel, channel, msg.id)
                    new_msg_id = copied_msg.id
                    
                    doc = {
                        "file_id": media.file_id,
                        "msg_id": new_msg_id,
                        "title": raw_text,
                        "clean_title": metadata["clean_title"],
                        "size": media.file_size,
                        "quality": metadata["quality"],
                        "language": metadata["language"],
                        "year": metadata["year"]
                    }
                    
                    batch.append(doc)
                    count += 1
                    copied_count += 1
                    
                    # Anti-Ban Strategy: 4 second sleep after *every* copy
                    print(f"[{copied_count}] Copied and added {media.file_id} to batch. Sleeping 4s...")
                    await asyncio.sleep(4)
                    
                    # Deep Breath every 50 copies
                    if copied_count % 50 == 0:
                        print("🧘‍♂️ Deep Breath: Sleeping for 60 seconds to mimic human behavior...")
                        await asyncio.sleep(60)
                    
                    if len(batch) >= BATCH_SIZE:
                        print(f"🔄 Inserting batch of {len(batch)} movies... (Total processed: {count})")
                        await insert_movies_batch(batch)
                        batch.clear()
                        
            except FloodWait as e:
                wait_time = e.value + 5
                print(f"⚠️ FloodWait caught processing message! Sleeping for {wait_time} seconds before continuing...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                print(f"❌ Error processing message {msg.id if hasattr(msg, 'id') else 'unknown'}: {e}")
                
        # insert any remaining
        if batch:
            print(f"🔄 Inserting final batch of {len(batch)} movies...")
            await insert_movies_batch(batch)
            batch.clear()
            
        print(f"✅ Successfully finished indexing! Total movies indexed: {count}")

    except FloodWait as e:
        wait_time = e.value + 5
        print(f"⚠️ Critical FloodWait caught on channel history iteration! Sleeping for {wait_time} seconds...")
        await asyncio.sleep(wait_time)
        print("Warning: The loop broke due to FloodWait on history retrieval. Re-run script to continue indexing.")
    except Exception as e:
        print(f"❌ Error during scraping: {str(e)}")
        
    await app.stop()

if __name__ == "__main__":
    try:
        # Run explicitly in the current loop for Python 3.7+ compatibility
        asyncio.run(run_indexer())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
