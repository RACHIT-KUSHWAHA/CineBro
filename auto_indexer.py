"""
Auto-Indexer Module for Telegram File Indexer Bot
Automatically catches new movies/files forwarded to database channel,
extracts metadata, and stores in MongoDB with intelligent deduplication.

Author: Senior Backend Developer
Framework: Pyrogram 2.0+ with Motor AsyncIO
"""

import logging
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorClient
from pymongo import UpdateOne, ASCENDING
from pymongo.errors import DuplicateKeyError
import config

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('[AUTO-INDEXER] %(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


class AutoIndexer:
    """
    Manages async automatic indexing of Telegram files to MongoDB.
    Handles deduplication, unique indexing, and metadata extraction.
    """

    def __init__(self, mongo_client: AsyncIOMotorClient, db_name: str = "CineBro"):
        """
        Initialize the AutoIndexer.
        
        Args:
            mongo_client: AsyncIOMotorClient connected to MongoDB
            db_name: Database name (default: "CineBro")
        """
        self.db = mongo_client[db_name]
        self.indexed_files_col: AsyncIOMotorCollection = self.db["indexed_files"]
        self.database_channel_id = config.DATABASE_CHANNEL_ID
        
    async def setup_indexes(self) -> None:
        """
        Create database indexes on startup.
        This is a secondary shield against duplicate entries.
        """
        try:
            # Unique index on file_unique_id (primary deduplication)
            await self.indexed_files_col.create_index(
                [("file_unique_id", ASCENDING)],
                unique=True,
                name="idx_file_unique_id"
            )
            logger.info("✓ Created unique index on file_unique_id")
            
            # Compound index for fast queries by message_id
            await self.indexed_files_col.create_index(
                [("message_id", ASCENDING), ("database_channel_id", ASCENDING)],
                name="idx_message_lookup"
            )
            logger.info("✓ Created compound index on message_id + channel_id")
            
            # Index for file_name searches
            await self.indexed_files_col.create_index(
                [("file_name", ASCENDING)],
                name="idx_file_name"
            )
            logger.info("✓ Created index on file_name")
            
            # Index for created_at (useful for cleanup/analytics)
            await self.indexed_files_col.create_index(
                [("created_at", ASCENDING)],
                name="idx_created_at"
            )
            logger.info("✓ Created index on created_at (timestamp)")
            
        except Exception as e:
            logger.error(f"✗ Failed to setup indexes: {e}")
            raise

    @staticmethod
    def _format_file_size(size_bytes: int) -> str:
        """
        Convert bytes to human-readable format (B, KB, MB, GB, TB).
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Formatted string (e.g., "125.5 MB")
        """
        size = float(size_bytes or 0)
        units = ["B", "KB", "MB", "GB", "TB"]
        idx = 0
        
        while size >= 1024 and idx < len(units) - 1:
            size /= 1024
            idx += 1
        
        if idx == 0:
            return f"{int(size)} {units[idx]}"
        return f"{size:.2f} {units[idx]}"

    @staticmethod
    def _extract_filename(message) -> Optional[str]:
        """
        Extract filename from message document or caption.
        Priority: document.file_name > parse from caption
        
        Args:
            message: Pyrogram Message object
            
        Returns:
            Extracted filename or None
        """
        # Priority 1: Document file_name attribute
        if message.document and message.document.file_name:
            return message.document.file_name
        
        # Priority 2: Video file_name attribute
        if message.video and message.video.file_name:
            return message.video.file_name
        
        # Priority 3: Parse from caption (extract first line or title-like pattern)
        if message.caption:
            first_line = message.caption.split('\n')[0].strip()
            if first_line and len(first_line) > 3:
                return first_line[:100]  # Cap at 100 chars
        
        # Fallback
        return f"file_{message.message_id}"

    async def extract_metadata(self, message) -> Optional[Dict[str, Any]]:
        """
        Extract file metadata from a Pyrogram message.
        
        Args:
            message: Pyrogram Message object
            
        Returns:
            Dictionary with metadata or None if invalid
        """
        try:
            # Check if message contains document or video
            if not (message.document or message.video):
                logger.debug(f"Skipped message {message.message_id}: No document/video")
                return None
            
            # Extract file object
            file_obj = message.document or message.video
            
            # Extract metadata
            metadata = {
                "file_id": file_obj.file_id,
                "file_unique_id": file_obj.file_unique_id,
                "file_name": self._extract_filename(message),
                "file_size": file_obj.file_size or 0,
                "file_size_formatted": self._format_file_size(file_obj.file_size or 0),
                "caption": message.caption or "",
                "message_id": message.message_id,
                "database_channel_id": self.database_channel_id,
                "media_type": "document" if message.document else "video",
                "forwarded_from": message.forward_from.id if message.forward_from else None,
                "forwarded_from_chat": message.forward_from_chat.id if message.forward_from_chat else None,
                "created_at": datetime.utcnow(),
                "indexed_at": datetime.utcnow(),
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"✗ Error extracting metadata from message {message.message_id}: {e}")
            return None

    async def upsert_file(self, metadata: Dict[str, Any]) -> bool:
        """
        Upsert file metadata to MongoDB with intelligent deduplication.
        
        Uses UpdateOne with upsert=True:
        - Filter: Check if file_unique_id or message_id exists
        - Update: Update file_name and caption if exists, or insert new document
        
        Args:
            metadata: Extracted metadata dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Filter: Check by file_unique_id (primary) OR message_id (fallback)
            filter_query = {
                "$or": [
                    {"file_unique_id": metadata["file_unique_id"]},
                    {"message_id": metadata["message_id"], "database_channel_id": metadata["database_channel_id"]}
                ]
            }
            
            # Update operation: Set all fields, use $setOnInsert for creation timestamp
            update_query = {
                "$set": {
                    "file_id": metadata["file_id"],
                    "file_unique_id": metadata["file_unique_id"],
                    "file_name": metadata["file_name"],
                    "file_size": metadata["file_size"],
                    "file_size_formatted": metadata["file_size_formatted"],
                    "caption": metadata["caption"],
                    "media_type": metadata["media_type"],
                    "forwarded_from": metadata["forwarded_from"],
                    "forwarded_from_chat": metadata["forwarded_from_chat"],
                    "indexed_at": metadata["indexed_at"],
                },
                "$setOnInsert": {
                    "message_id": metadata["message_id"],
                    "database_channel_id": metadata["database_channel_id"],
                    "created_at": metadata["created_at"],
                }
            }
            
            # Execute upsert
            result = await self.indexed_files_col.update_one(
                filter_query,
                update_query,
                upsert=True
            )
            
            # Check if inserted or updated
            if result.upserted_id:
                logger.info(f"✓ INDEXED (NEW): {metadata['file_name']} | Size: {metadata['file_size_formatted']} | ID: {result.upserted_id}")
                return True
            elif result.modified_count > 0:
                logger.info(f"⟳ UPDATED: {metadata['file_name']} | Size: {metadata['file_size_formatted']}")
                return True
            else:
                logger.debug(f"~ SKIPPED (Already Indexed): {metadata['file_name']}")
                return False
            
        except DuplicateKeyError as e:
            logger.warning(f"⚠ Duplicate key error (race condition): {metadata['file_name']} - {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Error upserting file {metadata['file_name']}: {e}")
            return False

    async def process_message(self, message) -> bool:
        """
        Complete pipeline: Extract metadata → Upsert to MongoDB
        
        Args:
            message: Pyrogram Message object
            
        Returns:
            True if processing successful, False otherwise
        """
        try:
            # Step 1: Extract metadata
            metadata = await self.extract_metadata(message)
            if not metadata:
                return False
            
            # Step 2: Upsert to database
            success = await self.upsert_file(metadata)
            return success
            
        except Exception as e:
            logger.error(f"✗ Error processing message: {e}")
            return False

    async def get_indexed_count(self) -> int:
        """Get total count of indexed files in database."""
        try:
            count = await self.indexed_files_col.count_documents({})
            return count
        except Exception as e:
            logger.error(f"✗ Error getting indexed count: {e}")
            return 0

    async def get_channel_indexed_count(self) -> int:
        """Get count of indexed files from current database channel."""
        try:
            count = await self.indexed_files_col.count_documents(
                {"database_channel_id": self.database_channel_id}
            )
            return count
        except Exception as e:
            logger.error(f"✗ Error getting channel count: {e}")
            return 0


# ============================================================================
# INTEGRATION EXAMPLE (Use in your bot.py)
# ============================================================================

"""
In your bot.py, add this at the top level (after initializing `app` from Pyrogram):

from auto_indexer import AutoIndexer
from motor.motor_asyncio import AsyncIOMotorClient

# Initialize Auto-Indexer (place in your async main() or as global)
mongo_client = AsyncIOMotorClient(config.MONGO_URI)
auto_indexer = AutoIndexer(mongo_client, db_name="CineBro")

# During startup (in async def main()):
await auto_indexer.setup_indexes()

# Then create the message handler:

@app.on_message(filters.chat(config.DATABASE_CHANNEL_ID))
async def handle_database_channel(client, message):
    '''Automatically index files forwarded to database channel.'''
    # Process the message through auto-indexer
    success = await auto_indexer.process_message(message)
    
    if success:
        # Optional: Send confirmation to admin
        indexed_count = await auto_indexer.get_channel_indexed_count()
        total_count = await auto_indexer.get_indexed_count()
        await client.send_message(
            config.ADMIN_ID,
            f"📦 Auto-Indexed: {indexed_count} files in channel | 💾 Total: {total_count} in DB"
        )

"""
