#!/usr/bin/env python3
"""
CineBro - Simple Bot Starter
Just initializes indexes and runs the bot!
"""

import sys
import asyncio
from pyrogram import idle
from bot import app
from database import setup_indexes
import config


async def main():
    """Initialize database indexes and start bot."""
    print("🔧 Initializing database indexes...")
    await setup_indexes()
    print("✅ Database initialization complete\n")
    
    print("🚀 Starting CineBro bot...")
    await app.start()
    
    me = await app.get_me()
    print(f"✅ Bot Online as @{me.username}\n")
    
    print("""
╔════════════════════════════════════════════════════════════╗
║                                                            ║
║         🎬 CineBro - Production Bot Started 🎬            ║
║                                                            ║
║  Features:                                                 ║
║  ✅ Series Hierarchy (Season/Episode Selection)           ║
║  ✅ Smart Fallback (Quality/Language)                     ║
║  ✅ Rate Limiting (5/min per user)                        ║
║  ✅ Auto-Delete (30 minutes)                              ║
║  ✅ Production Indexes (Millisecond queries)              ║
║  ✅ Async Motor (Non-blocking DB)                         ║
║                                                            ║
║  Commands:                                                 ║
║  /start       - Welcome message                           ║
║  /help        - Help menu                                 ║
║  /stats       - Admin dashboard                           ║
║                                                            ║
║  Just type a movie name to search!                         ║
║                                                            ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    print("🌐 Bot ready for incoming messages...")
    print("   Use Ctrl+C to stop\n")
    
    # Keep bot running forever
    await idle()
    
    await app.stop()


if __name__ == "__main__":
    print("CineBro - Telegram File Indexer Bot")
    print("=" * 50 + "\n")
    
    # Validate configuration
    if not config.API_ID or not config.API_HASH or not config.BOT_TOKEN:
        print("❌ Error: Missing Telegram credentials in .env")
        print("   Please set: API_ID, API_HASH, BOT_TOKEN")
        sys.exit(1)
    
    if not config.MONGO_URI:
        print("❌ Error: Missing MONGO_URI in .env")
        sys.exit(1)
    
    try:
        # Run using asyncio (handles event loop properly)
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✅ Bot stopped gracefully")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
