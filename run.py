#!/usr/bin/env python3
"""
CineBro Production Bot - Entry Point
Runs the telegram search bot with production-grade features.
"""

import sys
import asyncio
from pyrogram import idle
from bot import app, start_time
from database import setup_indexes
import config


async def main():
    """Initialize database indexes and start bot."""
    print("🔧 Initializing database indexes...")
    await setup_indexes()
    print("✅ Database initialization complete")
    
    print("🚀 Starting CineBro bot...")
    await app.start()
    
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
║  Admin Commands:                                           ║
║  /stats        - Dashboard (CPU, RAM, Users, Movies)      ║
║  /broadcast    - Message all users                        ║
║  /reply        - Send DM to user                          ║
║                                                            ║
║  Deployment Checklist:                                     ║
║  ✅ Async Motor configured                                ║
║  ✅ Rate limiter (5/min sliding window)                   ║
║  ✅ Compound indexes for performance                      ║
║  ✅ Auto-delete with FloodWait retry                      ║
║  ✅ Series hierarchy detection                            ║
║  ✅ Smart fallback disclaimers                            ║
║  ✅ Admin monitoring & tracking                           ║
║                                                            ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    print(f"🌐 Bot ready for incoming messages...")
    print(f"⏱️  Start time: {start_time}\n")


if __name__ == "__main__":
    print("CineBro - Telegram File Indexer Bot")
    print("=" * 50)
    
    # Validate configuration
    if not config.API_ID or not config.API_HASH or not config.BOT_TOKEN:
        print("❌ Error: Missing Telegram credentials in .env")
        print("   Please set: API_ID, API_HASH, BOT_TOKEN")
        sys.exit(1)
    
    if not config.MONGO_URI:
        print("❌ Error: Missing MONGO_URI in .env")
        sys.exit(1)
    
    try:
        # Initialize bot and create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
        
        # Keep bot running
        idle()
        
    except KeyboardInterrupt:
        print("\n✅ Bot stopped gracefully")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
