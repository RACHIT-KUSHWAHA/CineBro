import asyncio
from pyrogram import Client
import config

async def main():
    print("Welcome to the Pyrogram Session Generator!")
    print("Ensure you have API_ID and API_HASH set in your .env file.\n")
    
    if not config.API_ID or not config.API_HASH:
        print("❌ Error: API_ID and API_HASH are missing.")
        return
        
    async with Client("session_generator", api_id=config.API_ID, api_hash=config.API_HASH, in_memory=True) as app:
        session_string = await app.export_session_string()
        print("\n" + "="*40)
        print("YOUR SESSION STRING:")
        print("="*40)
        print(session_string)
        print("="*40 + "\n")
        print("👉 Save this in your .env file as: SESSION_STRING=" + session_string[:10] + "...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
