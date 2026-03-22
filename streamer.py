import re
import mimetypes
from aiohttp import web
from bson import ObjectId
from database import movies_col

class TelegramStreamer:
    def __init__(self, client):
        self.client = client
        self.app = web.Application()
        self.app.add_routes([
            web.get('/stream/{mongo_id}', self.stream_file),
            web.head('/stream/{mongo_id}', self.stream_file),
            web.options('/stream/{mongo_id}', self.handle_options)
        ])
        self.runner = None
        
    async def handle_options(self, request):
        return web.Response(
            status=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "Range",
                "Access-Control-Max-Age": "86400",
            }
        )
        
    async def start(self, host="0.0.0.0", port=8080):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, host, port)
        await site.start()
        print(f"🎬 Streamer started on http://{host}:{port}")

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    async def stream_file(self, request):
        mongo_id = request.match_info.get('mongo_id')
        print(f"\n{'='*40}")
        print(f"🚀 [NEW REQUEST] Stream requested for ID: {mongo_id}")
        range_header = request.headers.get("Range", "")
        print(f"📡 Range Header: {range_header or 'Full File Requested'}")
        
        try:
            movie = await movies_col.find_one({"_id": ObjectId(mongo_id)})
            if not movie:
                print("❌ [DB ERROR] Movie not found in MongoDB!")
                return web.Response(status=404, text="File not found in database.")
            print(f"✅ [DB SUCCESS] Found: {movie.get('title')}")
        except Exception as e:
            print(f"❌ [DB CRASH] Invalid Mongo ID: {e}")
            return web.Response(status=400, text="Invalid ID format.")

        try:
            print("🔄 [TELEGRAM] Fetching fresh message to avoid expired offset...")
            msg = await self.client.get_messages(movie.get("source_chat_id"), movie.get("msg_id"))
            if getattr(msg, "empty", False) or not msg:
                print("❌ [TELEGRAM ERROR] Message is empty or deleted!")
                return web.Response(status=404, text="Message deleted or inaccessible.")
            print("✅ [TELEGRAM SUCCESS] Fresh message fetched.")
        except Exception as e:
            print(f"❌ [TELEGRAM CRASH] Failed to fetch message: {e}")
            return web.Response(status=500, text=f"Backend Error: Cannot fetch message.")

        media = getattr(msg, msg.media.value) if msg.media else None
        if not media:
            print("❌ [MEDIA ERROR] No video/document inside message!")
            return web.Response(status=404, text="No media found in message.")

        file_size = getattr(media, "file_size", 0)
        print(f"📦 File Size: {file_size} bytes")
        
        raw_name = movie.get("title") or movie.get("clean_title") or "video.mp4"
        file_name = raw_name.replace('"', '').replace('\n', ' ').strip()
        
        offset = 0
        limit = file_size
        status = 200
        content_length = file_size
        
        mime_type, _ = mimetypes.guess_type(file_name)
        mime_type = mime_type or "video/mp4"

        chunk_size = 1024 * 1024 
        skip_bytes = 0

        if range_header:
            match = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if match:
                start = int(match.group(1))
                end = match.group(2)
                end = int(end) if end else file_size - 1
                    
                if start >= file_size or end >= file_size or start > end:
                    print("❌ [RANGE ERROR] Requested range out of bounds.")
                    return web.Response(status=416, headers={"Content-Range": f"bytes */{file_size}"})
                
                aligned_start = start - (start % chunk_size)
                skip_bytes = start - aligned_start 
                
                offset = aligned_start
                limit = end - start + 1 
                content_length = limit
                status = 206
                headers = {
                    "Accept-Ranges": "bytes",
                    "Content-Type": mime_type,
                    "Content-Disposition": f'inline; filename="{file_name}"',
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(content_length)
                }
                print(f"✂️ [MATH] Range: {start}-{end}. Aligned Offset: {offset}. Skip: {skip_bytes}")

        else:
            headers = {
                "Accept-Ranges": "bytes",
                "Content-Type": mime_type,
                "Content-Disposition": f'inline; filename="{file_name}"',
                "Content-Length": str(content_length)
            }

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)
        
        if request.method == "HEAD":
            print("🛑 [HEAD REQUEST] Returning headers only.")
            return response

        print("🚀 [STREAMING] Sending bytes to browser...")
        
        try:
            bytes_sent = 0
            async for chunk in self.client.stream_media(message=msg, offset=offset):
                if bytes_sent >= limit:
                    break

                chunk_len = len(chunk)

                if skip_bytes > 0:
                    if skip_bytes >= chunk_len:
                        skip_bytes -= chunk_len
                        continue
                    else:
                        chunk = chunk[skip_bytes:]
                        chunk_len = len(chunk)
                        skip_bytes = 0
                
                if bytes_sent + chunk_len > limit:
                    chunk = chunk[:limit - bytes_sent]
                    
                await response.write(chunk)
                bytes_sent += len(chunk)
            print("✅ [STREAM SUCCESS] Finished sending chunks.")

        except ConnectionResetError:
            print("⚠️ [STREAM HALTED] User closed the browser/player.")
        except Exception as e:
            print(f"❌ [STREAM CRASH] Critical Error: {e}")
            
        return response