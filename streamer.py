import re
import mimetypes
from aiohttp import web
from bson import ObjectId
from database import movies_col

CHUNK_SIZE = 1024 * 1024  # 1MB Telegram-aligned chunk size

class TelegramStreamer:
    def __init__(self, client):
        self.client = client
        self.app = web.Application()
        self.app.add_routes([
            web.get('/stream/{mongo_id}', self.stream_file),
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

        if file_size <= 0:
            print("❌ [SIZE ERROR] File size is zero or unknown.")
            return web.Response(status=500, text="Unknown file size.")

        raw_name = movie.get("title") or movie.get("clean_title") or "video.mp4"
        file_name = raw_name.replace('"', '').replace('\n', ' ').strip()

        # Default range: whole file
        start = 0
        end = file_size - 1
        status = 200
        skip_bytes = 0

        mime_type, _ = mimetypes.guess_type(file_name)
        mime_type = mime_type or "video/mp4"

        # Parse HTTP Range header if present
        if range_header:
            match = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if match:
                start = int(match.group(1))
                end_part = match.group(2)
                end = int(end_part) if end_part else file_size - 1

                # Validate range
                if start < 0 or start >= file_size or end < start:
                    print("❌ [RANGE ERROR] Requested range out of bounds.")
                    return web.Response(
                        status=416,
                        headers={"Content-Range": f"bytes */{file_size}"},
                        text="Requested Range Not Satisfiable",
                    )

                # Align offset to Telegram's 1MB chunk boundary safely
                aligned_offset = (start // CHUNK_SIZE) * CHUNK_SIZE
                if aligned_offset >= file_size:
                    aligned_offset = file_size - CHUNK_SIZE
                aligned_offset = max(0, aligned_offset)

                skip_bytes = start - aligned_offset
                offset = aligned_offset

                bytes_to_send = end - start + 1
                content_length = bytes_to_send
                status = 206
                headers = {
                    "Accept-Ranges": "bytes",
                    "Content-Type": mime_type,
                    "Content-Disposition": f'inline; filename="{file_name}"',
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(content_length),
                }
                print(
                    f"✂️ [MATH] Range: {start}-{end}. "
                    f"Aligned Offset: {offset}. Skip: {skip_bytes}. To Send: {bytes_to_send}"
                )
            else:
                # Malformed Range header -> ignore and send full file
                print("⚠️ [RANGE WARN] Malformed Range header, ignoring.")
                offset = 0
                bytes_to_send = file_size
                content_length = file_size
                headers = {
                    "Accept-Ranges": "bytes",
                    "Content-Type": mime_type,
                    "Content-Disposition": f'inline; filename="{file_name}"',
                    "Content-Length": str(content_length),
                }
        else:
            # No Range: serve entire file with 200 OK
            offset = 0
            bytes_to_send = file_size
            content_length = file_size
            headers = {
                "Accept-Ranges": "bytes",
                "Content-Type": mime_type,
                "Content-Disposition": f'inline; filename="{file_name}"',
                "Content-Length": str(content_length),
            }

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)
        
        if request.method == "HEAD":
            print("🛑 [HEAD REQUEST] Returning headers only.")
            return response

        print("🚀 [STREAMING] Sending bytes to browser...")

        try:
            bytes_sent = 0
            # Stream from Telegram starting at the aligned offset, trimming in-Python
            async for chunk in self.client.stream_media(message=msg, offset=offset):
                if bytes_sent >= bytes_to_send:
                    break

                chunk_len = len(chunk)

                # Skip initial bytes inside the first Telegram chunk
                if skip_bytes > 0:
                    if skip_bytes >= chunk_len:
                        skip_bytes -= chunk_len
                        continue
                    chunk = chunk[skip_bytes:]
                    chunk_len = len(chunk)
                    skip_bytes = 0

                # Trim if this chunk would exceed requested range
                remaining = bytes_to_send - bytes_sent
                if chunk_len > remaining:
                    chunk = chunk[:remaining]
                    chunk_len = len(chunk)

                if chunk_len <= 0:
                    break

                await response.write(chunk)
                await response.drain()
                bytes_sent += chunk_len
            print("✅ [STREAM SUCCESS] Finished sending chunks.")

        except ConnectionResetError:
            print("⚠️ [STREAM HALTED] User closed the browser/player.")
        except Exception as e:
            print(f"❌ [STREAM CRASH] Critical Error: {e}")
            
        return response