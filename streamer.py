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
            web.get('/stream/{mongo_id}', self.stream_file)
        ])
        self.runner = None
        
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
        
        try:
            movie = await movies_col.find_one({"_id": ObjectId(mongo_id)})
        except Exception:
            return web.Response(status=400, text="Invalid ID format.")
            
        if not movie:
            return web.Response(status=404, text="File not found in database.")
            
        try:
            msg = await self.client.get_messages(
                movie.get("source_chat_id"), 
                movie.get("msg_id")
            )
        except Exception as e:
            print(f"❌ [STREAM ERROR] Failed to fetch message: {e}")
            return web.Response(status=500, text=f"Backend Error: Cannot fetch message.")

        if getattr(msg, "empty", False) or not msg:
            return web.Response(status=404, text="Message deleted or inaccessible.")

        media = getattr(msg, msg.media.value) if msg.media else None
        if not media:
            return web.Response(status=404, text="No media found in message.")

        file_size = getattr(media, "file_size", 0)
        
        raw_name = movie.get("title") or movie.get("clean_title") or "video.mp4"
        file_name = raw_name.replace('"', '').replace('\n', ' ').strip()
        
        range_header = request.headers.get("Range", "")
        offset = 0
        limit = file_size
        
        status = 200
        content_length = file_size
        
        mime_type, _ = mimetypes.guess_type(file_name)
        mime_type = mime_type or "video/mp4"

        # The Magic Number (Telegram Chunk Size = 1 MB)
        chunk_size = 1024 * 1024 
        skip_bytes = 0

        if range_header:
            match = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if match:
                start = int(match.group(1))
                end = match.group(2)
                
                if end:
                    end = int(end)
                else:
                    end = file_size - 1
                    
                if start >= file_size or end >= file_size or start > end:
                    return web.Response(
                        status=416, 
                        text="Requested Range Not Satisfiable", 
                        headers={"Content-Range": f"bytes */{file_size}"}
                    )
                
                # Align offset to the nearest 1MB boundary
                aligned_start = start - (start % chunk_size)
                skip_bytes = start - aligned_start # Bytes to ignore
                
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

        else:
            headers = {
                "Accept-Ranges": "bytes",
                "Content-Type": mime_type,
                "Content-Disposition": f'inline; filename="{file_name}"',
                "Content-Length": str(content_length)
            }

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)
        
        try:
            bytes_sent = 0
            # Ask Pyrogram for data starting at the aligned 1MB mark
            async for chunk in self.client.stream_media(message=msg, offset=offset):
                if bytes_sent >= limit:
                    break

                chunk_len = len(chunk)

                # Skip initial unrequested bytes if seeking mid-chunk
                if skip_bytes > 0:
                    if skip_bytes >= chunk_len:
                        skip_bytes -= chunk_len
                        continue
                    else:
                        chunk = chunk[skip_bytes:]
                        chunk_len = len(chunk)
                        skip_bytes = 0
                
                # Trim the end if we exceed the limit
                if bytes_sent + chunk_len > limit:
                    chunk = chunk[:limit - bytes_sent]
                    
                await response.write(chunk)
                bytes_sent += len(chunk)

        except ConnectionResetError:
            pass 
        except Exception as e:
            print(f"❌ [STREAM CRASH] Error while streaming {file_name}: {e}")
            
        return response