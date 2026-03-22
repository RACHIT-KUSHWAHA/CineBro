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
            return web.Response(status=404, text="Invalid ID format.")
            
        if not movie:
            return web.Response(status=404, text="File not found in database.")
            
        file_size = movie.get("size")
        file_id = movie.get("file_id")
        
        # Fallback to get message if missing required properties
        if not file_size or not file_id:
            try:
                msg = await self.client.get_messages(
                    movie.get("source_chat_id"), 
                    movie.get("msg_id")
                )
                media = getattr(msg, msg.media.value) if msg.media else None
                if not media:
                    return web.Response(status=404, text="Media not found")
                file_size = getattr(media, "file_size", 0)
                file_id = getattr(media, "file_id", "")
            except Exception as e:
                return web.Response(status=500, text=f"Error fetching message: {str(e)}")

        file_name = movie.get("title") or movie.get("clean_title") or "video.mp4"
        
        range_header = request.headers.get("Range", "")
        offset = 0
        limit = 0
        
        status = 200
        content_length = file_size
        
        mime_type, _ = mimetypes.guess_type(file_name)
        mime_type = mime_type or "application/octet-stream"
        
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Type": mime_type,
            "Content-Disposition": f'inline; filename="{file_name}"'
        }

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
                    
                offset = start
                limit = end - start + 1
                content_length = limit
                status = 206
                headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

        headers["Content-Length"] = str(content_length)

        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)
        
        try:
            async for chunk in self.client.stream_media(message=file_id, limit=limit, offset=offset):
                await response.write(chunk)
        except Exception:
            # Client disconnected or streaming failed
            pass
            
        return response
