import re
import mimetypes
from aiohttp import web
from bson import ObjectId
from pyrogram.errors import OffsetInvalid
from database import movies_col

CHUNK_SIZE = 1024 * 1024  # 1MB Telegram aligned chunk


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
        print(f"🚀 [NEW REQUEST] ID: {mongo_id}")

        range_header = request.headers.get("Range", "")
        print(f"📡 Range: {range_header or 'FULL'}")

        # ---------------- DB ----------------
        try:
            movie = await movies_col.find_one({"_id": ObjectId(mongo_id)})
            if not movie:
                return web.Response(status=404, text="Not found")
        except:
            return web.Response(status=400, text="Invalid ID")

        # ---------------- TELEGRAM ----------------
        try:
            msg = await self.client.get_messages(
                movie["source_chat_id"],
                movie["msg_id"]
            )
        except Exception as e:
            return web.Response(status=500, text=f"Telegram error: {e}")

        media = getattr(msg, msg.media.value) if msg.media else None
        if not media:
            return web.Response(status=404, text="No media")

        file_size = getattr(media, "file_size", 0)
        if file_size <= 0:
            return web.Response(status=500, text="Invalid file")

        file_name = (movie.get("title") or "video.mp4").replace('"', '')
        mime_type = mimetypes.guess_type(file_name)[0] or "video/mp4"

        # ---------------- RANGE DEFAULT ----------------
        start = 0
        end = file_size - 1
        status = 200
        skip_bytes = 0

        # ---------------- RANGE PARSE ----------------
        if range_header:
            match = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if match:
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else file_size - 1

                if end >= file_size:
                    end = file_size - 1

                if start < 0 or start >= file_size or end < start:
                    return web.Response(
                        status=416,
                        headers={"Content-Range": f"bytes */{file_size}"}
                    )

                # 🔥 ALIGNMENT FIX
                aligned_offset = start - (start % CHUNK_SIZE)

                if aligned_offset + CHUNK_SIZE > file_size:
                    aligned_offset = max(0, file_size - CHUNK_SIZE)

                aligned_offset = max(0, aligned_offset)

                offset = aligned_offset
                skip_bytes = max(0, start - aligned_offset)

                # 🔥 TELEGRAM LIMIT (CRITICAL)
                telegram_limit = min(CHUNK_SIZE, file_size - aligned_offset)

                bytes_to_send = end - start + 1
                status = 206

                headers = {
                    "Content-Type": mime_type,
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(bytes_to_send),
                    "Content-Disposition": f'inline; filename="{file_name}"',
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                }

                print(f"✂️ Range {start}-{end} | Offset {offset} | Skip {skip_bytes}")

            else:
                # fallback
                offset = 0
                telegram_limit = CHUNK_SIZE
                bytes_to_send = file_size

                headers = {
                    "Content-Type": mime_type,
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(file_size),
                }

        else:
            offset = 0
            telegram_limit = CHUNK_SIZE
            bytes_to_send = file_size

            headers = {
                "Content-Type": mime_type,
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
            }

        # ---------------- RESPONSE ----------------
        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)

        if request.method == "HEAD":
            return response

        print("🚀 Streaming started...")

        try:
            bytes_sent = 0

            async for chunk in self.client.stream_media(
                message=msg,
                offset=offset,
                limit=telegram_limit  # 🔥 FIXED
            ):
                if bytes_sent >= bytes_to_send:
                    break

                chunk_len = len(chunk)

                # skip initial bytes
                if skip_bytes > 0:
                    if skip_bytes >= chunk_len:
                        skip_bytes -= chunk_len
                        continue
                    chunk = chunk[skip_bytes:]
                    chunk_len = len(chunk)
                    skip_bytes = 0

                # trim overflow
                remaining = bytes_to_send - bytes_sent
                if chunk_len > remaining:
                    chunk = chunk[:remaining]
                    chunk_len = len(chunk)

                if chunk_len <= 0:
                    break

                await response.write(chunk)
                bytes_sent += chunk_len

            print("✅ Done streaming")

        except OffsetInvalid as e:
            print(f"❌ OFFSET ERROR: {e}")
        except ConnectionResetError:
            print("⚠️ Client closed")
        except Exception as e:
            print(f"❌ Crash: {e}")

        return response