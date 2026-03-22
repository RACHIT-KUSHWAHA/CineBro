import re
import mimetypes
import time
from aiohttp import web
from bson import ObjectId
from pyrogram.errors import OffsetInvalid, FloodWait
from database import movies_col

# Pyrogram stream_media uses 1 MiB chunks internally. "offset" and "limit"
# are expressed in number of chunks, not bytes.
CHUNK_SIZE = 1024 * 1024  # 1 MiB


class TelegramStreamer:
    def __init__(self, client):
        self.client = client
        self.app = web.Application()
        self.app.add_routes([
            web.get('/stream/{mongo_id}', self.stream_file),
            web.options('/stream/{mongo_id}', self.handle_options)
        ])
        self.runner = None
        # When Telegram returns a FloodWait for streaming, block new
        # stream attempts until this timestamp to avoid hammering.
        self._cooldown_until = 0.0

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

    async def _stream_from_offset(self, response, msg, chunk_offset: int, chunk_limit: int,
                                   skip_bytes: int, bytes_to_send: int):
        """Stream bytes from Telegram starting at a given chunk offset.

        chunk_offset / chunk_limit are in units of CHUNK_SIZE, as expected by
        pyrogram.Client.stream_media. Within the first chunk we skip
        `skip_bytes` bytes, then stream exactly `bytes_to_send` bytes total.
        """
        bytes_sent = 0
        async for chunk in self.client.stream_media(message=msg, offset=chunk_offset, limit=chunk_limit):
            if bytes_sent >= bytes_to_send:
                break

            chunk_len = len(chunk)

            # Skip initial extra bytes from the aligned offset
            if skip_bytes > 0:
                if skip_bytes >= chunk_len:
                    skip_bytes -= chunk_len
                    continue
                chunk = chunk[skip_bytes:]
                chunk_len = len(chunk)
                skip_bytes = 0

            remaining = bytes_to_send - bytes_sent
            if chunk_len > remaining:
                chunk = chunk[:remaining]
                chunk_len = len(chunk)

            if chunk_len <= 0:
                break

            await response.write(chunk)
            await response.drain()
            bytes_sent += chunk_len

        return bytes_sent

    async def stream_file(self, request):
        mongo_id = request.match_info.get('mongo_id')
        print(f"\n{'='*40}")
        print(f"🚀 [NEW REQUEST] ID: {mongo_id}")

        range_header = request.headers.get("Range", "")
        print(f"📡 Range: {range_header or 'FULL'}")

        # If we recently hit a Telegram FloodWait, short-circuit with 503
        # instead of triggering more ExportAuthorization calls.
        now = time.time()
        if now < self._cooldown_until:
            retry_after = int(self._cooldown_until - now)
            return web.Response(
                status=503,
                headers={
                    "Retry-After": str(retry_after),
                    "Content-Type": "text/plain; charset=utf-8",
                },
                text=f"Temporary Telegram flood limit. Try again in {retry_after} seconds.",
            )

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
        chunk_offset = 0
        chunk_limit = 0  # 0 means "no limit" to stream_media

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

                # Map byte range to stream_media chunk offset/limit.
                # stream_media's offset/limit are chunk counts, not bytes.
                first_chunk = start // CHUNK_SIZE
                first_chunk_offset = start % CHUNK_SIZE
                last_chunk = end // CHUNK_SIZE
                needed_chunks = (last_chunk - first_chunk) + 1

                chunk_offset = first_chunk
                chunk_limit = needed_chunks
                skip_bytes = first_chunk_offset
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

                print(f"✂️ Range {start}-{end} | ChunkOffset {chunk_offset} | ChunkLimit {chunk_limit} | Skip {skip_bytes}")

            else:
                # fallback
                chunk_offset = 0
                chunk_limit = 0
                bytes_to_send = file_size

                headers = {
                    "Content-Type": mime_type,
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(file_size),
                }

        else:
            chunk_offset = 0
            chunk_limit = 0
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
            bytes_sent = await self._stream_from_offset(
                response=response,
                msg=msg,
                chunk_offset=chunk_offset,
                chunk_limit=chunk_limit,
                skip_bytes=skip_bytes,
                bytes_to_send=bytes_to_send,
            )
            print(f"✅ Done streaming ({bytes_sent} bytes sent)")

        except FloodWait as e:
            # Remember cooldown for subsequent requests and log clearly.
            self._cooldown_until = time.time() + int(e.value) + 1
            print(f"❌ FLOOD WAIT: Must wait {e.value} seconds before streaming again.")
        except OffsetInvalid as e:
            print(f"❌ OFFSET ERROR: {e}")
        except ConnectionResetError:
            print("⚠️ Client closed")
        except Exception as e:
            print(f"❌ Crash: {e}")

        return response