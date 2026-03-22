import re
import mimetypes
import asyncio
from aiohttp import web
from bson import ObjectId
from pyrogram.errors import OffsetInvalid, FloodWait
from database import movies_col

# Telegram uses 1 MiB chunks internally for file streaming.
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
        # Limit global concurrency to reduce FloodWait risk.
        self.semaphore = asyncio.Semaphore(2)

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

        file_name = (movie.get("title") or "video").replace('"', '')
        mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"

        # ---------------- RANGE DEFAULT ----------------
        start = 0
        end = file_size - 1
        skip_bytes = 0
        chunk_offset = 0
        chunk_limit = 0  # 0 = no limit (Telegram handles chunking)
        bytes_to_send = file_size
        status = 200

        # ---------------- RANGE PARSE ----------------
        if range_header:
            match = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if match:
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else file_size - 1

                # Clamp end within file size
                end = min(end, file_size - 1)

                # Invalid start range
                if start < 0 or start >= file_size or end < start:
                    return web.Response(
                        status=416,
                        headers={"Content-Range": f"bytes */{file_size}"},
                    )

                # --- Safe offset alignment ---
                aligned_offset = start - (start % CHUNK_SIZE)
                aligned_offset = max(0, aligned_offset)

                if aligned_offset + CHUNK_SIZE > file_size:
                    aligned_offset = max(0, file_size - CHUNK_SIZE)

                chunk_offset = aligned_offset // CHUNK_SIZE

                # Fix OFFSET_INVALID near file end
                if (file_size - (chunk_offset * CHUNK_SIZE)) < CHUNK_SIZE:
                    chunk_offset = max(0, (file_size // CHUNK_SIZE) - 1)

                skip_bytes = max(0, start - aligned_offset)
                bytes_to_send = end - start + 1
                status = 206

                headers = {
                    "Content-Type": mime_type,
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(bytes_to_send),
                    "Content-Disposition": f'inline; filename="{file_name}"',
                    "Cache-Control": "no-cache",
                }

                print(
                    f"✂️ Range {start}-{end} | AlignedOffset {aligned_offset} | "
                    f"ChunkOffset {chunk_offset} | Skip {skip_bytes} | Bytes {bytes_to_send}"
                )

            else:
                # fallback
                headers = {
                    "Content-Type": mime_type,
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(file_size),
                }

        else:
            headers = {
                "Content-Type": mime_type,
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
            }

        # Download mode (?download=1)
        if request.rel_url.query.get("download") == "1":
            headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
        else:
            headers.setdefault(
                "Content-Disposition",
                f'inline; filename="{file_name}"',
            )

        # ---------------- RESPONSE ----------------
        response = web.StreamResponse(status=status, headers=headers)
        await response.prepare(request)

        if request.method == "HEAD":
            return response

        print("🚀 Streaming started...")

        # Global concurrency limiter
        async with self.semaphore:
            # Retry mechanism: up to 2 attempts for transient errors
            for attempt in range(2):
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
                    break

                except FloodWait as e:
                    # Respect Telegram's FloodWait and surface a 503
                    print(f"🚫 FloodWait: waiting {e.value}s")
                    await asyncio.sleep(e.value)
                    return web.Response(
                        status=503,
                        text=f"Server busy, retry after {e.value}s",
                    )

                except OffsetInvalid as e:
                    print(f"❌ OFFSET ERROR: {e}")
                    # OFFSET_INVALID should be prevented by alignment; if it
                    # still happens, don't hammer Telegram.
                    if attempt == 0:
                        continue
                    break

                except ConnectionResetError:
                    print("⚠️ Client closed")
                    break

                except Exception as e:
                    print(f"❌ Crash (attempt {attempt + 1}): {e}")
                    if attempt == 0:
                        # Retry once on generic errors
                        continue
                    break

        return response