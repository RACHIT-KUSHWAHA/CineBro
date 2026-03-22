export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const fileId = path.split('/').pop();

    const BACKEND_URL = "https://cinebro.beyondrachit.me";

    const referer = request.headers.get("referer");

    // =========================
    // 🎬 STREAM PROXY (FIXED)
    // =========================
    if (path.startsWith("/stream/") && fileId) {

      // 🔒 Hotlink protection
      if (referer && !referer.includes(url.hostname) && !referer.includes("localhost")) {
        return new Response("Hotlink blocked", { status: 403 });
      }

      const backendUrl = `${BACKEND_URL}/stream/${fileId}`;

      const headers = new Headers();

      // 🔥 IMPORTANT: Forward Range only
      const range = request.headers.get("range");
      if (range) headers.set("range", range);

      try {
        const backendRes = await fetch(backendUrl, {
          method: request.method,
          headers,
          cf: {
            cacheEverything: false,
            cacheTtl: 0
          }
        });

        // 🔥 Passthrough response (do not modify headers)
        return new Response(
          request.method === "HEAD" ? null : backendRes.body,
          {
            status: backendRes.status,
            headers: backendRes.headers
          }
        );

      } catch (e) {
        return new Response("Worker error: " + e.message, { status: 500 });
      }
    }

    // =========================
    // 🎥 WATCH PAGE (UPGRADED UI)
    // =========================
    if (path.startsWith("/watch/") && fileId) {
      const streamUrl = `${url.origin}/stream/${fileId}`;
      const downloadUrl = `${streamUrl}?download=1`;

      const html = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>CineBro Streamer</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #050816;
      --card: #0b1020;
      --accent: #e50914;
      --accent-soft: rgba(229, 9, 20, 0.15);
      --text: #f9fafb;
      --muted: #9ca3af;
    }
    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    body {
      min-height: 100vh;
      background: radial-gradient(circle at top, #111827 0, #020617 45%, #000 100%);
      color: var(--text);
      display: flex;
      justify-content: center;
      align-items: stretch;
      padding: 16px;
    }
    .shell {
      width: 100%;
      max-width: 1100px;
      margin: auto;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    .header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .logo {
      width: 36px;
      height: 36px;
      border-radius: 999px;
      background: radial-gradient(circle at 30% 30%, #f97316, #ec4899 45%, #6366f1 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 10px 25px rgba(0,0,0,0.4);
      font-size: 18px;
    }
    .title-main {
      font-weight: 700;
      letter-spacing: 0.02em;
    }
    .title-accent {
      color: var(--accent);
    }
    .badge {
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 11px;
      border: 1px solid rgba(34,197,94,0.4);
      color: #bbf7d0;
      background: rgba(34,197,94,0.16);
      white-space: nowrap;
    }
    .card {
      background: linear-gradient(135deg, rgba(15,23,42,0.98), rgba(15,23,42,0.9));
      border-radius: 18px;
      padding: 14px;
      box-shadow: 0 18px 40px rgba(0,0,0,0.55);
      border: 1px solid rgba(148,163,184,0.25);
    }
    .video-wrapper {
      position: relative;
      border-radius: 14px;
      overflow: hidden;
      background: #020617;
      border: 1px solid rgba(148,163,184,0.4);
    }
    video {
      width: 100%;
      max-height: 70vh;
      display: block;
      background: #000;
    }
    .button-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 12px;
    }
    .btn {
      flex: 1 1 120px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      padding: 10px 14px;
      border-radius: 999px;
      font-size: 13px;
      font-weight: 500;
      border: 1px solid transparent;
      cursor: pointer;
      text-decoration: none;
      transition: all 0.18s ease-out;
      white-space: nowrap;
    }
    .btn-primary {
      background: var(--accent);
      border-color: rgba(248,113,113,0.6);
      color: #fff;
      box-shadow: 0 10px 28px rgba(239,68,68,0.45);
    }
    .btn-primary:hover {
      filter: brightness(1.06);
      transform: translateY(-1px);
    }
    .btn-secondary {
      background: rgba(15,23,42,0.8);
      border-color: rgba(148,163,184,0.6);
      color: var(--text);
    }
    .btn-secondary:hover {
      background: rgba(30,64,175,0.65);
    }
    .btn-ghost {
      background: rgba(15,23,42,0.6);
      border-color: rgba(148,163,184,0.4);
      color: var(--muted);
    }
    .btn-ghost:hover {
      background: rgba(31,41,55,0.9);
      color: var(--text);
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      font-size: 11px;
      padding: 4px 9px;
      border-radius: 999px;
      background: rgba(15,23,42,0.95);
      border: 1px solid rgba(148,163,184,0.4);
      color: var(--muted);
    }
    .meta {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-top: 10px;
      gap: 8px;
      font-size: 12px;
      color: var(--muted);
      flex-wrap: wrap;
    }
    .meta span {
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }
    .note {
      margin-top: 8px;
      font-size: 11px;
      color: var(--muted);
    }
    .status {
      margin-top: 8px;
      font-size: 12px;
      color: var(--muted);
    }
    .alert {
      margin-top: 8px;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid rgba(248,113,113,0.5);
      background: rgba(127,29,29,0.75);
      color: #fee2e2;
      font-size: 12px;
    }
    .alert a {
      color: #fee2e2;
      text-decoration: underline;
    }
    .hidden {
      display: none;
    }
    @media (max-width: 640px) {
      .shell {
        gap: 12px;
      }
      .header {
        flex-direction: column;
        align-items: flex-start;
      }
      .button-row {
        flex-direction: column;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="header">
      <div class="brand">
        <div class="logo">▶</div>
        <div>
          <div class="title-main">CineBro <span class="title-accent">Streamer</span></div>
          <div style="font-size:11px;color:var(--muted)">Telegram → Browser ultra-fast proxy</div>
        </div>
      </div>
      <div class="badge">Secure, direct stream</div>
    </header>

    <main class="card">
      <div class="video-wrapper">
        <video id="video" controls autoplay playsinline preload="none"></video>
      </div>

      <div id="status" class="status"></div>
      <div id="errorBox" class="alert hidden"></div>

      <div class="button-row">
        <a class="btn btn-primary" href="${downloadUrl}" download>
          ⬇ Download (Browser) (not recommended)
        </a>
        <a class="btn btn-secondary" href="intent:${streamUrl}#Intent;package=com.mxtech.videoplayer.ad;S.title=${fileId};end">
          ▶ MX Player
        </a>
        <a class="btn btn-secondary" href="vlc://${streamUrl}">
          🟠 VLC Player
        </a>
        <button class="btn btn-ghost" id="copyBtn" type="button">
          🔗 Copy stream link
        </button>
      </div>

      <div class="meta">
        <span>⚠ This stream depends on the CineBro bot being online.</span>
        <span class="pill">Tip: For best stability, use MX Player or VLC on Android.</span>
      </div>
      <div class="note">
        Downloading via the red button uses your browser’s downloader for maximum speed.
      </div>
    </main>
  </div>

  <script>
    (function() {
      const streamUrl = '${streamUrl}';
      const botUrl = 'https://t.me/CineBroBot';

      const video = document.getElementById('video');
      const statusEl = document.getElementById('status');
      const errorBox = document.getElementById('errorBox');
      const btn = document.getElementById('copyBtn');

      function showError(message) {
        if (!errorBox) return;
        const extra = ` If this keeps happening, you can still watch or download the movie directly via our Telegram bot: <a href="${botUrl}" target="_blank" rel="noopener noreferrer">@CineBroBot</a>.`;
        errorBox.innerHTML = message + extra;
        errorBox.classList.remove('hidden');
        if (statusEl) statusEl.textContent = '';
        if (video) video.style.display = 'none';
      }

      async function initStream() {
        if (!video) return;
        if (statusEl) statusEl.textContent = 'Checking stream availability…';
        try {
          const res = await fetch(streamUrl, { method: 'HEAD' });
          const code = res.status;
          if (res.ok && (code === 200 || code === 206)) {
            video.src = streamUrl;
            if (statusEl) statusEl.textContent = '';
            if (errorBox) errorBox.classList.add('hidden');
            video.style.display = 'block';
            return;
          }

          if (code === 404) {
            showError('This file is no longer available on the server or was removed from Telegram.');
          } else if (code === 429) {
            showError('Too many people are streaming this file right now. Please wait a minute and try again.');
          } else if (code === 503) {
            showError('The stream backend is temporarily busy or Telegram is rate limiting downloads. Please try again in a few minutes.');
          } else if (code === 403) {
            showError('Access to this stream is forbidden from your current link. Please open the movie again from the CineBro bot.');
          } else {
            showError('The browser could not start the stream due to a network or server error.');
          }
        } catch (err) {
          showError('The browser could not connect to the stream (network error).');
        }
      }

      if (btn && navigator.clipboard) {
        btn.addEventListener('click', () => {
          navigator.clipboard.writeText(streamUrl).then(() => {
            const old = btn.textContent;
            btn.textContent = '✔ Link copied';
            setTimeout(() => { btn.textContent = old; }, 1800);
          }).catch(() => {
            btn.textContent = '✖ Copy failed';
            setTimeout(() => { btn.textContent = '🔗 Copy stream link'; }, 2000);
          });
        });
      }

      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initStream);
      } else {
        initStream();
      }
    })();
  </script>
</body>
</html>
`;

      return new Response(html, {
        headers: { "Content-Type": "text/html" }
      });
    }

    return new Response("Not Found", { status: 404 });
  }
};