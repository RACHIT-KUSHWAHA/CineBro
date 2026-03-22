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

      // 🔥 IMPORTANT: Forward Range
      const range = request.headers.get("range");
      if (range) headers.set("range", range);

      headers.set("accept", "*/*");
      headers.set("connection", "keep-alive");
      headers.set("accept-encoding", "identity");

      try {
        const backendRes = await fetch(backendUrl, {
          method: request.method,
          headers,
          cf: {
            cacheEverything: false,
            cacheTtl: 0
          }
        });

        // 🔥 FIX: Clone + sanitize headers
        const newHeaders = new Headers();

        const allowedHeaders = [
          "content-type",
          "content-length",
          "content-range",
          "accept-ranges",
          "content-disposition"
        ];

        for (const [key, value] of backendRes.headers.entries()) {
          if (allowedHeaders.includes(key.toLowerCase())) {
            newHeaders.set(key, value);
          }
        }

        // 🔥 FORCE critical headers
        newHeaders.set("Accept-Ranges", "bytes");
        newHeaders.set("Cache-Control", "no-cache");
        newHeaders.set("Connection", "keep-alive");

        return new Response(
          request.method === "HEAD" ? null : backendRes.body,
          {
            status: backendRes.status,
            headers: newHeaders
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

      const html = `
<!DOCTYPE html>
<html>
<head>
  <title>CineBro Player</title>
  <style>
    body {
      margin: 0;
      background: #000;
      display: flex;
      justify-content: center;
      align-items: center;
      height: 100vh;
    }
    video {
      width: 90%;
      max-height: 90vh;
      border-radius: 10px;
    }
  </style>
</head>
<body>
  <video controls autoplay preload="metadata">
    <source src="${streamUrl}" type="video/mp4">
  </video>
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