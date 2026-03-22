export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const path = url.pathname;
        const fileId = path.split('/').pop();

        // Environment variable for your Pyrogram backend server
        // e.g., https://your-pyrogram-server.com
        const BACKEND_URL = env.BACKEND_URL || "http://YOUR_BACKEND_IP:PORT"; 

        // Basic Anti-Hotlinking: Only allow requests from our own domain 
        // or empty referer (some players drop referer)
        const referer = request.headers.get('Referer');
        if (path.startsWith('/stream/')) {
            if (referer && !referer.includes(url.hostname) && !referer.includes("localhost")) {
                return new Response("Hotlinking explicitly disabled.", { status: 403 });
            }

            // Proxy the stream request to the Pyrogram backend
            const backendRequestUrl = `${BACKEND_URL}/stream/${fileId}`;
            
            // Forward headers, notably the 'Range' header for HTTP 206
            console.log("Incoming Range:", request.headers.get("Range"));
            const newHeaders = new Headers(request.headers);
            newHeaders.set('Forwarded-Host', url.hostname);

            try {
                const response = await fetch(backendRequestUrl, {
                    method: request.method,
                    headers: newHeaders,
                    redirect: 'follow',
                    cf: {
                        // This tells Cloudflare NOT to cache or interfere with this stream
                        cacheEverything: false,
                        cacheTtl: 0
                    }
                });
                
                // Return exactly what the backend gives us, including Content-Range and Content-Type
                const proxyHeaders = new Headers(response.headers);
                proxyHeaders.set('Accept-Ranges', 'bytes');
                proxyHeaders.set('Cache-Control', 'no-store');
                proxyHeaders.set('Access-Control-Allow-Origin', '*'); 
                proxyHeaders.set('Access-Control-Allow-Headers', 'Range');

                return new Response(response.body, {
                    status: response.status,
                    headers: proxyHeaders
                });
            } catch (e) {
                return new Response(`Cloudflare Fetch Error: ${e.message}`, { status: 500 });
            }
        }

        // Web Player UI
        if (path.startsWith('/watch/') && fileId) {
            const streamUrl = `${url.origin}/stream/${fileId}`;
            const downloadUrl = streamUrl; // Optionally add ?download=1 if backend supports forcing disposition

            const html = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cinebro Streamer | Premium Viewing</title>
    <!-- Fonts -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <!-- Plyr Video Player -->
    <link rel="stylesheet" href="https://cdn.plyr.io/3.7.8/plyr.css" />
    <!-- Tailwind CSS -->
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- Icons -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    fontFamily: {
                        sans: ['Poppins', 'sans-serif'],
                    },
                    colors: {
                        brand: {
                            500: '#e50914', // Premium Netflix-like red
                            600: '#b80710',
                        },
                        dark: {
                            900: '#0b0f19', // Deep dark blue/black
                            800: '#111827',
                            700: '#1f2937',
                        }
                    }
                }
            }
        }
    </script>
    <style>
        body {
            background-color: #0b0f19;
            color: #f3f4f6;
            background-image: radial-gradient(circle at top right, rgba(31, 41, 55, 0.4) 0%, transparent 40%),
                              radial-gradient(circle at bottom left, rgba(229, 9, 20, 0.1) 0%, transparent 40%);
            min-height: 100vh;
        }
        
        /* Customizing Plyr for a more premium look */
        .plyr {
            border-radius: 16px;
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
            overflow: hidden;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }
        .plyr--video .plyr__controls {
            padding-top: 35px;
            background: linear-gradient(rgba(0, 0, 0, 0), rgba(0, 0, 0, 0.8));
        }
        .plyr--full-ui input[type=range] {
            color: #e50914;
        }
        .plyr__control--overlaid {
            background: rgba(229, 9, 20, 0.8);
        }
        .plyr__control--overlaid:hover {
            background: #e50914;
        }

        .glass-card {
            background: rgba(17, 24, 39, 0.7);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.08);
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        }

        .action-btn {
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        .action-btn::after {
            content: '';
            position: absolute;
            top: 50%;
            left: 50%;
            width: 300%;
            height: 300%;
            background: rgba(255,255,255,0.1);
            transform: translate(-50%, -50%) scale(0);
            border-radius: 50%;
            transition: transform 0.5s;
        }
        .action-btn:hover::after {
            transform: translate(-50%, -50%) scale(1);
        }
    </style>
</head>
<body class="flex flex-col antialiased">

    <!-- Header -->
    <header class="w-full py-4 px-6 md:px-12 flex items-center justify-between glass-card sticky top-0 z-50">
        <div class="flex items-center gap-3">
            <div class="w-10 h-10 rounded-full bg-gradient-to-br from-brand-500 to-purple-600 flex items-center justify-center shadow-lg shadow-brand-500/30">
                <i class="fa-solid fa-play text-white ml-1"></i>
            </div>
            <h1 class="text-2xl font-bold tracking-tight text-white">Cinebro<span class="text-brand-500">Streamer</span></h1>
        </div>
        <div class="flex items-center gap-4">
            <span class="px-3 py-1 text-xs font-semibold bg-green-500/20 text-green-400 rounded-full border border-green-500/30">
                <i class="fa-solid fa-circle-check mr-1"></i> Secure Connection
            </span>
        </div>
    </header>

    <!-- Main Content -->
    <main class="flex-grow w-full max-w-6xl mx-auto px-4 py-8 flex flex-col items-center">
        
        <!-- Video Player Section -->
        <div class="w-full mb-8">
            <video id="player" playsinline controls data-poster="https://images.unsplash.com/photo-1626814026160-2237a95fc5a0?q=80&w=2070&auto=format&fit=crop" class="w-full">
                <source src="${streamUrl}" type="video/mp4" />
                <source src="${streamUrl}" type="video/webm" />
                <!-- Fallback content -->
                Your browser does not support HTML5 video.
            </video>
        </div>

        <!-- Meta & Actions Section -->
        <div class="w-full grid grid-cols-1 lg:grid-cols-3 gap-6">
            
            <!-- Video Info Card -->
            <div class="lg:col-span-1 glass-card rounded-2xl p-6">
                <h2 class="text-xl font-semibold mb-2 text-white">Stream Details</h2>
                <div class="space-y-4 mt-6">
                    <div class="flex items-center text-gray-400 text-sm">
                        <i class="fa-solid fa-file-video w-6 text-center text-brand-500"></i>
                        <span class="ml-2">High Quality Stream</span>
                    </div>
                    <div class="flex items-center text-gray-400 text-sm">
                        <i class="fa-solid fa-bolt w-6 text-center text-yellow-500"></i>
                        <span class="ml-2">Fast CDN Delivery</span>
                    </div>
                    <div class="flex items-center text-gray-400 text-sm">
                        <i class="fa-solid fa-shield-halved w-6 text-center text-green-500"></i>
                        <span class="ml-2">End-to-End Encrypted Proxy</span>
                    </div>
                </div>
            </div>

            <!-- External Players Card -->
            <div class="lg:col-span-2 glass-card rounded-2xl p-6">
                <h2 class="text-xl font-semibold mb-4 text-white">External Players & Downloads</h2>
                <p class="text-gray-400 text-sm mb-6">Experience uninterrupted playback by opening this stream in your favorite media player.</p>
                
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                    
                    <!-- MX Player -->
                    <a href="intent:${streamUrl}#Intent;package=com.mxtech.videoplayer.ad;S.title=${fileId};end" 
                       class="action-btn flex items-center justify-center gap-3 py-3 px-4 bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-medium shadow-lg shadow-blue-500/20">
                        <i class="fa-solid fa-play"></i>
                        MX Player
                    </a>

                    <!-- VLC Player -->
                    <a href="vlc://${streamUrl}" 
                       class="action-btn flex items-center justify-center gap-3 py-3 px-4 bg-orange-600 hover:bg-orange-700 text-white rounded-xl font-medium shadow-lg shadow-orange-500/20">
                        <i class="fa-solid fa-cone"></i>
                        VLC Player
                    </a>

                    <!-- Copy Link -->
                    <button onclick="copyToClipboard('${streamUrl}')" id="copyBtn"
                       class="action-btn flex items-center justify-center gap-3 py-3 px-4 bg-gray-700 hover:bg-gray-600 text-white rounded-xl font-medium shadow-lg shadow-gray-900/50">
                        <i class="fa-solid fa-link"></i>
                        Copy Link
                    </button>

                    <!-- Direct Download -->
                    <a href="${downloadUrl}" download 
                       class="action-btn sm:col-span-2 lg:col-span-3 flex items-center justify-center gap-3 py-3 px-4 bg-brand-500 hover:bg-brand-600 text-white rounded-xl font-medium shadow-lg shadow-brand-500/20 mt-2">
                        <i class="fa-solid fa-cloud-arrow-down text-lg"></i>
                        Download Original File
                    </a>

                </div>
            </div>
        </div>
    </main>

    <!-- Footer -->
    <footer class="w-full text-center py-6 mt-auto">
        <p class="text-gray-500 text-sm">© 2026 Cinebro Streamer. Powered by Cloudflare Workers.</p>
    </footer>

    <!-- Plyr Initialization and Scripts -->
    <script src="https://cdn.plyr.io/3.7.8/plyr.polyfilled.js"></script>
    <script>
        document.addEventListener('DOMContentLoaded', () => {
            // Initialize premium player
            const player = new Plyr('#player', {
                controls: [
                    'play-large', 'play', 'progress', 'current-time', 'mute', 'volume', 'captions', 'settings', 'pip', 'airplay', 'fullscreen'
                ],
                settings: ['quality', 'speed'],
                speed: { selected: 1, options: [0.5, 0.75, 1, 1.25, 1.5, 2] }
            });

            // Copy to clipboard functionality
            window.copyToClipboard = function(text) {
                navigator.clipboard.writeText(text).then(() => {
                    const btn = document.getElementById('copyBtn');
                    const originalHTML = btn.innerHTML;
                    btn.innerHTML = '<i class="fa-solid fa-check"></i> Copied!';
                    btn.classList.add('bg-green-600');
                    btn.classList.remove('bg-gray-700');
                    setTimeout(() => {
                        btn.innerHTML = originalHTML;
                        btn.classList.remove('bg-green-600');
                        btn.classList.add('bg-gray-700');
                    }, 2000);
                });
            }
        });
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
