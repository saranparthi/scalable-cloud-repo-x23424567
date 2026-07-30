import asyncio
import json
import time
import websockets

URI = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)

last_print = 0


async def listen():
    global last_print

    async with websockets.connect(URI, max_size=None) as websocket:
        print("Connected to Bluesky Jetstream...\n")

        while True:
            try:
                message = await websocket.recv()

                # JSON string -> Python dict
                data = json.loads(message)

                commit = data.get("commit", {})
                record = commit.get("record", {})

                # Only English posts
                langs = record.get("langs", [])
                if "en" not in langs:
                    continue

                # Skip empty posts
                text = record.get("text")
                if not text:
                    continue

                # Print only one record every second
                now = time.time()
                if now - last_print < 1:
                    continue

                last_print = now

                cleaned = {
                    "did": data.get("did"),
                    "timestamp_us": data.get("time_us"),
                    "collection": commit.get("collection"),
                    "operation": commit.get("operation"),
                    "created_at": record.get("createdAt"),
                    "language": langs,
                    "text": text,
                }

                print(json.dumps(cleaned, indent=4))

            except Exception as e:
                print("Error:", e)


asyncio.run(listen())