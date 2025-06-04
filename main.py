# Telegram Channel Forward Bot (Forward without tag with time delay and web UI)
# Requirements: telethon, pymongo, flask

from telethon import TelegramClient, events
from flask import Flask, request, render_template_string
from pymongo import MongoClient
import asyncio
import threading
import random
from dotenv import load_dotenv

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv()

# --- CONFIGURATION ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
client = TelegramClient('session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)
app = Flask(__name__)
mongo = MongoClient(MONGO_URI)
db = mongo['forwardBot']
sources_col = db['sources']  # {source_id, target_ids[], last_message_id}

# --- UTILITIES ---
async def copy_message_to_channel(message, target_id):
    try:
        if message.media:
            await client.send_file(target_id, file=message.media, caption=message.text or '', supports_streaming=True)
        elif message.text:
            await client.send_message(target_id, message.text)
        elif message.sticker:
            await client.send_file(target_id, file=message.sticker)
    except Exception as e:
        print(f"Error copying to {target_id}: {e}")

async def forward_messages(source_id, target_ids, last_msg_id):
    try:
        messages = await client.get_messages(source_id, limit=10, min_id=last_msg_id)
        messages = list(reversed(messages))
        for msg in messages:
            for target_id in target_ids:
                await copy_message_to_channel(msg, target_id)
            sources_col.update_one({'source_id': source_id}, {"$set": {"last_message_id": msg.id}})
            await asyncio.sleep(random.randint(3600, 18000))  # 1 to 5 hours
    except Exception as e:
        print(f"Forwarding error: {e}")

async def run_forwarder():
    while True:
        sources = sources_col.find()
        for source in sources:
            await forward_messages(source['source_id'], source['target_ids'], source.get('last_message_id', 0))
        await asyncio.sleep(300)  # 5 min check

# --- WEB UI ---
HTML_UI = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Channel Forwarder UI</title>
    <style>
        body { font-family: Arial; padding: 20px; background: #111; color: #fff; }
        input, button { padding: 10px; margin: 5px; }
        input { width: 300px; }
        .container { margin-bottom: 30px; }
        .log { background: #222; padding: 10px; margin-top: 10px; }
    </style>
</head>
<body>
<h1>Telegram Channel Forwarder</h1>
<div class="container">
    <form method="POST" action="/add">
        <input type="text" name="source_id" placeholder="Source Channel ID" required><br>
        <input type="text" name="target_ids" placeholder="Target Channel IDs (comma-separated)" required><br>
        <button type="submit">Add Source</button>
    </form>
</div>
<div class="log">
    <h3>Active Forwarding Channels</h3>
    <ul>
        {% for source in sources %}
            <li><strong>{{ source['source_id'] }}</strong> → {{ source['target_ids'] }}</li>
        {% endfor %}
    </ul>
</div>
</body>
</html>
'''

@app.route('/')
def index():
    sources = list(sources_col.find({}, {"_id": 0}))
    return render_template_string(HTML_UI, sources=sources)

@app.route('/add', methods=['POST'])
def add():
    source_id = int(request.form.get('source_id'))
    target_ids = list(map(int, request.form.get('target_ids').split(',')))
    sources_col.update_one({'source_id': source_id}, {"$set": {'target_ids': target_ids}}, upsert=True)
    return "<script>window.location = '/';</script>"

# --- BOT COMMAND TO ADD VIA TELEGRAM ---
@client.on(events.NewMessage(pattern='/addsource'))
async def add_source(event):
    args = event.message.text.split()
    if len(args) < 2:
        await event.reply("Usage: /addsource source_channel_id target_channel_id1 target_channel_id2 ...")
        return
    source_id = int(args[1])
    target_ids = list(map(int, args[2:]))
    sources_col.update_one({'source_id': source_id}, {"$set": {'target_ids': target_ids}}, upsert=True)
    await event.reply(f"Source {source_id} added with targets: {target_ids}")

# --- START THREADS ---
def start_bot():
    with client:
        client.loop.run_until_complete(run_forwarder())
        client.run_until_disconnected()

def start_flask():
    app.run(host='0.0.0.0', port=8000)

if __name__ == '__main__':
    threading.Thread(target=start_flask).start()
    threading.Thread(target=start_bot).start()
