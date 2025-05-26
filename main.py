import os
import asyncio
import random
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors.rpcerrorlist import ChatWriteForbiddenError
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from motor.motor_asyncio import AsyncIOMotorClient

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

# Load environment variables
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

# Setup Telethon and MongoDB
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
mongo = AsyncIOMotorClient(MONGO_URI)
db = mongo[DB_NAME]
settings_col = db['user_settings']
msglog_col = db['message_log']
users_col = db['users']

forwarding_tasks = {}

def parse_time(timestr):
    units = {'s':1, 'm':60, 'h':3600, 'd':86400}
    timestr = timestr.strip().lower()
    if timestr[-1] in units:
        return int(timestr[:-1]) * units[timestr[-1]]
    return int(timestr)

async def get_settings(user_id):
    return await settings_col.find_one({'user_id': user_id})

async def set_settings(user_id, update):
    await settings_col.update_one({'user_id': user_id}, {'$set': update}, upsert=True)

async def log_forwarded(user_id, source_channel, msg_id):
    await msglog_col.insert_one({'user_id': user_id, 'source_channel': source_channel, 'msg_id': msg_id})

async def is_forwarded(user_id, source_channel, msg_id):
    return await msglog_col.find_one({'user_id': user_id, 'source_channel': source_channel, 'msg_id': msg_id}) is not None

async def add_user(user_id):
    await users_col.update_one({'user_id': user_id}, {'$set': {'joined': datetime.utcnow()}}, upsert=True)

@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    await add_user(event.chat_id)
    await event.reply(
        "👋 Welcome!\n"
        "1. Forward a message from your **source channel** here.\n"
        "2. Forward a message from your **target channel** here.\n"
        "3. Set interval: `/set_fixed 1h` or `/set_random 1h-6h`\n"
        "4. Start: `/begin_forward`\n"
        "5. Stop: `/stop_forward`\n"
        "6. Toggle web preview: `/toggle_preview`\n"
        "7. Bulk forward all: `/forward_all`\n"
        "Admin broadcast: `/broadcast your message`"
    )

@client.on(events.NewMessage(pattern='/set_fixed'))
async def set_fixed(event):
    try:
        interval = parse_time(event.raw_text.split(maxsplit=1)[1])
        await set_settings(event.chat_id, {'mode':'fixed', 'interval':interval})
        await event.reply(f"✅ Fixed interval set to {interval} seconds.")
    except Exception:
        await event.reply("❌ Usage: `/set_fixed 1h` or `/set_fixed 90m`")

@client.on(events.NewMessage(pattern='/set_random'))
async def set_random(event):
    try:
        arg = event.raw_text.split(maxsplit=1)[1]
        min_t, max_t = arg.split('-')
        min_sec = parse_time(min_t)
        max_sec = parse_time(max_t)
        await set_settings(event.chat_id, {'mode':'random', 'min':min_sec, 'max':max_sec})
        await event.reply(f"✅ Random interval set between {min_sec}s and {max_sec}s.")
    except Exception:
        await event.reply("❌ Usage: `/set_random 1h-6h` or `/set_random 30m-2h`")

@client.on(events.NewMessage(pattern='/toggle_preview'))
async def toggle_preview(event):
    user_id = event.chat_id
    settings = await get_settings(user_id)
    new_setting = not settings.get('web_preview', True)
    await set_settings(user_id, {'web_preview': new_setting})
    status = "enabled" if new_setting else "disabled"
    await event.reply(f"🌐 Web preview {status}!")

@client.on(events.NewMessage(pattern='/begin_forward'))
async def begin_forward(event):
    user_id = event.chat_id
    if user_id in forwarding_tasks and not forwarding_tasks[user_id].done():
        await event.reply("⏳ Forwarding already running.")
        return
    settings = await get_settings(user_id)
    if not settings or 'source_channel' not in settings or 'target_channel' not in settings or 'mode' not in settings:
        await event.reply("❌ Please set source, target, and interval first.")
        return
    task = asyncio.create_task(forward_loop(user_id))
    forwarding_tasks[user_id] = task
    await event.reply("🚀 Scheduled forwarding started!")

@client.on(events.NewMessage(pattern='/stop_forward'))
async def stop_forward(event):
    user_id = event.chat_id
    task = forwarding_tasks.get(user_id)
    if task and not task.done():
        task.cancel()
        await event.reply("🛑 Forwarding stopped.")
    else:
        await event.reply("ℹ️ No active forwarding.")

@client.on(events.NewMessage(pattern='/forward_all'))
async def forward_all_messages(event):
    user_id = event.chat_id
    settings = await get_settings(user_id)
    if not settings or 'source_channel' not in settings or 'target_channel' not in settings:
        await event.reply("❌ Please set source and target channels first!")
        return
    if user_id in forwarding_tasks and not forwarding_tasks[user_id].done():
        await event.reply("⏳ Another operation is already in progress.")
        return
    task = asyncio.create_task(bulk_forward(user_id))
    forwarding_tasks[user_id] = task
    await event.reply("🚀 Bulk forwarding started! This may take time...")

async def bulk_forward(user_id):
    try:
        settings = await get_settings(user_id)
        source = settings['source_channel']
        target = settings['target_channel']
        web_preview = settings.get('web_preview', True)
        count = 0
        async for msg in client.iter_messages(source, reverse=True):
            if await is_forwarded(user_id, source, msg.id):
                continue
            try:
                if msg.media:
                    if isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
                        await client.send_file(
                            target,
                            msg.media,
                            caption=msg.text or "",
                            link_preview=web_preview
                        )
                else:
                    await client.send_message(
                        target,
                        msg.text,
                        link_preview=web_preview
                    )
                await log_forwarded(user_id, source, msg.id)
                count += 1
                await asyncio.sleep(1)  # Basic rate limiting
            except Exception as e:
                print(f"Bulk forward error: {e}")
                await asyncio.sleep(5)
        await client.send_message(user_id, f"✅ Bulk forwarding completed! {count} messages/media sent.")
    except asyncio.CancelledError:
        await client.send_message(user_id, "⏹ Bulk forwarding stopped.")

@client.on(events.NewMessage(pattern='/broadcast'))
async def broadcast(event):
    if event.chat_id != ADMIN_ID:
        await event.reply("❌ Only admin can broadcast.")
        return
    msg = event.raw_text[len('/broadcast'):].strip()
    if not msg:
        await event.reply("❌ Usage: `/broadcast your message`")
        return
    count = 0
    async for user in users_col.find({}):
        try:
            await client.send_message(user['user_id'], f"📢 Broadcast:\n{msg}")
            count += 1
        except Exception:
            pass
    await event.reply(f"✅ Broadcast sent to {count} users.")

@client.on(events.NewMessage(incoming=True, forwards=True))
async def set_channel(event):
    user_id = event.chat_id
    fwd = event.forward
    if hasattr(fwd, 'chat') and hasattr(fwd.chat, 'id'):
        channel_id = fwd.chat.id
        settings = await get_settings(user_id)
        if not settings or 'source_channel' not in settings:
            await set_settings(user_id, {'source_channel': channel_id})
            await event.reply("✅ Source channel set! Now forward a message from your target channel.")
        elif 'target_channel' not in settings:
            await set_settings(user_id, {'target_channel': channel_id})
            await event.reply("✅ Target channel set! Now set interval and start forwarding.")
        else:
            await event.reply("ℹ️ Channels already set. Use /set_fixed or /set_random to change interval.")

async def forward_loop(user_id):
    try:
        settings = await get_settings(user_id)
        source = settings['source_channel']
        target = settings['target_channel']
        mode = settings['mode']
        interval = settings.get('interval')
        min_t = settings.get('min')
        max_t = settings.get('max')
        web_preview = settings.get('web_preview', True)
        async for msg in client.iter_messages(source, reverse=True):
            if await is_forwarded(user_id, source, msg.id):
                continue
            try:
                if msg.text:
                    await client.send_message(
                        target, 
                        msg.text, 
                        link_preview=web_preview
                    )
                elif msg.media:
                    await client.send_file(
                        target, 
                        msg.media, 
                        caption=msg.text or "",
                        link_preview=web_preview
                    )
                await log_forwarded(user_id, source, msg.id)
            except ChatWriteForbiddenError:
                await client.send_message(user_id, "❌ Bot is not admin in target channel!")
                break
            except Exception as e:
                print(f"Failed to forward: {e}")
            if mode == 'fixed':
                await asyncio.sleep(interval)
            else:
                await asyncio.sleep(random.randint(min_t, max_t))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"Forward loop error: {e}")

# --- FastAPI Web Dashboard ---
app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    users = []
    async for user in users_col.find({}):
        settings = await settings_col.find_one({'user_id': user['user_id']})
        users.append({
            "user_id": user['user_id'],
            "source_channel": settings.get('source_channel') if settings else None,
            "target_channel": settings.get('target_channel') if settings else None,
            "mode": settings.get('mode') if settings else None,
            "interval": settings.get('interval') if settings else None,
            "min": settings.get('min') if settings else None,
            "max": settings.get('max') if settings else None,
            "web_preview": settings.get('web_preview', True) if settings else True,
        })
    html = "<h2>Telegram Forward Bot Dashboard</h2><table border=1><tr><th>User ID</th><th>Source Channel</th><th>Target Channel</th><th>Mode</th><th>Interval</th><th>Min</th><th>Max</th><th>Web Preview</th></tr>"
    for u in users:
        html += f"<tr><td>{u['user_id']}</td><td>{u['source_channel']}</td><td>{u['target_channel']}</td><td>{u['mode']}</td><td>{u['interval']}</td><td>{u['min']}</td><td>{u['max']}</td><td>{u['web_preview']}</td></tr>"
    html += "</table>"
    return html

async def start_all():
    await client.start(bot_token=BOT_TOKEN)
    print("Telegram bot started")
    # Run both the bot and the web server in the same event loop
    bot_task = asyncio.create_task(client.run_until_disconnected())
    config = uvicorn.Config(app, host="0.0.0.0", port=8080, loop="asyncio")
    server = uvicorn.Server(config)
    web_task = asyncio.create_task(server.serve())
    await asyncio.gather(bot_task, web_task)

if __name__ == "__main__":
    asyncio.run(start_all())
