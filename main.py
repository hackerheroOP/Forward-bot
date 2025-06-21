from fastapi import FastAPI
from threading import Thread
from channel_bot import ChannelCopyBot
import asyncio

app = FastAPI()

@app.get('/')
def status():
    return {"status": "Bot is running"}

def run_bot():
    asyncio.set_event_loop(asyncio.new_event_loop())  # ✅ Fix for event loop in thread
    bot = ChannelCopyBot()
    bot.run()

Thread(target=run_bot).start()
