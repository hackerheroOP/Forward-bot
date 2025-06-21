from fastapi import FastAPI
from threading import Thread
from channel_bot import ChannelCopyBot

app = FastAPI()

@app.get('/')
def status():
    return {"status": "Bot is running"}

def run_bot():
    bot = ChannelCopyBot()
    bot.run()

Thread(target=run_bot).start()