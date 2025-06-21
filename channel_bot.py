import asyncio
import random
import os
from typing import List
from pyrogram import Client, filters, types
from pymongo import MongoClient

class ChannelCopyBot:
    def __init__(self):
        self.bot = Client(
            "channel_copy_bot",
            api_id=int(os.getenv("API_ID")),
            api_hash=os.getenv("API_HASH"),
            bot_token=os.getenv("BOT_TOKEN")
        )
        self.db = MongoClient(os.getenv("MONGO_URI"))[os.getenv("DB_NAME", "promutthal_bot")]

        self.bot.add_handler(filters.command(["setsource", "settarget", "startcopying"])(self.restricted_commands))
        self.bot.add_handler(filters.command("adduser")(self.add_user_cmd))
        self.bot.add_handler(filters.command("removeuser")(self.remove_user_cmd))
        self.bot.add_handler(filters.command("listusers")(self.list_users))
        self.bot.add_handler(filters.command("start")(self.set_owner_if_not_set))

    def get_users(self):
        doc = self.db.users.find_one({"_id": "access"}) or {}
        return doc.get("owner_id"), doc.get("approved_users", [])

    def is_authorized(self, user_id):
        owner, approved = self.get_users()
        return user_id == owner or user_id in approved

    def is_owner(self, user_id):
        owner, _ = self.get_users()
        return user_id == owner

    def add_user(self, user_id):
        self.db.users.update_one({"_id": "access"}, {"$addToSet": {"approved_users": user_id}}, upsert=True)

    def remove_user(self, user_id):
        self.db.users.update_one({"_id": "access"}, {"$pull": {"approved_users": user_id}})

    def set_config(self, key, value):
        self.db.config.update_one({"_id": "config"}, {"$set": {key: value}}, upsert=True)

    def get_config(self):
        return self.db.config.find_one({"_id": "config"}) or {}

    def add_posted_id(self, msg_id):
        self.db.state.update_one({"_id": "posted"}, {"$addToSet": {"posted_ids": msg_id}}, upsert=True)

    def get_posted_ids(self):
        doc = self.db.state.find_one({"_id": "posted"}) or {}
        return doc.get("posted_ids", [])

    async def set_owner_if_not_set(self, _, msg):
        owner, _ = self.get_users()
        if not owner:
            self.db.users.update_one(
                {"_id": "access"},
                {"$set": {"owner_id": msg.from_user.id}, "$setOnInsert": {"approved_users": []}},
                upsert=True
            )
            await msg.reply(f"👑 You have been set as the owner of the bot. Your ID: `{msg.from_user.id}`")
        else:
            await msg.reply("✅ Bot is already configured.")

    async def restricted_commands(self, client, msg):
        if not self.is_authorized(msg.from_user.id):
            await msg.reply("You are not authorized to use this bot.")
            return

        cmd = msg.command[0]
        if cmd == "setsource" and len(msg.command) >= 2:
            self.set_config("source", msg.command[1])
            await msg.reply(f"Source set to `{msg.command[1]}`")
        elif cmd == "settarget" and len(msg.command) >= 2:
            self.set_config("target", msg.command[1])
            await msg.reply(f"Target set to `{msg.command[1]}`")
        elif cmd == "startcopying":
            await msg.reply("Starting message copy...")
            asyncio.create_task(self.auto_post())

    async def add_user_cmd(self, _, msg):
        if not self.is_owner(msg.from_user.id):
            await msg.reply("Only the owner can add users.")
            return
        if len(msg.command) < 2:
            await msg.reply("Usage: /adduser <user_id>")
            return
        self.add_user(int(msg.command[1]))
        await msg.reply(f"User `{msg.command[1]}` added.")

    async def remove_user_cmd(self, _, msg):
        if not self.is_owner(msg.from_user.id):
            await msg.reply("Only the owner can remove users.")
            return
        if len(msg.command) < 2:
            await msg.reply("Usage: /removeuser <user_id>")
            return
        self.remove_user(int(msg.command[1]))
        await msg.reply(f"User `{msg.command[1]}` removed.")

    async def list_users(self, _, msg):
        if not self.is_owner(msg.from_user.id):
            await msg.reply("❌ Only the owner can view users.")
            return
        owner, users = self.get_users()
        text = f"👑 **Owner ID:** `{owner}`\n✅ **Approved Users:**\n"
        text += "\n".join(f"`{u}`" for u in users) if users else "None"
        await msg.reply(text)

    async def fetch_all_messages(self, channel) -> List[types.Message]:
        messages = []
        async for msg in self.bot.get_chat_history(channel):
            messages.append(msg)
        return list(reversed(messages))

    async def auto_post(self):
        cfg = self.get_config()
        source = cfg.get("source")
        target = cfg.get("target")

        if not source or not target:
            print("Source or target not set.")
            return

        posted_ids = set(self.get_posted_ids())
        async with self.bot:
            all_msgs = await self.fetch_all_messages(source)
            filtered_msgs = [m for m in all_msgs if m.id not in posted_ids]

            while filtered_msgs:
                msg = random.choice(filtered_msgs)
                try:
                    if msg.media_group_id:
                        group = [m async for m in self.bot.get_media_group(source, msg.message_id)]
                        media = [types.InputMediaPhoto(m.photo.file_id, caption=m.caption or "") for m in group if m.photo]
                        if media:
                            await self.bot.send_media_group(target, media)
                    elif msg.video:
                        await self.bot.send_video(target, msg.video.file_id, caption=msg.caption)
                    elif msg.photo:
                        await self.bot.send_photo(target, msg.photo.file_id, caption=msg.caption)
                    elif msg.text:
                        await self.bot.send_message(target, msg.text)
                except Exception as e:
                    print(f"Error: {e}")
                self.add_posted_id(msg.id)
                filtered_msgs.remove(msg)

                wait_time = random.randint(3600, 10800)
                print(f"Sleeping for {wait_time // 60} min")
                await asyncio.sleep(wait_time)

    def run(self):
        self.bot.run()