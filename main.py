import asyncio
import random
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters, types
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import os
from dotenv import load_dotenv
from flask import Flask, render_template, jsonify
import threading
import json
from collections import defaultdict
import concurrent.futures

load_dotenv()

# Configure comprehensive logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger(__name__)

# Bot configuration
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
MONGO_URI = os.getenv('MONGO_URI')
ADMIN_IDS = [int(x) for x in os.getenv('ADMIN_IDS', '').split(',') if x]
PORT = int(os.getenv('PORT', 8000))

logger.info(f"Bot configuration loaded - API_ID: {API_ID}, PORT: {PORT}")

# Initialize Flask app
flask_app = Flask(__name__)

# Initialize bot
app = Client("forwarder_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# MongoDB setup
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client.forwarder_bot

# Collections
users_collection = db.users
channels_collection = db.channels
schedules_collection = db.schedules
forwarded_messages_collection = db.forwarded_messages
analytics_collection = db.analytics

def get_utc_now():
    """Get current UTC datetime in a timezone-aware format"""
    return datetime.now(timezone.utc)

def get_utc_date():
    """Get current UTC date as datetime at midnight"""
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

class ForwarderBot:
    def __init__(self):
        self.forwarding_tasks = {}
        self.is_running = {}
        self.stats_cache = {}
        self.last_stats_update = None
        self.message_cache = {}  # Cache for storing messages
        logger.info("ForwarderBot initialized")
        
    async def init_db(self):
        """Initialize database indexes"""
        try:
            await users_collection.create_index("user_id", unique=True)
            await channels_collection.create_index([("user_id", 1), ("source_channel", 1)])
            await forwarded_messages_collection.create_index([("source_channel", 1), ("message_id", 1)])
            await analytics_collection.create_index([("date", 1), ("metric", 1)])
            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating database indexes: {e}")
        
    async def add_user(self, user_id: int, username: str = None):
        """Add user to database"""
        try:
            now = get_utc_now()
            user_data = {
                "user_id": user_id,
                "username": username,
                "joined_date": now,
                "is_active": True,
                "last_activity": now
            }
            result = await users_collection.update_one(
                {"user_id": user_id},
                {"$set": user_data},
                upsert=True
            )
            
            if result.upserted_id:
                await self.track_analytics("new_user", 1)
                logger.info(f"New user added: {user_id} (@{username})")
            else:
                logger.info(f"Existing user updated: {user_id} (@{username})")
        except Exception as e:
            logger.error(f"Error adding user {user_id}: {e}")
            
    async def update_user_activity(self, user_id: int):
        """Update user last activity"""
        try:
            await users_collection.update_one(
                {"user_id": user_id},
                {"$set": {"last_activity": get_utc_now(), "is_active": True}}
            )
            logger.debug(f"Updated activity for user {user_id}")
        except Exception as e:
            logger.error(f"Error updating user activity {user_id}: {e}")
        
    async def track_analytics(self, metric: str, value: int = 1):
        """Track analytics data"""
        try:
            today = get_utc_date()
            await analytics_collection.update_one(
                {"date": today, "metric": metric},
                {"$inc": {"value": value}},
                upsert=True
            )
            logger.debug(f"Analytics tracked: {metric} = {value}")
        except Exception as e:
            logger.error(f"Error tracking analytics {metric}: {e}")
        
    async def get_user_channels(self, user_id: int):
        """Get user's configured channels"""
        try:
            channels = await channels_collection.find({"user_id": user_id}).to_list(None)
            logger.debug(f"Retrieved {len(channels)} channels for user {user_id}")
            return channels
        except Exception as e:
            logger.error(f"Error getting user channels {user_id}: {e}")
            return []
        
    async def add_channel_pair(self, user_id: int, source_channel: int, target_channel: int = None):
        """Add source-target channel pair"""
        try:
            channel_data = {
                "user_id": user_id,
                "source_channel": source_channel,
                "target_channel": target_channel,
                "created_date": get_utc_now(),
                "is_active": True,
                "last_forwarded_id": 0
            }
            await channels_collection.update_one(
                {"user_id": user_id, "source_channel": source_channel},
                {"$set": channel_data},
                upsert=True
            )
            logger.info(f"Channel pair added for user {user_id}: {source_channel} -> {target_channel}")
        except Exception as e:
            logger.error(f"Error adding channel pair: {e}")
        
    async def parse_time_interval(self, time_str: str) -> int:
        """Parse time string to seconds"""
        try:
            time_str = time_str.lower().strip()
            multipliers = {
                's': 1, 'sec': 1, 'second': 1, 'seconds': 1,
                'm': 60, 'min': 60, 'minute': 60, 'minutes': 60,
                'h': 3600, 'hr': 3600, 'hour': 3600, 'hours': 3600,
                'd': 86400, 'day': 86400, 'days': 86400
            }
            
            total_seconds = 0
            current_number = ""
            
            for char in time_str:
                if char.isdigit():
                    current_number += char
                elif char.isalpha() or char == ' ':
                    if current_number:
                        number = int(current_number)
                        unit = ""
                        remaining_str = time_str[time_str.index(current_number) + len(current_number):].strip()
                        for key in multipliers:
                            if remaining_str.startswith(key):
                                unit = key
                                break
                        if unit:
                            total_seconds += number * multipliers[unit]
                        current_number = ""
            
            if current_number:
                total_seconds += int(current_number) * 60
                
            result = max(total_seconds, 60)
            logger.debug(f"Parsed time '{time_str}' to {result} seconds")
            return result
        except Exception as e:
            logger.error(f"Error parsing time interval '{time_str}': {e}")
            return 3600

    async def get_recent_messages_from_db(self, source_channel: int, limit: int = 10):
        """Get recent messages from database cache"""
        try:
            messages = await forwarded_messages_collection.find(
                {"source_channel": source_channel}
            ).sort("forwarded_date", -1).limit(limit).to_list(None)
            return messages
        except Exception as e:
            logger.error(f"Error getting recent messages from DB: {e}")
            return []

    async def store_message_in_cache(self, source_channel: int, message_id: int, content_hash: str):
        """Store message info in cache"""
        try:
            cache_key = f"{source_channel}_{message_id}"
            self.message_cache[cache_key] = {
                "content_hash": content_hash,
                "timestamp": get_utc_now()
            }
            # Keep only last 100 messages per channel
            channel_messages = [k for k in self.message_cache.keys() if k.startswith(f"{source_channel}_")]
            if len(channel_messages) > 100:
                oldest_key = min(channel_messages, key=lambda k: self.message_cache[k]["timestamp"])
                del self.message_cache[oldest_key]
        except Exception as e:
            logger.error(f"Error storing message in cache: {e}")

    async def is_duplicate_message_by_content(self, content: str, source_channel: int) -> bool:
        """Check if message content is duplicate using cache"""
        try:
            if not content:
                return False
                
            content_hash = hash(content.strip().lower())
            
            # Check in memory cache
            for key, data in self.message_cache.items():
                if key.startswith(f"{source_channel}_") and data["content_hash"] == content_hash:
                    return True
                    
            return False
        except Exception as e:
            logger.error(f"Error checking duplicate message: {e}")
            return False
            
    async def forward_single_message(self, source_channel: int, target_channel: int, user_id: int):
        """Forward a single message from source to target using webhook/updates"""
        try:
            # Get the last forwarded message ID from database
            channel_data = await channels_collection.find_one({
                "user_id": user_id,
                "source_channel": source_channel
            })
            
            last_id = channel_data.get("last_forwarded_id", 0) if channel_data else 0
            
            # Since we can't use get_chat_history with bots, we'll rely on real-time message handling
            # This function will be called when new messages arrive via the message handler
            logger.info(f"Monitoring channel {source_channel} for new messages to forward to {target_channel}")
            return True
            
        except Exception as e:
            logger.error(f"Error in forward_single_message: {e}")
            return False

    async def handle_new_message_for_forwarding(self, message: Message):
        """Handle new messages for forwarding"""
        try:
            source_channel = message.chat.id
            
            # Find all forwarding rules for this source channel
            forwarding_rules = await channels_collection.find({
                "source_channel": source_channel,
                "target_channel": {"$ne": None},
                "is_active": True
            }).to_list(None)
            
            for rule in forwarding_rules:
                target_channel = rule["target_channel"]
                user_id = rule["user_id"]
                
                # Check if this message should be forwarded
                if message.id > rule.get("last_forwarded_id", 0):
                    try:
                        # Check for duplicates
                        content = message.text or message.caption or ""
                        if content and await self.is_duplicate_message_by_content(content, source_channel):
                            logger.info(f"Skipping duplicate message {message.id}")
                            continue
                        
                        # Forward the message
                        forwarded = await message.forward(target_channel)
                        
                        # Update last forwarded ID
                        await channels_collection.update_one(
                            {"user_id": user_id, "source_channel": source_channel},
                            {"$set": {"last_forwarded_id": message.id}}
                        )
                        
                        # Store forwarding record
                        await forwarded_messages_collection.insert_one({
                            "user_id": user_id,
                            "source_channel": source_channel,
                            "target_channel": target_channel,
                            "source_message_id": message.id,
                            "target_message_id": forwarded.id,
                            "forwarded_date": get_utc_now()
                        })
                        
                        # Cache message info
                        if content:
                            content_hash = hash(content.strip().lower())
                            await self.store_message_in_cache(source_channel, message.id, content_hash)
                        
                        await self.track_analytics("messages_forwarded", 1)
                        logger.info(f"Forwarded message {message.id} from {source_channel} to {target_channel}")
                        
                    except Exception as e:
                        logger.error(f"Error forwarding message {message.id}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error in handle_new_message_for_forwarding: {e}")
            
    async def fixed_time_forwarder(self, user_id: int, source_channel: int, target_channel: int, interval: int):
        """Monitor for forwarding at fixed intervals"""
        task_key = f"{user_id}_{source_channel}_{target_channel}"
        self.is_running[task_key] = True
        logger.info(f"Started fixed time forwarder: {task_key} with interval {interval}s")
        
        try:
            while self.is_running.get(task_key, False):
                # The actual forwarding happens in the message handler
                # This just keeps the task alive and updates status
                await asyncio.sleep(interval)
                logger.debug(f"Fixed time forwarder {task_key} is active")
        except asyncio.CancelledError:
            logger.info(f"Fixed time forwarder cancelled: {task_key}")
        except Exception as e:
            logger.error(f"Error in fixed time forwarder {task_key}: {e}")
        finally:
            self.is_running[task_key] = False
            logger.info(f"Fixed time forwarder stopped: {task_key}")
            
    async def random_time_forwarder(self, user_id: int, source_channel: int, target_channel: int, min_interval: int, max_interval: int):
        """Monitor for forwarding at random intervals"""
        task_key = f"{user_id}_{source_channel}_{target_channel}"
        self.is_running[task_key] = True
        logger.info(f"Started random time forwarder: {task_key} with interval {min_interval}-{max_interval}s")
        
        try:
            while self.is_running.get(task_key, False):
                random_interval = random.randint(min_interval, max_interval)
                logger.debug(f"Next check in {random_interval}s for {task_key}")
                await asyncio.sleep(random_interval)
                logger.debug(f"Random time forwarder {task_key} is active")
        except asyncio.CancelledError:
            logger.info(f"Random time forwarder cancelled: {task_key}")
        except Exception as e:
            logger.error(f"Error in random time forwarder {task_key}: {e}")
        finally:
            self.is_running[task_key] = False
            logger.info(f"Random time forwarder stopped: {task_key}")
            
    async def start_forwarding(self, user_id: int, source_channel: int, target_channel: int, mode: str, **kwargs):
        """Start forwarding task"""
        try:
            task_key = f"{user_id}_{source_channel}_{target_channel}"
            
            if task_key in self.forwarding_tasks:
                self.forwarding_tasks[task_key].cancel()
                logger.info(f"Cancelled existing task: {task_key}")
                
            if mode == "fixed":
                interval = kwargs.get("interval", 3600)
                task = asyncio.create_task(
                    self.fixed_time_forwarder(user_id, source_channel, target_channel, interval)
                )
            elif mode == "random":
                min_interval = kwargs.get("min_interval", 3600)
                max_interval = kwargs.get("max_interval", 21600)
                task = asyncio.create_task(
                    self.random_time_forwarder(user_id, source_channel, target_channel, min_interval, max_interval)
                )
            else:
                logger.error(f"Invalid forwarding mode: {mode}")
                return False
                
            self.forwarding_tasks[task_key] = task
            
            schedule_data = {
                "user_id": user_id,
                "source_channel": source_channel,
                "target_channel": target_channel,
                "mode": mode,
                "created_date": get_utc_now(),
                "is_active": True,
                **kwargs
            }
            await schedules_collection.update_one(
                {"user_id": user_id, "source_channel": source_channel, "target_channel": target_channel},
                {"$set": schedule_data},
                upsert=True
            )
            
            await self.track_analytics("tasks_started", 1)
            logger.info(f"Started forwarding task: {task_key} in {mode} mode")
            return True
        except Exception as e:
            logger.error(f"Error starting forwarding task: {e}")
            return False
        
    async def stop_forwarding(self, user_id: int, source_channel: int, target_channel: int):
        """Stop forwarding task"""
        try:
            task_key = f"{user_id}_{source_channel}_{target_channel}"
            
            if task_key in self.forwarding_tasks:
                self.forwarding_tasks[task_key].cancel()
                del self.forwarding_tasks[task_key]
                logger.info(f"Cancelled and removed task: {task_key}")
                
            self.is_running[task_key] = False
            
            await schedules_collection.update_one(
                {"user_id": user_id, "source_channel": source_channel, "target_channel": target_channel},
                {"$set": {"is_active": False}}
            )
            
            await self.track_analytics("tasks_stopped", 1)
            logger.info(f"Stopped forwarding task: {task_key}")
        except Exception as e:
            logger.error(f"Error stopping forwarding task: {e}")
        
    async def broadcast_message(self, message_text: str, admin_id: int):
        """Broadcast message to all users"""
        try:
            users = await users_collection.find({"is_active": True}).to_list(None)
            success_count = 0
            failed_count = 0
            
            logger.info(f"Starting broadcast to {len(users)} users")
            
            for user in users:
                try:
                    await app.send_message(user["user_id"], message_text)
                    success_count += 1
                    await asyncio.sleep(0.1)
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Failed to send to {user['user_id']}: {e}")
                    
            await self.track_analytics("broadcasts_sent", 1)
            
            report = f"📊 **Broadcast Report**\n\n✅ Success: {success_count}\n❌ Failed: {failed_count}\n📊 Total: {len(users)}"
            await app.send_message(admin_id, report)
            logger.info(f"Broadcast completed: {success_count} success, {failed_count} failed")
        except Exception as e:
            logger.error(f"Error in broadcast: {e}")

    def get_dashboard_stats_sync(self):
        """Synchronous wrapper for dashboard stats"""
        if self.last_stats_update and (get_utc_now() - self.last_stats_update).seconds < 300:
            return self.stats_cache
            
        try:
            logger.debug("Generating dashboard stats (sync)")
            
            # Use thread pool for database operations
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(self._get_basic_stats)
                stats = future.result(timeout=10)
                
            self.stats_cache = stats
            self.last_stats_update = get_utc_now()
            return self.stats_cache
            
        except Exception as e:
            logger.error(f"Error getting dashboard stats (sync): {e}")
            return {
                "total_users": 0,
                "active_users": 0,
                "new_users": 0,
                "growth_rate": 0,
                "active_tasks": 0,
                "total_forwarded": 0,
                "today_forwarded": 0,
                "total_channels": 0,
                "daily_stats": [],
                "current_tasks": [],
                "last_updated": get_utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
            }

    def _get_basic_stats(self):
        """Get basic stats without async operations"""
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                return loop.run_until_complete(self._async_get_basic_stats())
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"Error in _get_basic_stats: {e}")
            return {
                "total_users": 0,
                "active_users": 0,
                "new_users": 0,
                "growth_rate": 0,
                "active_tasks": 0,
                "total_forwarded": 0,
                "today_forwarded": 0,
                "total_channels": 0,
                "daily_stats": [],
                "current_tasks": [],
                "last_updated": get_utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
            }

    async def _async_get_basic_stats(self):
        """Async version of basic stats"""
        try:
            # Basic user stats
            total_users = await users_collection.count_documents({})
            
            # Active users (last 30 days)
            thirty_days_ago = get_utc_now() - timedelta(days=30)
            active_users = await users_collection.count_documents({
                "last_activity": {"$gte": thirty_days_ago}
            })
            
            # New users (last 7 days)
            week_ago = get_utc_now() - timedelta(days=7)
            new_users = await users_collection.count_documents({
                "joined_date": {"$gte": week_ago}
            })
            
            # Calculate growth rate
            prev_week = get_utc_now() - timedelta(days=14)
            prev_week_users = await users_collection.count_documents({
                "joined_date": {"$gte": prev_week, "$lt": week_ago}
            })
            
            growth_rate = 0
            if prev_week_users > 0:
                growth_rate = ((new_users - prev_week_users) / prev_week_users) * 100
            elif new_users > 0:
                growth_rate = 100
                
            # Active tasks
            active_tasks = await schedules_collection.count_documents({"is_active": True})
            
            # Total messages forwarded
            total_forwarded = await forwarded_messages_collection.count_documents({})
            
            # Messages forwarded today
            today_start = get_utc_date()
            today_forwarded = await forwarded_messages_collection.count_documents({
                "forwarded_date": {"$gte": today_start}
            })
            
            # Channel pairs
            total_channels = await channels_collection.count_documents({"target_channel": {"$ne": None}})
            
            # Get daily stats for chart (last 7 days)
            daily_stats = []
            for i in range(7):
                date_start = get_utc_date() - timedelta(days=i)
                date_end = date_start + timedelta(days=1)
                
                day_users = await users_collection.count_documents({
                    "joined_date": {"$gte": date_start, "$lt": date_end}
                })
                day_forwards = await forwarded_messages_collection.count_documents({
                    "forwarded_date": {"$gte": date_start, "$lt": date_end}
                })
                daily_stats.append({
                    "date": date_start.strftime("%Y-%m-%d"),
                    "new_users": day_users,
                    "messages_forwarded": day_forwards
                })
            
            return {
                "total_users": total_users,
                "active_users": active_users,
                "new_users": new_users,
                "growth_rate": round(growth_rate, 2),
                "active_tasks": active_tasks,
                "total_forwarded": total_forwarded,
                "today_forwarded": today_forwarded,
                "total_channels": total_channels,
                "daily_stats": list(reversed(daily_stats)),
                "current_tasks": [],  # Simplified for now
                "last_updated": get_utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
            }
            
        except Exception as e:
            logger.error(f"Error in _async_get_basic_stats: {e}")
            raise

# Initialize bot instance
bot = ForwarderBot()

# Flask Routes
@flask_app.route('/')
def dashboard():
    """Main dashboard page"""
    logger.info("Dashboard page accessed")
    return '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Telegram Forwarder Bot Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; color: white; margin-bottom: 30px; }
        .header h1 { font-size: 2.5rem; margin-bottom: 10px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: white; border-radius: 15px; padding: 25px; box-shadow: 0 10px 30px rgba(0,0,0,0.1); transition: transform 0.3s ease; }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-card h3 { color: #666; font-size: 0.9rem; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }
        .stat-card .value { font-size: 2.5rem; font-weight: bold; color: #333; margin-bottom: 5px; }
        .stat-card .change { font-size: 0.9rem; color: #28a745; }
        .chart-container { background: white; border-radius: 15px; padding: 25px; margin-bottom: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.1); }
        .tasks-container { background: white; border-radius: 15px; padding: 25px; box-shadow: 0 10px 30px rgba(0,0,0,0.1); }
        .task-item { border-bottom: 1px solid #eee; padding: 15px 0; display: grid; grid-template-columns: 1fr 1fr 1fr auto; gap: 15px; align-items: center; }
        .task-item:last-child { border-bottom: none; }
        .status-badge { padding: 5px 10px; border-radius: 20px; font-size: 0.8rem; font-weight: bold; }
        .status-running { background: #d4edda; color: #155724; }
        .status-stopped { background: #f8d7da; color: #721c24; }
        .refresh-btn { position: fixed; bottom: 30px; right: 30px; background: #007bff; color: white; border: none; border-radius: 50px; padding: 15px 25px; cursor: pointer; box-shadow: 0 5px 15px rgba(0,123,255,0.3); transition: all 0.3s ease; }
        .refresh-btn:hover { background: #0056b3; transform: scale(1.05); }
        .loading { text-align: center; color: #666; padding: 50px; }
        @media (max-width: 768px) { .stats-grid { grid-template-columns: 1fr; } .task-item { grid-template-columns: 1fr; text-align: center; } }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🤖 Telegram Forwarder Bot</h1>
            <p>Real-time Dashboard & Analytics</p>
        </div>
        <div id="stats-container">
            <div class="loading"><h3>Loading dashboard...</h3></div>
        </div>
    </div>
    <button class="refresh-btn" onclick="loadDashboard()">🔄 Refresh</button>
    <script>
        let chartInstance = null;
        async function loadDashboard() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                document.getElementById('stats-container').innerHTML = `
                    <div class="stats-grid">
                        <div class="stat-card"><h3>Total Users</h3><div class="value">${data.total_users.toLocaleString()}</div></div>
                        <div class="stat-card"><h3>Active Users (30d)</h3><div class="value">${data.active_users.toLocaleString()}</div></div>
                        <div class="stat-card"><h3>New Users (7d)</h3><div class="value">${data.new_users.toLocaleString()}</div><div class="change">Growth: ${data.growth_rate}%</div></div>
                        <div class="stat-card"><h3>Active Tasks</h3><div class="value">${data.active_tasks}</div></div>
                        <div class="stat-card"><h3>Total Forwarded</h3><div class="value">${data.total_forwarded.toLocaleString()}</div></div>
                        <div class="stat-card"><h3>Today's Forwards</h3><div class="value">${data.today_forwarded.toLocaleString()}</div></div>
                        <div class="stat-card"><h3>Channel Pairs</h3><div class="value">${data.total_channels}</div></div>
                        <div class="stat-card"><h3>Last Updated</h3><div class="value" style="font-size: 1rem;">${data.last_updated}</div></div>
                    </div>
                    <div class="chart-container">
                        <h3 style="margin-bottom: 20px;">📊 Daily Activity (Last 7 Days)</h3>
                        <canvas id="activityChart" width="400" height="200"></canvas>
                    </div>
                    <div class="tasks-container">
                        <h3 style="margin-bottom: 20px;">🔄 Current Active Tasks</h3>
                        ${data.current_tasks.length === 0 ? '<p style="text-align: center; color: #666; padding: 20px;">No active tasks</p>' :
                            data.current_tasks.map(task => `
                                <div class="task-item">
                                    <div><strong>@${task.username}</strong><br><small>User ID: ${task.user_id}</small></div>
                                    <div><strong>${task.source_channel}</strong><br>↓<br><strong>${task.target_channel}</strong></div>
                                    <div><strong>${task.mode.toUpperCase()}</strong><br><small>${task.schedule}</small><br><small>Started: ${task.created_date}</small></div>
                                    <div><span class="status-badge ${task.status === 'Running' ? 'status-running' : 'status-stopped'}">${task.status}</span></div>
                                </div>
                            `).join('')
                        }
                    </div>
                `;
                const ctx = document.getElementById('activityChart').getContext('2d');
                if (chartInstance) { chartInstance.destroy(); }
                chartInstance = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: data.daily_stats.map(d => d.date),
                        datasets: [
                            { label: 'New Users', data: data.daily_stats.map(d => d.new_users), borderColor: '#007bff', backgroundColor: 'rgba(0, 123, 255, 0.1)', tension: 0.4 },
                            { label: 'Messages Forwarded', data: data.daily_stats.map(d => d.messages_forwarded), borderColor: '#28a745', backgroundColor: 'rgba(40, 167, 69, 0.1)', tension: 0.4 }
                        ]
                    },
                    options: { responsive: true, plugins: { legend: { position: 'top' } }, scales: { y: { beginAtZero: true } } }
                });
            } catch (error) {
                console.error('Error loading dashboard:', error);
                document.getElementById('stats-container').innerHTML = '<div class="loading"><h3 style="color: #dc3545;">Error loading dashboard</h3><p>Please try refreshing the page</p></div>';
            }
        }
        loadDashboard();
        setInterval(loadDashboard, 300000);
    </script>
</body>
</html>
    '''

@flask_app.route('/api/stats')
def api_stats():
    """API endpoint for dashboard statistics"""
    try:
        logger.debug("API stats endpoint called")
        stats = bot.get_dashboard_stats_sync()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error in API stats: {e}")
        return jsonify({"error": "Failed to fetch stats"}), 500

@flask_app.route('/health')
def health_check():
    """Health check endpoint for deployment platforms"""
    logger.debug("Health check endpoint called")
    return jsonify({"status": "healthy", "timestamp": get_utc_now().isoformat()})

# Bot handlers with comprehensive logging
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    try:
        logger.info(f"/start command triggered by user {message.from_user.id} (@{message.from_user.username})")
        user_id = message.from_user.id
        username = message.from_user.username
        await bot.add_user(user_id, username)
        await bot.update_user_activity(user_id)
        
        welcome_text = """🤖 **Welcome to Advanced Forwarder Bot!**

**Features:**
📤 Forward messages between channels
⏰ Fixed time scheduling
🎲 Random time intervals  
📢 Broadcast messages (Admin only)
📊 Web Dashboard

**Commands:**
/help - Show all commands
/addchannel - Add source/target channels
/schedule - Set up forwarding schedule
/stop - Stop forwarding
/status - Check forwarding status
/channels - List your channels

**Setup:**
1. Add me to your source and target channels as admin
2. Use /addchannel to configure channels
3. Use /schedule to start forwarding

Let's get started! 🚀"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📚 Help", callback_data="help")],
            [InlineKeyboardButton("⚙️ Add Channels", callback_data="add_channels")]
        ])
        
        await message.reply(welcome_text, reply_markup=keyboard)
        logger.info(f"Welcome message sent to user {user_id}")
    except Exception as e:
        logger.error(f"Error in /start command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

@app.on_message(filters.command("help"))
async def help_command(client, message: Message):
    try:
        logger.info(f"/help command triggered by user {message.from_user.id}")
        await bot.update_user_activity(message.from_user.id)
        
        help_text = """📚 **Bot Commands Help**

**Channel Management:**
/addchannel - Add source and target channels
/channels - List configured channels

**Forwarding Control:**
/schedule - Set up forwarding schedule
/stop - Stop active forwarding
/status - Check forwarding status

**Admin Only:**
/broadcast - Broadcast message to all users
/users - Get user statistics

**Time Format Examples:**
- `1h 30m` = 1 hour 30 minutes
- `2h` = 2 hours  
- `30m` = 30 minutes
- `1d` = 1 day

**Usage Examples:**
1. Forward every 2 hours: `/schedule fixed 2h`
2. Random 1-6 hours: `/schedule random 1h 6h`"""
        await message.reply(help_text)
        logger.info(f"Help message sent to user {message.from_user.id}")
    except Exception as e:
        logger.error(f"Error in /help command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

@app.on_message(filters.command("addchannel"))
async def add_channel_command(client, message: Message):
    try:
        logger.info(f"/addchannel command triggered by user {message.from_user.id}")
        await bot.update_user_activity(message.from_user.id)
        
        text = """📝 **Add Channel Pair**

To add channels, forward one message from your **source channel** and one from your **target channel**.

**Steps:**
1. Forward a message from source channel
2. Forward a message from target channel  
3. I'll automatically detect and save the pair

**Requirements:**
- Bot must be admin in both channels
- Channels must be accessible to the bot

Forward the first message now! 👇"""
        await message.reply(text)
        logger.info(f"Add channel instructions sent to user {message.from_user.id}")
    except Exception as e:
        logger.error(f"Error in /addchannel command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

@app.on_message(filters.forwarded)
async def handle_forwarded_message(client, message: Message):
    try:
        logger.info(f"Forwarded message received from user {message.from_user.id}")
        await bot.update_user_activity(message.from_user.id)
        user_id = message.from_user.id
        
        if message.forward_from_chat:
            channel_id = message.forward_from_chat.id
            channel_title = message.forward_from_chat.title
            logger.info(f"Processing forwarded message from channel {channel_id} ({channel_title})")
            
            user_channels = await bot.get_user_channels(user_id)
            temp_source = None
            
            for channel_pair in user_channels:
                if channel_pair.get("target_channel") is None:
                    temp_source = channel_pair["source_channel"]
                    break
                    
            if temp_source is None:
                await bot.add_channel_pair(user_id, channel_id, None)
                await message.reply(f"✅ **Source channel added:** {channel_title}\n\nNow forward a message from your **target channel**.")
                logger.info(f"Source channel {channel_id} added for user {user_id}")
            else:
                await channels_collection.update_one(
                    {"user_id": user_id, "source_channel": temp_source},
                    {"$set": {"target_channel": channel_id}}
                )
                
                try:
                    source_info = await app.get_chat(temp_source)
                    target_info = await app.get_chat(channel_id)
                    
                    success_text = f"""✅ **Channel pair configured successfully!**

📤 **Source:** {source_info.title}
📥 **Target:** {target_info.title}

Use /schedule to start forwarding! 🚀"""
                    
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("⏰ Schedule Now", callback_data=f"schedule_{temp_source}_{channel_id}")]
                    ])
                    
                    await message.reply(success_text, reply_markup=keyboard)
                    logger.info(f"Channel pair completed for user {user_id}: {temp_source} -> {channel_id}")
                except Exception as e:
                    await message.reply("✅ **Channel pair configured!** Use /schedule to start forwarding.")
                    logger.error(f"Error getting channel info: {e}")
        else:
            await message.reply("❌ Please forward a message from a channel.")
    except Exception as e:
        logger.error(f"Error handling forwarded message: {e}")
        await message.reply("❌ An error occurred processing the forwarded message.")

# New message handler for automatic forwarding
@app.on_message(filters.channel)
async def handle_channel_message(client, message: Message):
    """Handle new messages in channels for automatic forwarding"""
    try:
        await bot.handle_new_message_for_forwarding(message)
    except Exception as e:
        logger.error(f"Error in channel message handler: {e}")

@app.on_message(filters.command("schedule"))
async def schedule_command(client, message: Message):
    try:
        logger.info(f"/schedule command triggered by user {message.from_user.id}")
        await bot.update_user_activity(message.from_user.id)
        
        user_id = message.from_user.id
        args = message.text.split()[1:]
        
        if len(args) < 2:
            help_text = """⏰ **Schedule Forwarding**

**Fixed Time Mode:**
`/schedule fixed <interval>`
Example: `/schedule fixed 2h 30m`

**Random Time Mode:**  
`/schedule random <min_time> <max_time>`
Example: `/schedule random 1h 6h`

**Time Format:**
- s/sec/second(s)
- m/min/minute(s)  
- h/hr/hour(s)
- d/day(s)

Select a channel pair first using the buttons below:"""
            
            channels = await bot.get_user_channels(user_id)
            keyboard = []
            
            for channel in channels:
                if channel.get("target_channel"):
                    try:
                        source_info = await app.get_chat(channel["source_channel"])
                        target_info = await app.get_chat(channel["target_channel"])
                        button_text = f"{source_info.title[:15]}→{target_info.title[:15]}"
                        callback_data = f"schedule_{channel['source_channel']}_{channel['target_channel']}"
                        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
                    except:
                        continue
                        
            if keyboard:
                await message.reply(help_text, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await message.reply("❌ No channel pairs found. Use /addchannel first.")
            return
            
        mode = args[0].lower()
        
        if mode == "fixed" and len(args) >= 2:
            interval_str = " ".join(args[1:])
            interval = await bot.parse_time_interval(interval_str)
            
            channels = await bot.get_user_channels(user_id)
            keyboard = []
            
            for channel in channels:
                if channel.get("target_channel"):
                    try:
                        source_info = await app.get_chat(channel["source_channel"])
                        target_info = await app.get_chat(channel["target_channel"])
                        button_text = f"{source_info.title[:15]}→{target_info.title[:15]}"
                        callback_data = f"start_fixed_{channel['source_channel']}_{channel['target_channel']}_{interval}"
                        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
                    except:
                        continue
                        
            if keyboard:
                await message.reply(f"⏰ **Fixed Schedule:** Every {interval_str}\n\nSelect channel pair:", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await message.reply("❌ No channel pairs found.")
                
        elif mode == "random" and len(args) >= 3:
            min_time_str = args[1]
            max_time_str = args[2]
            min_interval = await bot.parse_time_interval(min_time_str)
            max_interval = await bot.parse_time_interval(max_time_str)
            
            if min_interval >= max_interval:
                await message.reply("❌ Minimum time must be less than maximum time.")
                return
                
            channels = await bot.get_user_channels(user_id)
            keyboard = []
            
            for channel in channels:
                if channel.get("target_channel"):
                    try:
                        source_info = await app.get_chat(channel["source_channel"])
                        target_info = await app.get_chat(channel["target_channel"])
                        button_text = f"{source_info.title[:15]}→{target_info.title[:15]}"
                        callback_data = f"start_random_{channel['source_channel']}_{channel['target_channel']}_{min_interval}_{max_interval}"
                        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
                    except:
                        continue
                        
            if keyboard:
                await message.reply(f"🎲 **Random Schedule:** {min_time_str} to {max_time_str}\n\nSelect channel pair:", reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await message.reply("❌ No channel pairs found.")
        else:
            await message.reply("❌ Invalid format. Use /help for examples.")
            
        logger.info(f"Schedule command processed for user {user_id}")
    except Exception as e:
        logger.error(f"Error in /schedule command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

@app.on_message(filters.command("stop"))
async def stop_command(client, message: Message):
    try:
        logger.info(f"/stop command triggered by user {message.from_user.id}")
        await bot.update_user_activity(message.from_user.id)
        
        user_id = message.from_user.id
        
        active_schedules = await schedules_collection.find({
            "user_id": user_id,
            "is_active": True
        }).to_list(None)
        
        if not active_schedules:
            await message.reply("❌ No active forwarding tasks found.")
            return
            
        keyboard = []
        for schedule in active_schedules:
            try:
                source_info = await app.get_chat(schedule["source_channel"])
                target_info = await app.get_chat(schedule["target_channel"])
                button_text = f"Stop {source_info.title[:15]}→{target_info.title[:15]}"
                callback_data = f"stop_{schedule['source_channel']}_{schedule['target_channel']}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
            except:
                continue
                
        keyboard.append([InlineKeyboardButton("🛑 Stop All", callback_data="stop_all")])
        
        await message.reply("🛑 **Stop Forwarding**\n\nSelect which task to stop:", reply_markup=InlineKeyboardMarkup(keyboard))
        logger.info(f"Stop options sent to user {user_id}")
    except Exception as e:
        logger.error(f"Error in /stop command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

@app.on_message(filters.command("status"))
async def status_command(client, message: Message):
    try:
        logger.info(f"/status command triggered by user {message.from_user.id}")
        await bot.update_user_activity(message.from_user.id)
        
        user_id = message.from_user.id
        
        active_schedules = await schedules_collection.find({
            "user_id": user_id,
            "is_active": True
        }).to_list(None)
        
        if not active_schedules:
            await message.reply("📊 **Status:** No active forwarding tasks.")
            return
            
        status_text = "📊 **Active Forwarding Tasks:**\n\n"
        
        for i, schedule in enumerate(active_schedules, 1):
            try:
                source_info = await app.get_chat(schedule["source_channel"])
                target_info = await app.get_chat(schedule["target_channel"])
                
                mode = schedule["mode"].title()
                if mode == "Fixed":
                    interval = schedule.get("interval", 0)
                    hours = interval // 3600
                    minutes = (interval % 3600) // 60
                    time_info = f"Every {hours}h {minutes}m" if hours else f"Every {minutes}m"
                else:
                    min_int = schedule.get("min_interval", 0)
                    max_int = schedule.get("max_interval", 0)
                    min_h = min_int // 3600
                    max_h = max_int // 3600
                    time_info = f"Random {min_h}h-{max_h}h"
                    
                status_text += f"**{i}.** {source_info.title} → {target_info.title}\n"
                status_text += f"   📅 Mode: {mode}\n"
                status_text += f"   ⏰ Schedule: {time_info}\n"
                status_text += f"   📅 Started: {schedule['created_date'].strftime('%Y-%m-%d %H:%M')}\n\n"
            except:
                continue
                
        await message.reply(status_text)
        logger.info(f"Status sent to user {user_id}")
    except Exception as e:
        logger.error(f"Error in /status command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

@app.on_message(filters.command("channels"))
async def channels_command(client, message: Message):
    try:
        logger.info(f"/channels command triggered by user {message.from_user.id}")
        await bot.update_user_activity(message.from_user.id)
        
        user_id = message.from_user.id
        
        channels = await bot.get_user_channels(user_id)
        
        if not channels:
            await message.reply("📋 **Your Channels:** None configured.\n\nUse /addchannel to add channels.")
            return
            
        channels_text = "📋 **Your Channel Pairs:**\n\n"
        
        for i, channel in enumerate(channels, 1):
            try:
                source_info = await app.get_chat(channel["source_channel"])
                if channel.get("target_channel"):
                    target_info = await app.get_chat(channel["target_channel"])
                    channels_text += f"**{i}.** {source_info.title} → {target_info.title}\n"
                    channels_text += f"   📊 Status: {'🟢 Active' if channel.get('is_active', True) else '🔴 Inactive'}\n\n"
                else:
                    channels_text += f"**{i}.** {source_info.title} → ❌ Target not set\n\n"
            except:
                channels_text += f"**{i}.** ❌ Invalid channel pair\n\n"
                
        await message.reply(channels_text)
        logger.info(f"Channels list sent to user {user_id}")
    except Exception as e:
        logger.error(f"Error in /channels command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

# Admin commands
@app.on_message(filters.command("broadcast") & filters.user(ADMIN_IDS))
async def broadcast_command(client, message: Message):
    try:
        logger.info(f"/broadcast command triggered by admin {message.from_user.id}")
        
        if not message.reply_to_message:
            await message.reply("❌ Reply to a message to broadcast it.")
            return
            
        broadcast_text = message.reply_to_message.text or message.reply_to_message.caption
        if not broadcast_text:
            await message.reply("❌ Message must contain text.")
            return
            
        confirm_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm Broadcast", callback_data="confirm_broadcast")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")]
        ])
        
        app.pending_broadcast = broadcast_text
        
        await message.reply(f"📢 **Confirm Broadcast**\n\nMessage: {broadcast_text[:100]}...\n\nProceed?", reply_markup=confirm_keyboard)
        logger.info(f"Broadcast confirmation sent to admin {message.from_user.id}")
    except Exception as e:
        logger.error(f"Error in /broadcast command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

@app.on_message(filters.command("users") & filters.user(ADMIN_IDS))
async def users_command(client, message: Message):
    try:
        logger.info(f"/users command triggered by admin {message.from_user.id}")
        
        total_users = await users_collection.count_documents({})
        active_users = await users_collection.count_documents({"is_active": True})
        
        week_ago = get_utc_now() - timedelta(days=7)
        recent_users = await users_collection.count_documents({"joined_date": {"$gte": week_ago}})
        
        stats_text = f"""📊 **User Statistics**

👥 Total Users: {total_users}
🟢 Active Users: {active_users}
📅 New (7 days): {recent_users}
📈 Growth Rate: {(recent_users/max(total_users-recent_users, 1)*100):.1f}%"""
        
        await message.reply(stats_text)
        logger.info(f"User stats sent to admin {message.from_user.id}")
    except Exception as e:
        logger.error(f"Error in /users command: {e}")
        await message.reply("❌ An error occurred. Please try again.")

# Callback handlers
@app.on_callback_query()
async def callback_handler(client, callback_query):
    try:
        data = callback_query.data
        user_id = callback_query.from_user.id
        
        logger.info(f"Callback query received: {data} from user {user_id}")
        
        if data.startswith("start_fixed_"):
            parts = data.split("_")
            source_channel = int(parts[2])
            target_channel = int(parts[3])
            interval = int(parts[4])
            
            success = await bot.start_forwarding(
                user_id, source_channel, target_channel, "fixed", interval=interval
            )
            
            if success:
                hours = interval // 3600
                minutes = (interval % 3600) // 60
                time_str = f"{hours}h {minutes}m" if hours else f"{minutes}m"
                await callback_query.answer("✅ Fixed forwarding started!")
                await callback_query.message.edit_text(f"✅ **Forwarding Started**\n\n⏰ Mode: Fixed every {time_str}\n📊 Status: Active\n\n**Note:** Messages will be forwarded automatically when they arrive in the source channel.")
            else:
                await callback_query.answer("❌ Failed to start forwarding")
                
        elif data.startswith("start_random_"):
            parts = data.split("_")
            source_channel = int(parts[2])
            target_channel = int(parts[3])
            min_interval = int(parts[4])
            max_interval = int(parts[5])
            
            success = await bot.start_forwarding(
                user_id, source_channel, target_channel, "random", 
                min_interval=min_interval, max_interval=max_interval
            )
            
            if success:
                min_h = min_interval // 3600
                max_h = max_interval // 3600
                await callback_query.answer("✅ Random forwarding started!")
                await callback_query.message.edit_text(f"✅ **Forwarding Started**\n\n🎲 Mode: Random {min_h}h-{max_h}h\n📊 Status: Active\n\n**Note:** Messages will be forwarded automatically when they arrive in the source channel.")
            else:
                await callback_query.answer("❌ Failed to start forwarding")
                
        elif data.startswith("stop_"):
            if data == "stop_all":
                active_schedules = await schedules_collection.find({
                    "user_id": user_id,
                    "is_active": True
                }).to_list(None)
                
                for schedule in active_schedules:
                    await bot.stop_forwarding(user_id, schedule["source_channel"], schedule["target_channel"])
                    
                await callback_query.answer("🛑 All forwarding stopped!")
                await callback_query.message.edit_text("🛑 **All forwarding tasks stopped.**")
            else:
                parts = data.split("_")
                source_channel = int(parts[1])
                target_channel = int(parts[2])
                
                await bot.stop_forwarding(user_id, source_channel, target_channel)
                await callback_query.answer("🛑 Forwarding stopped!")
                await callback_query.message.edit_text("🛑 **Forwarding task stopped.**")
                
        elif data == "confirm_broadcast":
            if hasattr(app, 'pending_broadcast'):
                await callback_query.answer("📢 Broadcasting...")
                await bot.broadcast_message(app.pending_broadcast, user_id)
                await callback_query.message.edit_text("✅ **Broadcast sent successfully!**")
                delattr(app, 'pending_broadcast')
            else:
                await callback_query.answer("❌ No pending broadcast")
                
        elif data == "cancel_broadcast":
            if hasattr(app, 'pending_broadcast'):
                delattr(app, 'pending_broadcast')
            await callback_query.answer("❌ Broadcast cancelled")
            await callback_query.message.edit_text("❌ **Broadcast cancelled.**")
            
    except Exception as e:
        logger.error(f"Error in callback handler: {e}")
        await callback_query.answer("❌ An error occurred")

def run_flask():
    """Run Flask app in a separate thread"""
    try:
        logger.info("Starting Flask web server")
        flask_app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)
    except Exception as e:
        logger.error(f"Error running Flask: {e}")

async def main():
    """Main function to run both bot and web server"""
    try:
        logger.info("Initializing bot...")
        await bot.init_db()
        
        # Start Flask in a separate thread
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        
        logger.info(f"🤖 Bot started successfully!")
        logger.info(f"🌐 Web dashboard running on port {PORT}")
        
        await app.start()
        logger.info("Bot is now running and listening for messages...")
        await asyncio.Event().wait()
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
    finally:
        logger.info("Bot shutting down...")
        await app.stop()

if __name__ == "__main__":
    try:
        app.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

