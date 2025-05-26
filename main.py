import asyncio
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters, types
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import os
#from dotenv import load_dotenv
from flask import Flask, render_template, jsonify
import threading
import json
from collections import defaultdict

#load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bot configuration
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
MONGO_URI = os.getenv('MONGO_URI')
ADMIN_IDS = [int(x) for x in os.getenv('ADMIN_IDS', '').split(',') if x]
PORT = int(os.getenv('PORT', 8000))

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

class ForwarderBot:
    def __init__(self):
        self.forwarding_tasks = {}
        self.is_running = {}
        self.stats_cache = {}
        self.last_stats_update = None
        
    async def init_db(self):
        """Initialize database indexes"""
        await users_collection.create_index("user_id", unique=True)
        await channels_collection.create_index([("user_id", 1), ("source_channel", 1)])
        await forwarded_messages_collection.create_index([("source_channel", 1), ("message_id", 1)])
        await analytics_collection.create_index([("date", 1), ("metric", 1)])
        
    async def add_user(self, user_id: int, username: str = None):
        """Add user to database"""
        user_data = {
            "user_id": user_id,
            "username": username,
            "joined_date": datetime.utcnow(),
            "is_active": True,
            "last_activity": datetime.utcnow()
        }
        result = await users_collection.update_one(
            {"user_id": user_id},
            {"$set": user_data},
            upsert=True
        )
        
        # Track new user analytics
        if result.upserted_id:
            await self.track_analytics("new_user", 1)
            
    async def update_user_activity(self, user_id: int):
        """Update user last activity"""
        await users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"last_activity": datetime.utcnow(), "is_active": True}}
        )
        
    async def track_analytics(self, metric: str, value: int = 1):
        """Track analytics data"""
        today = datetime.utcnow().date()
        await analytics_collection.update_one(
            {"date": today, "metric": metric},
            {"$inc": {"value": value}},
            upsert=True
        )
        
    async def get_user_channels(self, user_id: int):
        """Get user's configured channels"""
        return await channels_collection.find({"user_id": user_id}).to_list(None)
        
    async def add_channel_pair(self, user_id: int, source_channel: int, target_channel: int):
        """Add source-target channel pair"""
        channel_data = {
            "user_id": user_id,
            "source_channel": source_channel,
            "target_channel": target_channel,
            "created_date": datetime.utcnow(),
            "is_active": True,
            "last_forwarded_id": 0
        }
        await channels_collection.update_one(
            {"user_id": user_id, "source_channel": source_channel},
            {"$set": channel_data},
            upsert=True
        )
        
    async def parse_time_interval(self, time_str: str) -> int:
        """Parse time string to seconds"""
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
                    for key in multipliers:
                        if time_str[len(current_number):].startswith(key):
                            unit = key
                            break
                    if unit:
                        total_seconds += number * multipliers[unit]
                    current_number = ""
        
        if current_number:
            total_seconds += int(current_number) * 60
            
        return max(total_seconds, 60)
        
    async def get_channel_messages_hash(self, channel_id: int, limit: int = 100) -> set:
        """Get hash of recent messages from target channel for duplicate detection"""
        try:
            message_hashes = set()
            async for message in app.get_chat_history(channel_id, limit=limit):
                if message.text:
                    content_hash = hash(message.text.strip().lower())
                    message_hashes.add(content_hash)
                elif message.caption:
                    content_hash = hash(message.caption.strip().lower())
                    message_hashes.add(content_hash)
            return message_hashes
        except Exception as e:
            logger.error(f"Error getting channel messages: {e}")
            return set()
            
    async def is_duplicate_message(self, message: Message, target_channel_hashes: set) -> bool:
        """Check if message is duplicate"""
        if message.text:
            content_hash = hash(message.text.strip().lower())
        elif message.caption:
            content_hash = hash(message.caption.strip().lower())
        else:
            return False
            
        return content_hash in target_channel_hashes
        
    async def delete_duplicate_from_source(self, source_channel: int, target_channel: int):
        """Delete duplicate messages from source channel"""
        try:
            target_hashes = await self.get_channel_messages_hash(target_channel)
            deleted_count = 0
            
            async for message in app.get_chat_history(source_channel, limit=200):
                if await self.is_duplicate_message(message, target_hashes):
                    try:
                        await message.delete()
                        deleted_count += 1
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f"Error deleting message {message.id}: {e}")
                        
            await self.track_analytics("duplicates_deleted", deleted_count)
            return deleted_count
        except Exception as e:
            logger.error(f"Error in duplicate deletion: {e}")
            return 0
            
    async def forward_single_message(self, source_channel: int, target_channel: int, user_id: int):
        """Forward a single message from source to target"""
        try:
            channel_data = await channels_collection.find_one({
                "user_id": user_id,
                "source_channel": source_channel
            })
            
            last_id = channel_data.get("last_forwarded_id", 0) if channel_data else 0
            
            async for message in app.get_chat_history(source_channel, limit=50):
                if message.id > last_id:
                    try:
                        forwarded = await message.forward(target_channel)
                        
                        await channels_collection.update_one(
                            {"user_id": user_id, "source_channel": source_channel},
                            {"$set": {"last_forwarded_id": message.id}}
                        )
                        
                        await forwarded_messages_collection.insert_one({
                            "user_id": user_id,
                            "source_channel": source_channel,
                            "target_channel": target_channel,
                            "source_message_id": message.id,
                            "target_message_id": forwarded.id,
                            "forwarded_date": datetime.utcnow()
                        })
                        
                        await self.track_analytics("messages_forwarded", 1)
                        return True
                    except Exception as e:
                        logger.error(f"Error forwarding message {message.id}: {e}")
                        continue
                        
            return False
        except Exception as e:
            logger.error(f"Error in forward_single_message: {e}")
            return False
            
    async def fixed_time_forwarder(self, user_id: int, source_channel: int, target_channel: int, interval: int):
        """Forward messages at fixed intervals"""
        task_key = f"{user_id}_{source_channel}_{target_channel}"
        self.is_running[task_key] = True
        
        try:
            while self.is_running.get(task_key, False):
                await self.forward_single_message(source_channel, target_channel, user_id)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass
        finally:
            self.is_running[task_key] = False
            
    async def random_time_forwarder(self, user_id: int, source_channel: int, target_channel: int, min_interval: int, max_interval: int):
        """Forward messages at random intervals"""
        task_key = f"{user_id}_{source_channel}_{target_channel}"
        self.is_running[task_key] = True
        
        try:
            while self.is_running.get(task_key, False):
                await self.forward_single_message(source_channel, target_channel, user_id)
                random_interval = random.randint(min_interval, max_interval)
                await asyncio.sleep(random_interval)
        except asyncio.CancelledError:
            pass
        finally:
            self.is_running[task_key] = False
            
    async def start_forwarding(self, user_id: int, source_channel: int, target_channel: int, mode: str, **kwargs):
        """Start forwarding task"""
        task_key = f"{user_id}_{source_channel}_{target_channel}"
        
        if task_key in self.forwarding_tasks:
            self.forwarding_tasks[task_key].cancel()
            
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
            return False
            
        self.forwarding_tasks[task_key] = task
        
        schedule_data = {
            "user_id": user_id,
            "source_channel": source_channel,
            "target_channel": target_channel,
            "mode": mode,
            "created_date": datetime.utcnow(),
            "is_active": True,
            **kwargs
        }
        await schedules_collection.update_one(
            {"user_id": user_id, "source_channel": source_channel, "target_channel": target_channel},
            {"$set": schedule_data},
            upsert=True
        )
        
        await self.track_analytics("tasks_started", 1)
        return True
        
    async def stop_forwarding(self, user_id: int, source_channel: int, target_channel: int):
        """Stop forwarding task"""
        task_key = f"{user_id}_{source_channel}_{target_channel}"
        
        if task_key in self.forwarding_tasks:
            self.forwarding_tasks[task_key].cancel()
            del self.forwarding_tasks[task_key]
            
        self.is_running[task_key] = False
        
        await schedules_collection.update_one(
            {"user_id": user_id, "source_channel": source_channel, "target_channel": target_channel},
            {"$set": {"is_active": False}}
        )
        
        await self.track_analytics("tasks_stopped", 1)
        
    async def broadcast_message(self, message_text: str, admin_id: int):
        """Broadcast message to all users"""
        users = await users_collection.find({"is_active": True}).to_list(None)
        success_count = 0
        failed_count = 0
        
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
        
    async def get_dashboard_stats(self):
        """Get comprehensive dashboard statistics"""
        if self.last_stats_update and (datetime.utcnow() - self.last_stats_update).seconds < 300:
            return self.stats_cache
            
        try:
            # Basic user stats
            total_users = await users_collection.count_documents({})
            
            # Active users (last 30 days)
            thirty_days_ago = datetime.utcnow() - timedelta(days=30)
            active_users = await users_collection.count_documents({
                "last_activity": {"$gte": thirty_days_ago}
            })
            
            # New users (last 7 days)
            week_ago = datetime.utcnow() - timedelta(days=7)
            new_users = await users_collection.count_documents({
                "joined_date": {"$gte": week_ago}
            })
            
            # Calculate growth rate
            prev_week = datetime.utcnow() - timedelta(days=14)
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
            today = datetime.utcnow().date()
            today_start = datetime.combine(today, datetime.min.time())
            today_forwarded = await forwarded_messages_collection.count_documents({
                "forwarded_date": {"$gte": today_start}
            })
            
            # Channel pairs
            total_channels = await channels_collection.count_documents({"target_channel": {"$ne": None}})
            
            # Recent activity (last 24 hours)
            yesterday = datetime.utcnow() - timedelta(days=1)
            recent_activity = await analytics_collection.find({
                "date": {"$gte": yesterday.date()}
            }).to_list(None)
            
            # Get daily stats for chart (last 7 days)
            daily_stats = []
            for i in range(7):
                date = (datetime.utcnow() - timedelta(days=i)).date()
                day_users = await users_collection.count_documents({
                    "joined_date": {
                        "$gte": datetime.combine(date, datetime.min.time()),
                        "$lt": datetime.combine(date + timedelta(days=1), datetime.min.time())
                    }
                })
                day_forwards = await forwarded_messages_collection.count_documents({
                    "forwarded_date": {
                        "$gte": datetime.combine(date, datetime.min.time()),
                        "$lt": datetime.combine(date + timedelta(days=1), datetime.min.time())
                    }
                })
                daily_stats.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "new_users": day_users,
                    "messages_forwarded": day_forwards
                })
                
            # Current running tasks details
            current_tasks = []
            active_schedules = await schedules_collection.find({"is_active": True}).to_list(None)
            
            for schedule in active_schedules:
                try:
                    user_info = await users_collection.find_one({"user_id": schedule["user_id"]})
                    source_info = await app.get_chat(schedule["source_channel"])
                    target_info = await app.get_chat(schedule["target_channel"])
                    
                    task_info = {
                        "user_id": schedule["user_id"],
                        "username": user_info.get("username", "Unknown") if user_info else "Unknown",
                        "source_channel": source_info.title,
                        "target_channel": target_info.title,
                        "mode": schedule["mode"],
                        "created_date": schedule["created_date"].strftime("%Y-%m-%d %H:%M"),
                        "status": "Running" if f"{schedule['user_id']}_{schedule['source_channel']}_{schedule['target_channel']}" in self.is_running else "Stopped"
                    }
                    
                    if schedule["mode"] == "fixed":
                        interval = schedule.get("interval", 0)
                        hours = interval // 3600
                        minutes = (interval % 3600) // 60
                        task_info["schedule"] = f"Every {hours}h {minutes}m" if hours else f"Every {minutes}m"
                    else:
                        min_int = schedule.get("min_interval", 0)
                        max_int = schedule.get("max_interval", 0)
                        min_h = min_int // 3600
                        max_h = max_int // 3600
                        task_info["schedule"] = f"Random {min_h}h-{max_h}h"
                        
                    current_tasks.append(task_info)
                except Exception as e:
                    logger.error(f"Error getting task info: {e}")
                    continue
            
            self.stats_cache = {
                "total_users": total_users,
                "active_users": active_users,
                "new_users": new_users,
                "growth_rate": round(growth_rate, 2),
                "active_tasks": active_tasks,
                "total_forwarded": total_forwarded,
                "today_forwarded": today_forwarded,
                "total_channels": total_channels,
                "daily_stats": list(reversed(daily_stats)),
                "current_tasks": current_tasks,
                "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            }
            
            self.last_stats_update = datetime.utcnow()
            return self.stats_cache
            
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {e}")
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
                "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            }

# Initialize bot instance
bot = ForwarderBot()

# Flask Routes
@flask_app.route('/')
def dashboard():
    """Main dashboard page"""
    return '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Telegram Forwarder Bot Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .header {
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }
        
        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
        }
        
        .stat-card h3 {
            color: #666;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        
        .stat-card .value {
            font-size: 2.5rem;
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }
        
        .stat-card .change {
            font-size: 0.9rem;
            color: #28a745;
        }
        
        .chart-container {
            background: white;
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        
        .tasks-container {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        
        .task-item {
            border-bottom: 1px solid #eee;
            padding: 15px 0;
            display: grid;
            grid-template-columns: 1fr 1fr 1fr auto;
            gap: 15px;
            align-items: center;
        }
        
        .task-item:last-child {
            border-bottom: none;
        }
        
        .status-badge {
            padding: 5px 10px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: bold;
        }
        
        .status-running {
            background: #d4edda;
            color: #155724;
        }
        
        .status-stopped {
            background: #f8d7da;
            color: #721c24;
        }
        
        .refresh-btn {
            position: fixed;
            bottom: 30px;
            right: 30px;
            background: #007bff;
            color: white;
            border: none;
            border-radius: 50px;
            padding: 15px 25px;
            cursor: pointer;
            box-shadow: 0 5px 15px rgba(0,123,255,0.3);
            transition: all 0.3s ease;
        }
        
        .refresh-btn:hover {
            background: #0056b3;
            transform: scale(1.05);
        }
        
        .loading {
            text-align: center;
            color: #666;
            padding: 50px;
        }
        
        @media (max-width: 768px) {
            .stats-grid {
                grid-template-columns: 1fr;
            }
            
            .task-item {
                grid-template-columns: 1fr;
                text-align: center;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🤖 Telegram Forwarder Bot</h1>
            <p>Real-time Dashboard & Analytics</p>
        </div>
        
        <div id="stats-container">
            <div class="loading">
                <h3>Loading dashboard...</h3>
            </div>
        </div>
    </div>
    
    <button class="refresh-btn" onclick="loadDashboard()">
        🔄 Refresh
    </button>
    
    <script>
        let chartInstance = null;
        
        async function loadDashboard() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                document.getElementById('stats-container').innerHTML = `
                    <div class="stats-grid">
                        <div class="stat-card">
                            <h3>Total Users</h3>
                            <div class="value">${data.total_users.toLocaleString()}</div>
                        </div>
                        
                        <div class="stat-card">
                            <h3>Active Users (30d)</h3>
                            <div class="value">${data.active_users.toLocaleString()}</div>
                        </div>
                        
                        <div class="stat-card">
                            <h3>New Users (7d)</h3>
                            <div class="value">${data.new_users.toLocaleString()}</div>
                            <div class="change">Growth: ${data.growth_rate}%</div>
                        </div>
                        
                        <div class="stat-card">
                            <h3>Active Tasks</h3>
                            <div class="value">${data.active_tasks}</div>
                        </div>
                        
                        <div class="stat-card">
                            <h3>Total Forwarded</h3>
                            <div class="value">${data.total_forwarded.toLocaleString()}</div>
                        </div>
                        
                        <div class="stat-card">
                            <h3>Today's Forwards</h3>
                            <div class="value">${data.today_forwarded.toLocaleString()}</div>
                        </div>
                        
                        <div class="stat-card">
                            <h3>Channel Pairs</h3>
                            <div class="value">${data.total_channels}</div>
                        </div>
                        
                        <div class="stat-card">
                            <h3>Last Updated</h3>
                            <div class="value" style="font-size: 1rem;">${data.last_updated}</div>
                        </div>
                    </div>
                    
                    <div class="chart-container">
                        <h3 style="margin-bottom: 20px;">📊 Daily Activity (Last 7 Days)</h3>
                        <canvas id="activityChart" width="400" height="200"></canvas>
                    </div>
                    
                    <div class="tasks-container">
                        <h3 style="margin-bottom: 20px;">🔄 Current Active Tasks</h3>
                        ${data.current_tasks.length === 0 ? 
                            '<p style="text-align: center; color: #666; padding: 20px;">No active tasks</p>' :
                            data.current_tasks.map(task => `
                                <div class="task-item">
                                    <div>
                                        <strong>@${task.username}</strong><br>
                                        <small>User ID: ${task.user_id}</small>
                                    </div>
                                    <div>
                                        <strong>${task.source_channel}</strong><br>
                                        ↓<br>
                                        <strong>${task.target_channel}</strong>
                                    </div>
                                    <div>
                                        <strong>${task.mode.toUpperCase()}</strong><br>
                                        <small>${task.schedule}</small><br>
                                        <small>Started: ${task.created_date}</small>
                                    </div>
                                    <div>
                                        <span class="status-badge ${task.status === 'Running' ? 'status-running' : 'status-stopped'}">
                                            ${task.status}
                                        </span>
                                    </div>
                                </div>
                            `).join('')
                        }
                    </div>
                `;
                
                // Create chart
                const ctx = document.getElementById('activityChart').getContext('2d');
                
                if (chartInstance) {
                    chartInstance.destroy();
                }
                
                chartInstance = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: data.daily_stats.map(d => d.date),
                        datasets: [
                            {
                                label: 'New Users',
                                data: data.daily_stats.map(d => d.new_users),
                                borderColor: '#007bff',
                                backgroundColor: 'rgba(0, 123, 255, 0.1)',
                                tension: 0.4
                            },
                            {
                                label: 'Messages Forwarded',
                                data: data.daily_stats.map(d => d.messages_forwarded),
                                borderColor: '#28a745',
                                backgroundColor: 'rgba(40, 167, 69, 0.1)',
                                tension: 0.4
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        plugins: {
                            legend: {
                                position: 'top',
                            }
                        },
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                });
                
            } catch (error) {
                console.error('Error loading dashboard:', error);
                document.getElementById('stats-container').innerHTML = `
                    <div class="loading">
                        <h3 style="color: #dc3545;">Error loading dashboard</h3>
                        <p>Please try refreshing the page</p>
                    </div>
                `;
            }
        }
        
        // Load dashboard on page load
        loadDashboard();
        
        // Auto-refresh every 5 minutes
        setInterval(loadDashboard, 300000);
    </script>
</body>
</html>
    '''

@flask_app.route('/api/stats')
def api_stats():
    """API endpoint for dashboard statistics"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stats = loop.run_until_complete(bot.get_dashboard_stats())
        loop.close()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error in API stats: {e}")
        return jsonify({"error": "Failed to fetch stats"}), 500

@flask_app.route('/health')
def health_check():
    """Health check endpoint for deployment platforms"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

# Bot handlers (keeping all previous handlers)
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    await bot.add_user(user_id, username)
    await bot.update_user_activity(user_id)
    
    welcome_text = f"""
🤖 **Welcome to Advanced Forwarder Bot!**

**Features:**
📤 Forward messages between channels
⏰ Fixed time scheduling
🎲 Random time intervals  
🗑️ Duplicate message deletion
📢 Broadcast messages (Admin only)
📊 Web Dashboard: [View Stats](https://your-app-name.koyeb.app)

**Commands:**
/help - Show all commands
/addchannel - Add source/target channels
/schedule - Set up forwarding schedule
/stop - Stop forwarding
/status - Check forwarding status
/cleanup - Remove duplicates
/channels - List your channels

**Setup:**
1. Add me to your source and target channels as admin
2. Use /addchannel to configure channels
3. Use /schedule to start forwarding

Let's get started! 🚀
"""
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📚 Help", callback_data="help")],
        [InlineKeyboardButton("⚙️ Add Channels", callback_data="add_channels")],
        [InlineKeyboardButton("📊 Dashboard", url="https://your-app-name.koyeb.app")]
    ])
    
    await message.reply(welcome_text, reply_markup=keyboard)

# Keep all other bot handlers from previous code...
# (All the previous handlers remain the same)

# Add activity tracking to existing handlers
async def track_user_activity(message):
    """Track user activity for analytics"""
    await bot.update_user_activity(message.from_user.id)

# Wrap existing handlers with activity tracking
original_handlers = [
    start_command, help_command, add_channel_command, handle_forwarded_message,
    schedule_command, stop_command, status_command, cleanup_command, channels_command
]

for handler in original_handlers:
    handler.add_handler(track_user_activity)

def run_flask():
    """Run Flask app in a separate thread"""
    flask_app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)

async def main():
    """Main function to run both bot and web server"""
    await bot.init_db()
    
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    print(f"🤖 Bot started successfully!")
    print(f"🌐 Web dashboard running on port {PORT}")
    
    await app.start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    app.run(main())
