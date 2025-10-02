#  ========================================================================================
#  =====            COMPLETE INSTAGRAM TELEGRAM BOT (v11 - UNABRIDGED)                =====
#  ========================================================================================
#  This is the definitive, unabridged script with all features, including the yt-dlp
#  fallback, written in a clear and explicit style with no unnecessary shortening.
#  ========================================================================================

import os
import threading
import http.server
import socketserver
import time
import asyncio
import logging
import subprocess
from datetime import datetime, timedelta
from pymongo import MongoClient
from urllib.parse import quote_plus
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ConversationHandler,
    MessageHandler, filters, ContextTypes, CallbackQueryHandler
)
from telegram.error import TelegramError
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, PrivateError, ChallengeRequired
from pydantic import ValidationError

# --- Basic Setup and Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Environment Variables ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID")

if not all([TELEGRAM_BOT_TOKEN, MONGO_URI, ADMIN_USER_ID]):
    raise ValueError("One or more essential environment variables are missing!")

# ========================================================================================
# =====                                DATABASE LOGIC                                =====
# ========================================================================================

client = MongoClient(MONGO_URI)
db = client.instagram_bot
users_collection = db.users
accounts_collection = db.instagram_accounts
seen_reels_collection = db.seen_reels
topics_collection = db.topics

def db_add_user(user_id):
    users_collection.update_one({"user_id": user_id}, {"$setOnInsert": {"user_id": user_id}}, upsert=True)

def db_set_log_channel(user_id, channel_id):
    users_collection.update_one({"user_id": user_id}, {"$set": {"log_channel": channel_id}})

def db_get_log_channel(user_id):
    user = users_collection.find_one({"user_id": int(user_id)})
    return user.get("log_channel") if user else None

def db_get_all_accounts():
    return list(accounts_collection.find({}))

def db_add_or_update_account(user_id, ig_username, ig_session_id):
    accounts_collection.update_one(
        {"owner_id": user_id, "ig_username": ig_username},
        {"$set": {"ig_session_id": ig_session_id, "last_check": datetime.now() - timedelta(hours=1), "interval_minutes": 60, "is_active": True}},
        upsert=True
    )

def db_get_user_accounts(user_id):
    return list(accounts_collection.find({"owner_id": user_id}))

def db_get_account(ig_username):
    return accounts_collection.find_one({"ig_username": ig_username})

def db_remove_account(user_id, ig_username):
    accounts_collection.delete_one({"owner_id": user_id, "ig_username": ig_username})
    seen_reels_collection.delete_many({"ig_username": ig_username})
    topics_collection.delete_many({"ig_username": ig_username})

def db_set_target_chat(ig_username, chat_id):
    accounts_collection.update_one({"ig_username": ig_username}, {"$set": {"target_chat_id": chat_id}})

def db_set_topic_mode(ig_username, topic_mode_enabled):
    accounts_collection.update_one({"ig_username": ig_username}, {"$set": {"topic_mode": topic_mode_enabled}})

def db_set_periodic_interval(ig_username, interval_minutes):
    accounts_collection.update_one({"ig_username": ig_username}, {"$set": {"interval_minutes": interval_minutes}})

def db_update_last_check(ig_username):
    accounts_collection.update_one({"ig_username": ig_username}, {"$set": {"last_check": datetime.now()}})

def db_add_seen_reel(ig_username, reel_pk):
    seen_reels_collection.insert_one({"ig_username": ig_username, "reel_pk": reel_pk})

def db_has_seen_reel(ig_username, reel_pk):
    return seen_reels_collection.count_documents({"ig_username": ig_username, "reel_pk": reel_pk}) > 0

def db_clear_seen_reels(ig_username):
    seen_reels_collection.delete_many({"ig_username": ig_username})

def db_get_or_create_topic(chat_id, topic_name):
    topic = topics_collection.find_one({"chat_id": chat_id, "name": topic_name})
    return topic.get("topic_id") if topic else None

def db_save_topic(chat_id, topic_name, topic_id, ig_username):
    topics_collection.update_one(
        {"chat_id": chat_id, "name": topic_name},
        {"$set": {"topic_id": topic_id, "ig_username": ig_username}},
        upsert=True
    )

# ========================================================================================
# =====                             INSTAGRAM CLIENT LOGIC                           =====
# ========================================================================================

class InstagramClient:
    def __init__(self, username, session_id):
        self.username = username
        self.client = Client()
        session_path = f"/tmp/{self.username}_session.json"
        try:
            if os.path.exists(session_path):
                self.client.load_settings(session_path)
            self.client.login_by_sessionid(session_id)
            self.client.user_info_by_username(self.username)
            self.client.dump_settings(session_path)
            logger.info(f"IG client for {self.username} initialized.")
        except Exception as e:
            logger.error(f"Failed to init IG client for {self.username}: {e}")
            raise

    def get_new_reels_from_dms(self):
        new_reels = []
        threads = self.client.direct_threads(amount=20)
        for thread in threads:
            messages = self.client.direct_messages(thread.id, amount=20)
            for message in messages:
                try:
                    sender_username = "Unknown"
                    if hasattr(message, 'user') and message.user:
                        sender_username = message.user.username
                    
                    if message.item_type == "clip":
                        clip = message.clip
                        if not db_has_seen_reel(self.username, clip.pk):
                            new_reels.append({
                                "reel_pk": clip.pk, "ig_chat_name": thread.thread_title, "from_user": sender_username, 
                                "clip_obj": clip, "fallback": False
                            })
                except ValidationError as e:
                    logger.warning(f"Validation error for a reel in chat '{thread.thread_title}'. Attempting fallback. Error: {e}")
                    if hasattr(message, 'clip') and hasattr(message.clip, 'code') and message.clip.code:
                        reel_url = f"https://www.instagram.com/reel/{message.clip.code}/"
                        if not db_has_seen_reel(self.username, reel_url):
                             new_reels.append({
                                 "reel_pk": reel_url, "ig_chat_name": thread.thread_title, "from_user": sender_username, 
                                 "clip_obj": None, "fallback": True, "url": reel_url
                            })
                    continue
        return new_reels

    def download_reel(self, reel_pk):
        download_path = "/tmp/downloads"
        os.makedirs(download_path, exist_ok=True)
        return self.client.video_download(reel_pk, folder=download_path)

def download_reel_with_ytdlp(reel_url):
    download_path = "/tmp/downloads"
    os.makedirs(download_path, exist_ok=True)
    
    command = ["yt-dlp", "--quiet", "-o", f"{download_path}/%(id)s.%(ext)s", reel_url]
    subprocess.run(command, check=True)
    
    file_id = reel_url.split("/reel/")[1].strip('/')
    for file in os.listdir(download_path):
        if file_id in file:
            return os.path.join(download_path, file)
    raise FileNotFoundError(f"yt-dlp downloaded a file for {file_id}, but it could not be found.")


# ========================================================================================
# =====                             UPLOAD PROGRESS LOGIC                            =====
# ========================================================================================

class ProgressManager:
    def __init__(self, bot: Bot, chat_id: int, message_id: int, total_size: int, filename: str):
        self.bot, self.chat_id, self.message_id = bot, chat_id, message_id
        self.total_size, self.filename = total_size, filename
        self.uploaded_size, self.start_time, self.last_update_time = 0, time.time(), 0

    async def progress_callback(self, current, total):
        self.uploaded_size = current
        now = time.time()
        if now - self.last_update_time < 2: return
        self.last_update_time = now
        elapsed_time = now - self.start_time
        if elapsed_time == 0: return
        speed_mbps = (self.uploaded_size / elapsed_time) / (1024 * 1024)
        percentage = (self.uploaded_size / self.total_size) * 100
        eta_seconds = ((self.total_size - self.uploaded_size) / (self.uploaded_size / elapsed_time)) if self.uploaded_size > 0 else 0
        eta = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s" if eta_seconds > 0 else "..."
        bar = '■' * int(10 * self.uploaded_size / self.total_size) + '□' * (10 - int(10 * self.uploaded_size / self.total_size))
        text = (
            f"<b>Uploading Reel...</b>\n<code>{self.filename}</code>\n[{bar}] {percentage:.1f}%\n"
            f"<code>{self.uploaded_size/1024/1024:.2f} / {self.total_size/1024/1024:.2f} MB</code>\n"
            f"Speed: <code>{speed_mbps:.2f} MB/s</code> | ETA: <code>{eta}</code>"
        )
        try:
            await self.bot.edit_message_text(text, chat_id=self.chat_id, message_id=self.message_id, parse_mode='HTML')
        except TelegramError:
            pass

async def upload_video_with_progress(bot: Bot, chat_id: int, video_path: str, caption: str, message_thread_id: int = None):
    filename = os.path.basename(str(video_path))
    total_size = os.path.getsize(video_path)
    status_message = await bot.send_message(chat_id, f"Preparing to upload: {filename}", message_thread_id=message_thread_id)
    progress = ProgressManager(bot, chat_id, status_message.message_id, total_size, filename)
    try:
        with open(video_path, 'rb') as video_file:
            await bot.send_video(
                chat_id=chat_id, video=video_file, caption=caption, parse_mode='HTML',
                message_thread_id=message_thread_id, write_timeout=120,
                progress=progress.progress_callback
            )
        await bot.delete_message(chat_id, status_message.message_id)
    except Exception as e:
        await bot.edit_message_text(f"❌ Upload failed for {filename}.\n<b>Reason:</b> {e}", chat_id, status_message.message_id, parse_mode='HTML')
        raise

# ========================================================================================
# =====                            TELEGRAM HANDLERS LOGIC                           =====
# ========================================================================================

(AWAIT_SESSION_ID, AWAIT_TARGET_CHAT_ID, AWAIT_INTERVAL) = range(3)

async def log_to_channel(bot: Bot, user_id, message: str, forward_error_to: int = None):
    log_channel_id = db_get_log_channel(user_id)
    if log_channel_id:
        try:
            await bot.send_message(chat_id=log_channel_id, text=message)
        except TelegramError as e:
            logger.error(f"Failed to send log to {log_channel_id}: {e}")
            if forward_error_to:
                safe_error = str(e).replace("<", "&lt;").replace(">", "&gt;")
                await bot.send_message(
                    chat_id=forward_error_to,
                    text=f"⚠️ <b>Log Channel Error!</b>\nTo <code>{log_channel_id}</code>: {safe_error}",
                    parse_mode='HTML'
                )

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_add_user(user.id)
    await update.message.reply_html(
        f"👋 <b>Hi {user.mention_html()}!</b>\n\nI am your Instagram DM Reels Bot.\n\n"
        "<b>Admin Commands:</b>\n"
        "<code>/addaccount</code> - Link a new Instagram account.\n"
        "<code>/myaccounts</code> - View and manage your accounts.\n"
        "<code>/forcecheck &lt;username&gt;</code> - Force an immediate check.\n"
        "<code>/checkchat &lt;ID&gt;</code> - Check permissions for a chat.\n"
        "<code>/logc &lt;ID&gt;</code> - Set a channel for bot logs.\n"
        "<code>/kill</code> - Gracefully shut down the bot.\n\n"
        "<b>Public Commands:</b>\n"
        "<code>/ping</code> - Check if the bot is alive.\n"
        "<code>/help</code> - Show this message."
    )

async def kill_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text("Shutting down bot gracefully...")
    logger.info("Shutdown command received. Terminating application.")
    asyncio.create_task(context.application.shutdown())

async def add_account_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Please send the `sessionid` cookie from your Instagram account.\n\nSend /cancel to stop.")
    return AWAIT_SESSION_ID

async def add_account_get_session_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_id = update.message.text
    user_id = update.effective_user.id
    try:
        await update.message.reply_text("Validating session and logging in...")
        temp_client = Client()
        temp_client.login_by_sessionid(session_id)
        ig_username = temp_client.user_info(temp_client.user_id).username
        db_add_or_update_account(user_id, ig_username, session_id)
        await update.message.reply_html(f"✅ Success! Instagram account <b>{ig_username}</b> linked. Use <code>/myaccounts</code> to configure it.")
        return ConversationHandler.END
    except Exception as e:
        await update.message.reply_html(f"❌ Login failed: {e}.\n\nPlease check your session ID, or /cancel.")
        return AWAIT_SESSION_ID

async def my_accounts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    accounts = db_get_user_accounts(update.effective_user.id)
    if not accounts:
        await update.message.reply_text("You haven't linked any Instagram accounts. Use /addaccount.")
        return
    keyboard = [[InlineKeyboardButton(a['ig_username'], callback_data=f"manage_{a['ig_username']}")] for a in accounts]
    text = "Select an Instagram account to manage:"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def manage_account_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ig_username = query.data.split("_")[1]
    account = db_get_account(ig_username)
    if not account:
        await query.edit_message_text("This account no longer exists.")
        return
    text = (
        f"<b>Managing: {ig_username}</b>\n\n"
        f"Target Chat: <code>{account.get('target_chat_id', 'Not Set')}</code>\n"
        f"Check Interval: <code>{account.get('interval_minutes', 60)}</code> mins\n"
        f"Topic Mode: {'✅ Enabled' if account.get('topic_mode') else '❌ Disabled'}"
    )
    keyboard = [
        [InlineKeyboardButton("🎯 Set Target Chat", callback_data=f"settarget_{ig_username}")],
        [InlineKeyboardButton("⏰ Set Interval", callback_data=f"setinterval_{ig_username}")],
        [InlineKeyboardButton("📂 Toggle Topic Mode", callback_data=f"toggletopic_{ig_username}")],
        [InlineKeyboardButton("🗑️ Clear Seen Data", callback_data=f"cleardata_{ig_username}")],
        [InlineKeyboardButton("❌ Remove Account", callback_data=f"remove_{ig_username}")],
        [InlineKeyboardButton("⬅️ Back to Accounts", callback_data="myaccounts")]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong! 🏓")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Bot is running.")

async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text("Restarting...")

async def log_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /logc <ID>")
        return
    try:
        db_set_log_channel(int(ADMIN_USER_ID), int(context.args[0]))
        await update.message.reply_html(f"✅ Log channel set to <code>{context.args[0]}</code>.")
    except (IndexError, ValueError):
        await update.message.reply_text("Invalid ID.")

async def test_log_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text("Sending test message...")
    if await log_to_channel(context.bot, int(ADMIN_USER_ID), "✅ Test message.", forward_error_to=update.effective_chat.id):
        await update.message.reply_text("Test message sent!")

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action, ig_username = query.data.split("_", 1)
    context.user_data['ig_username_to_manage'] = ig_username

    if action == "settarget":
        await query.message.reply_html(f"Please send the target chat ID for <b>{ig_username}</b>.")
        return AWAIT_TARGET_CHAT_ID
    elif action == "setinterval":
        await query.message.reply_html(f"Please send the interval in minutes for <b>{ig_username}</b>.")
        return AWAIT_INTERVAL
    elif action == "toggletopic":
        acc = db_get_account(ig_username)
        new_mode = not acc.get('topic_mode', False)
        db_set_topic_mode(ig_username, new_mode)
        await query.message.reply_text(f"Topic mode for {ig_username} is now {'enabled' if new_mode else 'disabled'}.")
        query.data = f"manage_{ig_username}"
        await manage_account_menu(update, context)
    elif action == "cleardata":
        db_clear_seen_reels(ig_username)
        await query.message.reply_html(f"✅ Seen data for <b>{ig_username}</b> cleared.")
        query.data = f"manage_{ig_username}"
        await manage_account_menu(update, context)
    elif action == "remove":
        kbd = [[InlineKeyboardButton("YES, REMOVE", callback_data=f"confirmremove_{ig_username}"), InlineKeyboardButton("NO", callback_data=f"manage_{ig_username}")]]
        await query.edit_message_text(f"⚠️ Remove <b>{ig_username}</b>?", reply_markup=InlineKeyboardMarkup(kbd), parse_mode='HTML')
    elif action == "confirmremove":
        db_remove_account(update.effective_user.id, ig_username)
        await query.edit_message_text(f"✅ Account <b>{ig_username}</b> has been removed.")

async def get_target_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ig_username = context.user_data.get('ig_username_to_manage')
    if not ig_username: return ConversationHandler.END
    try:
        db_set_target_chat(ig_username, int(update.message.text))
        await update.message.reply_html(f"✅ Target chat for <b>{ig_username}</b> set.")
    except ValueError:
        await update.message.reply_text("Invalid ID.")
    del context.user_data['ig_username_to_manage']
    return ConversationHandler.END

async def get_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ig_username = context.user_data.get('ig_username_to_manage')
    if not ig_username: return ConversationHandler.END
    try:
        interval = int(update.message.text)
        if interval < 5:
            await update.message.reply_text("Min interval is 5 mins.")
            return AWAIT_INTERVAL
        db_set_periodic_interval(ig_username, interval)
        await update.message.reply_html(f"✅ Interval for <b>{ig_username}</b> set to {interval} mins.")
    except ValueError:
        await update.message.reply_text("Invalid number.")
    del context.user_data['ig_username_to_manage']
    return ConversationHandler.END

async def check_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /checkchat <chat_id>")
        return
    try:
        chat_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Error: Chat ID must be a number.")
        return
    
    preliminary_message = await update.message.reply_text(f"Checking permissions for chat ID: {chat_id}...")
    
    try:
        chat = await context.bot.get_chat(chat_id)
        me = await context.bot.get_me()
        member = await chat.get_member(me.id)
        
        perms = [f"<b>Chat Name:</b> {chat.title}", f"<b>Chat Type:</b> {chat.type}", f"<b>Is Forum:</b> {'✅ Yes' if chat.is_forum else '❌ No'}"]
        perms.append(f"\n<b>Bot's Status:</b>")
        perms.append(f"{'✅' if member.status in ['administrator', 'creator'] else '❌'} Is an Administrator (`{member.status}`)")

        perms.append(f"\n<b>Live Action Tests:</b>")
        try:
            test_msg = await context.bot.send_message(chat_id=chat_id, text="...checking permissions...")
            await context.bot.delete_message(chat_id=chat_id, message_id=test_msg.message_id)
            perms.append("✅ Can Send & Delete Messages")
        except Exception as e:
            perms.append(f"❌ Failed to Send/Delete Messages: {e}")
            
        if chat.is_forum:
            try:
                new_topic = await context.bot.create_forum_topic(chat_id=chat_id, name="Bot Permission Test")
                await context.bot.delete_forum_topic(chat_id=chat_id, message_thread_id=new_topic.message_thread_id)
                perms.append("✅ Can Create & Delete Topics")
            except Exception as e:
                perms.append(f"❌ Failed to Manage Topics: {e}")
        
        status_text = f"<b>Permissions Check for <code>{chat_id}</code></b>\n\n" + "\n".join(perms)
        await preliminary_message.edit_text(status_text, parse_mode='HTML')
        
    except Exception as e:
        await preliminary_message.edit_text(f"Could not check chat.\n<b>Error:</b> {e}", parse_mode='HTML')


# ========================================================================================
# =====                     CORE MONITORING AND UPLOAD LOGIC                         =====
# ========================================================================================

async def check_and_upload_account(bot: Bot, account: dict, owner_id: int):
    ig_username = account.get('ig_username'); session_id = account.get('ig_session_id')
    target_chat_id = account.get('target_chat_id'); topic_mode = account.get('topic_mode', False)
    if not all([ig_username, session_id, target_chat_id]): return
    try:
        await log_to_channel(bot, owner_id, f"🔍 Checking {ig_username}...", forward_error_to=owner_id)
        ig_client = InstagramClient(ig_username, session_id)
        new_reels = ig_client.get_new_reels_from_dms()
        db_update_last_check(ig_username)
        
        if not new_reels:
            await log_to_channel(bot, owner_id, f"✅ No new reels for {ig_username}.", forward_error_to=owner_id)
            return
        
        await log_to_channel(bot, owner_id, f"Found {len(new_reels)} new reel(s) for {ig_username}.", forward_error_to=owner_id)
        
        for reel in new_reels:
            filepath, caption = (None, "")
            if reel.get('fallback'):
                await log_to_channel(bot, owner_id, f"⚠️ Using yt-dlp fallback for a reel from {reel['from_user']}.")
                filepath = download_reel_with_ytdlp(reel['url'])
                caption = (f"<i>Reel from <a href='{reel['url']}'>Instagram</a></i>\n\n"
                           f"<b>Shared by:</b> {reel['from_user']} | <b>In Chat:</b> {reel['ig_chat_name']}")
            else:
                filepath = ig_client.download_reel(reel['reel_pk'])
                clip_user = reel['clip_obj'].user
                caption_text = (reel['clip_obj'].caption_text or "").strip()
                caption = (
                    f"<i>Reel by <a href='https://instagram.com/{clip_user.username}'>{clip_user.full_name or clip_user.username}</a></i>\n\n"
                    f"{caption_text}\n\n"
                    f"<b>Shared by:</b> {reel['from_user']} | <b>In Chat:</b> {reel['ig_chat_name']}"
                )
            
            message_thread_id = None
            if topic_mode:
                try:
                    topic_name = reel['ig_chat_name']
                    topic_id = db_get_or_create_topic(target_chat_id, topic_name)
                    if not topic_id:
                        new_topic = await bot.create_forum_topic(chat_id=target_chat_id, name=topic_name)
                        topic_id = new_topic.message_thread_id
                        db_save_topic(target_chat_id, topic_name, topic_id, ig_username)
                    message_thread_id = topic_id
                except Exception as e:
                    await log_to_channel(bot, owner_id, f"⚠️ Topic Error: {e}", forward_error_to=owner_id)

            await upload_video_with_progress(bot, target_chat_id, filepath, caption, message_thread_id)
            db_add_seen_reel(ig_username, reel['reel_pk'])
            os.remove(filepath)
            await log_to_channel(bot, owner_id, f"✅ Sent reel from {reel['from_user']}.", forward_error_to=owner_id)
        
        await log_to_channel(bot, owner_id, f"🎉 Finished all uploads for {ig_username}.", forward_error_to=owner_id)

    except Exception as e:
        safe_error = str(e).replace("<", "&lt;").replace(">", "&gt;")
        await log_to_channel(bot, owner_id, f"❌ CRITICAL ERROR for {ig_username}: {safe_error}", forward_error_to=owner_id)

async def force_check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /forcecheck <instagram_username>")
        return
    ig_username = context.args[0]
    account = db_get_account(ig_username)
    if not account or account['owner_id'] != update.effective_user.id:
        await update.message.reply_text(f"Account '{ig_username}' not found.")
        return
    await update.message.reply_html(f"Manual check for <b>{ig_username}</b> triggered.")
    asyncio.create_task(check_and_upload_account(context.bot, account, update.effective_user.id))

async def monitor_loop(bot: Bot):
    while True:
        try:
            logger.info("Starting periodic check cycle...")
            for account in db_get_all_accounts():
                try:
                    if datetime.now() - account.get('last_check', datetime.min) >= timedelta(minutes=account.get('interval_minutes', 60)):
                        asyncio.create_task(check_and_upload_account(bot, account, account['owner_id']))
                except Exception as e:
                    logger.critical(f"UNHANDLED account error for {account.get('ig_username', 'N/A')}: {e}")
                    safe_error = str(e).replace("<","&lt;").replace(">","&gt;")
                    await log_to_channel(bot, int(ADMIN_USER_ID), f"‼️ MONITOR ERROR for {account.get('ig_username', 'N/A')}: {safe_error}", forward_error_to=int(ADMIN_USER_ID))
            await asyncio.sleep(60)
        except Exception as e:
            logger.critical(f"FATAL MONITOR LOOP CRASH: {e}")
            safe_error = str(e).replace("<","&lt;").replace(">","&gt;")
            await log_to_channel(bot, int(ADMIN_USER_ID), f"‼️ MONITOR LOOP CRASHED: {safe_error}", forward_error_to=int(ADMIN_USER_ID))
            await asyncio.sleep(300)

# ========================================================================================
# =====                             MAIN APPLICATION SETUP                           =====
# ========================================================================================

def main():
    web_thread = threading.Thread(target=lambda: http.server.HTTPServer(("", int(os.environ.get("PORT", 8080))), http.server.SimpleHTTPRequestHandler).serve_forever())
    web_thread.daemon = True; web_thread.start()
    logger.info(f"Web server started on port {os.environ.get('PORT', 8080)}.")
    
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    
    conv_handlers = [
        ConversationHandler(entry_points=[CommandHandler("addaccount", add_account_start)], states={AWAIT_SESSION_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_account_get_session_id)]}, fallbacks=[CommandHandler("cancel", cancel_conversation)]),
        ConversationHandler(entry_points=[CallbackQueryHandler(button_callback_handler, pattern="^settarget_")], states={AWAIT_TARGET_CHAT_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_target_chat_id)]}, fallbacks=[CommandHandler("cancel", cancel_conversation)], per_message=False),
        ConversationHandler(entry_points=[CallbackQueryHandler(button_callback_handler, pattern="^setinterval_")], states={AWAIT_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_interval)]}, fallbacks=[CommandHandler("cancel", cancel_conversation)], per_message=False)
    ]
    cmd_handlers = [CommandHandler(cmd, func) for cmd, func in [
        ("start", start_command), ("help", start_command), ("ping", ping_command),
        ("status", status_command), ("restart", restart_command), ("myaccounts", my_accounts_command),
        ("logc", log_channel_command), ("testlog", test_log_command), ("forcecheck", force_check_command),
        ("checkchat", check_chat_command), ("kill", kill_command)
    ]]
    
    application.add_handlers(conv_handlers + cmd_handlers)
    application.add_handler(CallbackQueryHandler(manage_account_menu, pattern="^manage_"))
    application.add_handler(CallbackQueryHandler(my_accounts_command, pattern="^myaccounts$"))
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    monitor_thread = threading.Thread(target=lambda: asyncio.run(monitor_loop(application.bot)))
    monitor_thread.daemon = True; monitor_thread.start()
    logger.info("Instagram monitoring loop started.")

    logger.info("Telegram bot is polling for updates...")
    application.run_polling()

if __name__ == "__main__":
    main()
