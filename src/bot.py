#  ========================================================================================
#  =====        COMPLETE INSTAGRAM TELEGRAM BOT (STABLE & CORRECTED)                  =====
#  ========================================================================================
#  This final version uses the stable, proven logic from the reference project to
#  eliminate all errors and ensure full functionality.
#  ========================================================================================

import os
import threading
import http.server
import time
import logging
from datetime import datetime, timedelta
from pymongo import MongoClient
from urllib.parse import quote_plus
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater, CommandHandler, ConversationHandler,
    MessageHandler, Filters, CallbackContext, CallbackQueryHandler
)
from telegram.error import TelegramError
from instagrapi import Client
from instagrapi.exceptions import LoginRequired

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
        {"$set": {"ig_session_id": ig_session_id, "last_check": datetime.now() - timedelta(hours=24), "interval_minutes": 60, "is_active": True}},
        upsert=True
    )

def db_get_user_accounts(user_id):
    return list(accounts_collection.find({"owner_id": user_id}))

def db_get_account(ig_username):
    return accounts_collection.find_one({"ig_username": ig_username})

def db_remove_account(user_id, ig_username):
    accounts_collection.delete_one({"owner_id": user_id, "ig_username": ig_username})
    seen_reels_collection.delete_many({"ig_username": ig_username})

def db_set_target_chat(ig_username, chat_id):
    accounts_collection.update_one({"ig_username": ig_username}, {"$set": {"target_chat_id": chat_id}})

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

# ========================================================================================
# =====                             INSTAGRAM CLIENT LOGIC                           =====
# ========================================================================================
class InstagramClient:
    def __init__(self, username, session_id):
        self.username = username
        self.client = Client()
        session_path = f"/tmp/{self.username}_session.json"
        
        if os.path.exists(session_path):
            self.client.load_settings(session_path)

        self.client.login_by_sessionid(session_id)
        self.client.get_timeline_feed() # Lightweight check to validate the session
        self.client.dump_settings(session_path)
        logger.info(f"IG client for {self.username} initialized and session validated.")

    def get_new_reels_from_dms(self):
        new_reels_data = []
        # Fetch ALL threads. This is slow but ensures everything is checked.
        threads = self.client.direct_threads() 
        for thread in threads:
            # Fetch ALL messages in each thread.
            messages = self.client.direct_messages(thread.id)
            for message in messages:
                if message.item_type == "clip" and not db_has_seen_reel(self.username, message.clip.pk):
                    # Correctly find the sender's user object from the thread's user list
                    sender = None
                    for user in thread.users:
                        if user.pk == message.user_id:
                            sender = user
                            break
                    if sender: # Only proceed if we found the sender
                        new_reels_data.append({"message": message, "sender": sender})
        return new_reels_data

    def download_reel(self, reel_pk):
        download_path = "/tmp/downloads"
        os.makedirs(download_path, exist_ok=True)
        return self.client.video_download(reel_pk, folder=download_path)

# ========================================================================================
# =====                             UPLOAD PROGRESS LOGIC                            =====
# ========================================================================================

class ProgressManager:
    def __init__(self, bot: Bot, chat_id: int, message_id: int, total_size: int, filename: str):
        self.bot, self.chat_id, self.message_id = bot, chat_id, message_id
        self.total_size, self.filename = total_size, filename
        self.uploaded_size, self.start_time, self.last_update_time = 0, time.time(), 0

    def progress_callback(self, current, total):
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
            self.bot.edit_message_text(text, chat_id=self.chat_id, message_id=self.message_id, parse_mode='HTML')
        except TelegramError:
            pass

def upload_video_with_progress(bot: Bot, chat_id: int, video_path: str, caption: str, message_thread_id: int = None):
    filename = os.path.basename(str(video_path))
    total_size = os.path.getsize(video_path)
    status_message = bot.send_message(chat_id, f"Preparing to upload: {filename}", message_thread_id=message_thread_id)
    progress = ProgressManager(bot, chat_id, status_message.message_id, total_size, filename)
    try:
        with open(video_path, 'rb') as video_file:
            bot.send_video(
                chat_id=chat_id, video=video_file, caption=caption, parse_mode='HTML',
                message_thread_id=message_thread_id, timeout=120,
                progress=progress.progress_callback
            )
        bot.delete_message(chat_id, status_message.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Upload failed for {filename}.\n<b>Reason:</b> {e}", chat_id, status_message.message_id, parse_mode='HTML')
        raise
# ========================================================================================
# =====                            TELEGRAM HANDLERS LOGIC                           =====
# ========================================================================================

(AWAIT_SESSION_ID, AWAIT_TARGET_CHAT_ID, AWAIT_INTERVAL) = range(3)

def log_to_channel(bot: Bot, user_id, message: str, forward_error_to: int = None):
    log_channel_id = db_get_log_channel(user_id)
    if log_channel_id:
        try:
            bot.send_message(chat_id=log_channel_id, text=message, parse_mode='HTML')
        except TelegramError as e:
            logger.error(f"Failed to send log to {log_channel_id}: {e}")
            if forward_error_to:
                safe_error = str(e).replace("<", "&lt;").replace(">", "&gt;")
                bot.send_message(
                    chat_id=forward_error_to,
                    text=f"⚠️ <b>Log Channel Error!</b>\nTo <code>{log_channel_id}</code>: {safe_error}",
                    parse_mode='HTML'
                )

def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    db_add_user(user.id)
    update.message.reply_html(
        f"👋 <b>Hi {user.mention_html()}!</b>\n\nI am your Instagram DM Reels Bot.\n\n"
        "<b>Admin Commands:</b>\n"
        "<code>/addaccount</code> - Link a new Instagram account.\n"
        "<code>/myaccounts</code> - View and manage your accounts.\n"
        "<code>/forcecheck &lt;username&gt;</code> - Force an immediate check.\n"
        "<code>/logc &lt;ID&gt;</code> - Set a channel for bot logs.\n"
        "<code>/kill</code> - Instantly shut down the bot.\n\n"
        "<b>Public Commands:</b>\n"
        "<code>/ping</code> - Check if the bot is alive.\n"
        "<code>/help</code> - Show this message."
    )

def kill_command(update: Update, context: CallbackContext):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        update.message.reply_text("⛔ Not authorized.")
        return
    update.message.reply_text("Shutting down immediately...")
    logger.info("Immediate shutdown command received. Terminating process.")
    # Use os._exit(0) for an immediate, forceful shutdown.
    os._exit(0)

def add_account_start(update: Update, context: CallbackContext):
    update.message.reply_text("Please send the `sessionid` cookie from your Instagram account.\n\nSend /cancel to stop.")
    return AWAIT_SESSION_ID

def add_account_get_session_id(update: Update, context: CallbackContext):
    session_id = update.message.text
    user_id = update.effective_user.id
    try:
        update.message.reply_text("Validating session and logging in...")
        temp_client = Client()
        temp_client.login_by_sessionid(session_id)
        ig_username = temp_client.user_info(temp_client.user_id).username
        
        db_add_or_update_account(user_id, ig_username, session_id)
        update.message.reply_html(f"✅ Success! Instagram account <b>{ig_username}</b> linked. Use <code>/myaccounts</code> to configure it.")
        return ConversationHandler.END
    except Exception as e:
        update.message.reply_html(f"❌ Login failed: {e}.\n\nPlease check your session ID, or /cancel.")
        return AWAIT_SESSION_ID

def my_accounts_command(update: Update, context: CallbackContext):
    accounts = db_get_user_accounts(update.effective_user.id)
    if not accounts:
        update.message.reply_text("You haven't linked any Instagram accounts. Use /addaccount.")
        return
    keyboard = [[InlineKeyboardButton(a['ig_username'], callback_data=f"manage_{a['ig_username']}")] for a in accounts]
    text = "Select an Instagram account to manage:"
    if update.callback_query:
        update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

def manage_account_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    ig_username = query.data.split("_")[1]
    account = db_get_account(ig_username)
    if not account:
        query.edit_message_text("This account no longer exists.")
        return
    text = (
        f"<b>Managing: {ig_username}</b>\n\n"
        f"Target Chat: <code>{account.get('target_chat_id', 'Not Set')}</code>\n"
        f"Check Interval: <code>{account.get('interval_minutes', 60)}</code> mins"
    )
    keyboard = [
        [InlineKeyboardButton("🎯 Set Target Chat", callback_data=f"settarget_{ig_username}")],
        [InlineKeyboardButton("⏰ Set Interval", callback_data=f"setinterval_{ig_username}")],
        [InlineKeyboardButton("🗑️ Clear Seen Data", callback_data=f"cleardata_{ig_username}")],
        [InlineKeyboardButton("❌ Remove Account", callback_data=f"remove_{ig_username}")],
        [InlineKeyboardButton("⬅️ Back to Accounts", callback_data="myaccounts")]
    ]
    query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

def ping_command(update: Update, context: CallbackContext):
    update.message.reply_text("Pong! 🏓")

def status_command(update: Update, context: CallbackContext):
    update.message.reply_text("✅ Bot is running.")

def log_channel_command(update: Update, context: CallbackContext):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        update.message.reply_text("Usage: /logc <ID>")
        return
    try:
        db_set_log_channel(int(ADMIN_USER_ID), int(context.args[0]))
        update.message.reply_html(f"✅ Log channel set to <code>{context.args[0]}</code>.")
    except (IndexError, ValueError):
        update.message.reply_text("Invalid ID.")

def test_log_command(update: Update, context: CallbackContext):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        update.message.reply_text("⛔ Not authorized.")
        return
    update.message.reply_text("Sending test message...")
    if log_to_channel(context.bot, int(ADMIN_USER_ID), "✅ Test message.", forward_error_to=update.effective_chat.id):
        update.message.reply_text("Test message sent!")

def cancel_conversation(update: Update, context: CallbackContext):
    update.message.reply_text("Cancelled.")
    return ConversationHandler.END

def button_callback_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    action, ig_username = query.data.split("_", 1)
    context.user_data['ig_username_to_manage'] = ig_username
    if action == "settarget":
        query.message.reply_html(f"Please send the target chat ID for <b>{ig_username}</b>.")
        return AWAIT_TARGET_CHAT_ID
    elif action == "setinterval":
        query.message.reply_html(f"Please send the interval in minutes for <b>{ig_username}</b>.")
        return AWAIT_INTERVAL
    elif action == "cleardata":
        db_clear_seen_reels(ig_username)
        query.message.reply_html(f"✅ Seen data for <b>{ig_username}</b> cleared. All reels will be downloaded on the next check.")
        query.data = f"manage_{ig_username}"
        manage_account_menu(update, context)
    elif action == "remove":
        kbd = [[InlineKeyboardButton("YES, REMOVE", callback_data=f"confirmremove_{ig_username}"), InlineKeyboardButton("NO", callback_data=f"manage_{ig_username}")]]
        query.edit_message_text(f"⚠️ Remove <b>{ig_username}</b>?", reply_markup=InlineKeyboardMarkup(kbd), parse_mode='HTML')
    elif action == "confirmremove":
        db_remove_account(update.effective_user.id, ig_username)
        query.edit_message_text(f"✅ Account <b>{ig_username}</b> has been removed.")

def get_target_chat_id(update: Update, context: CallbackContext):
    ig_username = context.user_data.get('ig_username_to_manage')
    if not ig_username: return ConversationHandler.END
    try:
        db_set_target_chat(ig_username, int(update.message.text))
        update.message.reply_html(f"✅ Target chat for <b>{ig_username}</b> set.")
    except ValueError:
        update.message.reply_text("Invalid ID.")
    del context.user_data['ig_username_to_manage']
    return ConversationHandler.END

def get_interval(update: Update, context: CallbackContext):
    ig_username = context.user_data.get('ig_username_to_manage')
    if not ig_username: return ConversationHandler.END
    try:
        interval = int(update.message.text)
        if interval < 1:
            update.message.reply_text("Min interval is 1 min.")
            return AWAIT_INTERVAL
        db_set_periodic_interval(ig_username, interval)
        update.message.reply_html(f"✅ Interval for <b>{ig_username}</b> set to {interval} mins.")
    except ValueError:
        update.message.reply_text("Invalid number.")
    del context.user_data['ig_username_to_manage']
    return ConversationHandler.END

# ========================================================================================
# =====                     CORE MONITORING AND UPLOAD LOGIC                         =====
# ========================================================================================

def check_and_upload_account(context: CallbackContext):
    bot = context.bot
    account = context.job.context
    owner_id = account['owner_id']
    ig_username = account.get('ig_username')
    session_id = account.get('ig_session_id')
    target_chat_id = account.get('target_chat_id')
    
    if not all([ig_username, session_id, target_chat_id]): return
    
    try:
        log_to_channel(bot, owner_id, f"🔍 Checking {ig_username}...", forward_error_to=owner_id)
        ig_client = InstagramClient(ig_username, session_id)
        new_reels_data = ig_client.get_new_reels_from_dms()
        db_update_last_check(ig_username)
        
        if not new_reels_data:
            log_to_channel(bot, owner_id, f"✅ No new reels for {ig_username}.", forward_error_to=owner_id)
            return
        
        log_to_channel(bot, owner_id, f"Found {len(new_reels_data)} new reel(s) for {ig_username}.", forward_error_to=owner_id)
        
        for reel_data in new_reels_data:
            message = reel_data["message"]
            sender = reel_data["sender"]
            clip = message.clip
            
            caption_text = (clip.caption_text or "").strip()
            
            caption = (
                f"<i>Reel by <a href='https://instagram.com/{clip.user.username}'>{clip.user.full_name or clip.user.username}</a></i>\n\n"
                f"{caption_text}\n\n"
                f"<b>Shared by:</b> {sender.username}"
            )
            
            filepath = ig_client.download_reel(clip.pk)
            upload_video_with_progress(bot, target_chat_id, filepath, caption)
            os.remove(filepath)

            db_add_seen_reel(ig_username, clip.pk)
            log_to_channel(bot, owner_id, f"✅ Sent reel from {sender.username}.", forward_error_to=owner_id)
        
        log_to_channel(bot, owner_id, f"🎉 Finished all uploads for {ig_username}.", forward_error_to=owner_id)
    
    except LoginRequired:
        log_to_channel(bot, owner_id, f"🚨 <b>SESSION EXPIRED for {ig_username}.</b> Please use /addaccount to link it again.", forward_error_to=owner_id)
    except Exception as e:
        safe_error = str(e).replace("<", "&lt;").replace(">", "&gt;")
        log_to_channel(bot, owner_id, f"❌ CRITICAL ERROR for {ig_username}: {safe_error}", forward_error_to=owner_id)

def force_check_command(update: Update, context: CallbackContext):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        update.message.reply_text("Usage: /forcecheck <instagram_username>")
        return
    ig_username = context.args[0]
    account = db_get_account(ig_username)
    if not account or account['owner_id'] != update.effective_user.id:
        update.message.reply_text(f"Account '{ig_username}' not found.")
        return
    update.message.reply_html(f"Manual check for <b>{ig_username}</b> triggered.")
    context.job_queue.run_once(check_and_upload_account, 1, context=account, name=f"manual_check_{ig_username}")

def monitor_master_job(context: CallbackContext):
    for account in db_get_all_accounts():
        if datetime.now() - account.get('last_check', datetime.min) >= timedelta(minutes=account.get('interval_minutes', 60)):
            context.job_queue.run_once(
                check_and_upload_account, 
                when=1, 
                context=account,
                name=f"check_{account['ig_username']}"
            )

# ========================================================================================
# =====                             MAIN APPLICATION SETUP                           =====
# ========================================================================================

def main():
    web_thread = threading.Thread(target=lambda: http.server.HTTPServer(("", int(os.environ.get("PORT", 8080))), http.server.SimpleHTTPRequestHandler).serve_forever())
    web_thread.daemon = True
    web_thread.start()
    logger.info(f"Web server started on port {os.environ.get('PORT', 8080)}.")
    
    updater = Updater(TELEGRAM_BOT_TOKEN, use_context=True)
    dispatcher = updater.dispatcher

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("addaccount", add_account_start),
            CallbackQueryHandler(button_callback_handler, pattern="^settarget_"),
            CallbackQueryHandler(button_callback_handler, pattern="^setinterval_")
        ],
        states={
            AWAIT_SESSION_ID: [MessageHandler(Filters.text & ~Filters.command, add_account_get_session_id)],
            AWAIT_TARGET_CHAT_ID: [MessageHandler(Filters.text & ~Filters.command, get_target_chat_id)],
            AWAIT_INTERVAL: [MessageHandler(Filters.text & ~Filters.command, get_interval)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    dispatcher.add_handler(conv_handler)
    dispatcher.add_handler(CommandHandler("start", start_command))
    dispatcher.add_handler(CommandHandler("help", start_command))
    dispatcher.add_handler(CommandHandler("ping", ping_command))
    dispatcher.add_handler(CommandHandler("status", status_command))
    dispatcher.add_handler(CommandHandler("kill", kill_command))
    dispatcher.add_handler(CommandHandler("myaccounts", my_accounts_command))
    dispatcher.add_handler(CommandHandler("logc", log_channel_command))
    dispatcher.add_handler(CommandHandler("testlog", test_log_command))
    dispatcher.add_handler(CommandHandler("forcecheck", force_check_command))
    
    dispatcher.add_handler(CallbackQueryHandler(manage_account_menu, pattern="^manage_"))
    dispatcher.add_handler(CallbackQueryHandler(my_accounts_command, pattern="^myaccounts$"))
    dispatcher.add_handler(CallbackQueryHandler(button_callback_handler))

    job_queue = updater.job_queue
    job_queue.run_repeating(monitor_master_job, interval=60, first=10)

    logger.info("Telegram bot is polling for updates...")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
