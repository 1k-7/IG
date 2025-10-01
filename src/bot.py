#  ========================================================================================
#  =====              COMPLETE INSTAGRAM TELEGRAM BOT (BOT-MANAGED SESSIONS)          =====
#  ========================================================================================
#  This version removes the initial environment variable-based login, making all
#  session management happen exclusively through the bot interface.
#  ========================================================================================

import os
import threading
import http.server
import socketserver
import time
import asyncio
import logging
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
            if os.path.exists(session_path): self.client.load_settings(session_path)
            self.client.login_by_sessionid(session_id)
            self.client.user_info_by_username(self.username)
            self.client.dump_settings(session_path)
            logger.info(f"Successfully logged into Instagram as {self.username}")
        except Exception as e:
            logger.error(f"Failed to initialize Instagram client for {self.username}: {e}")
            raise

    def get_new_reels_from_dms(self):
        new_reels = []
        threads = self.client.direct_threads(amount=20)
        for thread in threads:
            messages = self.client.direct_messages(thread.id, amount=20)
            for message in messages:
                sender_username = "Unknown Sender"
                if hasattr(message, 'user') and message.user:
                    sender_username = message.user.username

                if message.item_type == "clip" and not db_has_seen_reel(self.username, message.clip.pk):
                    new_reels.append({
                        "reel_pk": message.clip.pk,
                        "ig_chat_name": thread.thread_title,
                        "from_user": sender_username
                    })
        return new_reels

    def download_reel(self, reel_pk):
        download_path = "/tmp/downloads"
        os.makedirs(download_path, exist_ok=True)
        return self.client.video_download_to_path(reel_pk, folder=download_path)

# ========================================================================================
# =====                            TELEGRAM HANDLERS LOGIC                           =====
# ========================================================================================

(AWAIT_SESSION_ID, AWAIT_TARGET_CHAT_ID, AWAIT_INTERVAL) = range(3)

async def log_to_channel(bot: Bot, user_id, message: str, forward_error_to: int = None):
    log_channel_id = db_get_log_channel(user_id)
    if log_channel_id:
        try:
            await bot.send_message(chat_id=log_channel_id, text=message)
            return True
        except TelegramError as e:
            logger.error(f"Failed to send log to channel {log_channel_id}: {e}")
            if forward_error_to:
                safe_error_message = str(e).replace("<", "&lt;").replace(">", "&gt;")
                await bot.send_message(
                    chat_id=forward_error_to,
                    text=f"⚠️ <b>Log Channel Error!</b>\nFailed to send log to <code>{log_channel_id}</code>.\n<b>Reason:</b> {safe_error_message}",
                    parse_mode='HTML'
                )
    return False

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_add_user(user.id)
    await update.message.reply_html(
        f"👋 <b>Hi {user.mention_html()}!</b>\n\nI am your Instagram DM Reels Bot.\n\n"
        "<b>Key Commands:</b>\n"
        "<code>/addaccount</code> - Link a new Instagram account.\n"
        "<code>/myaccounts</code> - View and manage your accounts.\n"
        "<code>/forcecheck &lt;username&gt;</code> - (Admin) Force an immediate check for an account.\n"
        "<code>/logc &lt;ID&gt;</code> - (Admin) Set a channel for bot logs.\n"
        "<code>/testlog</code> - (Admin) Test the log channel.\n"
        "<code>/ping</code> - Check if the bot is alive.\n"
        "<code>/help</code> - Show this message."
    )

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
        await update.message.reply_text(f"❌ Login failed: {e}.\n\nPlease check your session ID, or /cancel.")
        return AWAIT_SESSION_ID

async def my_accounts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    accounts = db_get_user_accounts(update.effective_user.id)
    if not accounts:
        await update.message.reply_text("You haven't linked any Instagram accounts. Use /addaccount.")
        return
    
    keyboard = [[InlineKeyboardButton(a['ig_username'], callback_data=f"manage_{a['ig_username']}")] for a in accounts]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = "Select an Instagram account to manage:"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

async def manage_account_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ig_username = query.data.split("_")[1]
    
    account = db_get_account(ig_username)
    if not account:
        await query.edit_message_text("This account no longer exists.")
        return

    text = (f"<b>Managing: {ig_username}</b>\n\n"
            f"Target Chat: <code>{account.get('target_chat_id', 'Not Set')}</code>\n"
            f"Check Interval: <code>{account.get('interval_minutes', 60)}</code> mins\n"
            f"Topic Mode: {'✅ Enabled' if account.get('topic_mode') else '❌ Disabled'}")
    
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
    await update.message.reply_text("✅ Bot is running.\n✅ Instagram monitor loop is active.")

async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text("Restarting bot...")

async def log_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /logc <channel_or_group_id>")
        return
    try:
        db_set_log_channel(int(ADMIN_USER_ID), int(context.args[0]))
        await update.message.reply_html(f"✅ Log channel set to <code>{context.args[0]}</code>. Use /testlog to verify.")
    except (IndexError, ValueError):
        await update.message.reply_text("Invalid ID.")

async def test_log_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != ADMIN_USER_ID:
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text("Sending test message...")
    if await log_to_channel(context.bot, int(ADMIN_USER_ID), "✅ This is a test message from the bot.", forward_error_to=update.effective_chat.id):
        await update.message.reply_text("Test message sent successfully!")

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operation cancelled.")
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
            await update.message.reply_text("Interval must be at least 5 mins.")
            return AWAIT_INTERVAL
        db_set_periodic_interval(ig_username, interval)
        await update.message.reply_html(f"✅ Interval for <b>{ig_username}</b> set to {interval} mins.")
    except ValueError:
        await update.message.reply_text("Invalid number.")
    del context.user_data['ig_username_to_manage']
    return ConversationHandler.END

# ========================================================================================
# =====                     CORE MONITORING AND UPLOAD LOGIC                         =====
# ========================================================================================

async def check_and_upload_account(bot: Bot, account: dict, owner_id: int):
    ig_username = account['ig_username']
    session_id = account['ig_session_id']
    target_chat_id = account.get('target_chat_id')
    topic_mode = account.get('topic_mode', False)

    if not target_chat_id:
        logger.warning(f"Skipping {ig_username} as no target chat is set.")
        return

    try:
        await log_to_channel(bot, owner_id, f"🔍 Checking DMs for {ig_username}...", forward_error_to=owner_id)
        ig_client = InstagramClient(ig_username, session_id)
        new_reels = ig_client.get_new_reels_from_dms()
        db_update_last_check(ig_username)
        
        if not new_reels:
            await log_to_channel(bot, owner_id, f"✅ No new reels found for {ig_username}.", forward_error_to=owner_id)
            return
        
        await log_to_channel(bot, owner_id, f"Found {len(new_reels)} new reel(s) for {ig_username}. Starting uploads.", forward_error_to=owner_id)
        for reel in new_reels:
            filepath = ig_client.download_reel(reel['reel_pk'])
            caption = f"🎬 Reel from <b>{reel['from_user']}</b>\n💬 In chat: <i>{reel['ig_chat_name']}</i>"
            
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
                    await log_to_channel(bot, owner_id, f"⚠️ Topic Error for {ig_username}: {e}", forward_error_to=owner_id)

            with open(filepath, 'rb') as video_file:
                await bot.send_video(
                    chat_id=target_chat_id, video=video_file, caption=caption,
                    parse_mode='HTML', message_thread_id=message_thread_id
                )
            
            db_add_seen_reel(ig_username, reel['reel_pk'])
            os.remove(filepath)
            await log_to_channel(bot, owner_id, f"✅ Sent reel from {reel['from_user']}.", forward_error_to=owner_id)
        
        await log_to_channel(bot, owner_id, f"🎉 Finished all uploads for {ig_username}.", forward_error_to=owner_id)

    except Exception as e:
        logger.error(f"CRITICAL ERROR processing {ig_username}: {e}")
        safe_error_message = str(e).replace("<", "&lt;").replace(">", "&gt;")
        await log_to_channel(bot, owner_id, f"❌ CRITICAL ERROR for {ig_username}: {safe_error_message}", forward_error_to=owner_id)

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
        await update.message.reply_text(f"Account '{ig_username}' not found or not yours.")
        return
        
    await update.message.reply_html(f"Manual check for <b>{ig_username}</b> triggered. Check log channel for progress.")
    asyncio.create_task(check_and_upload_account(context.bot, account, update.effective_user.id))

async def monitor_loop(bot: Bot):
    while True:
        try:
            logger.info("Starting periodic check cycle...")
            all_accounts = db_get_all_accounts()
            
            for account in all_accounts:
                last_check = account.get('last_check', datetime.min)
                interval = timedelta(minutes=account.get('interval_minutes', 60))
                if datetime.now() - last_check >= interval:
                    asyncio.create_task(check_and_upload_account(bot, account, account['owner_id']))
            
            await asyncio.sleep(60)
        except Exception as e:
            logger.critical(f"UNHANDLED EXCEPTION IN MONITOR LOOP: {e}")
            if ADMIN_USER_ID:
                safe_error_message = str(e).replace("<", "&lt;").replace(">", "&gt;")
                await log_to_channel(bot, int(ADMIN_USER_ID), f"‼️ MONITOR LOOP CRASHED: {safe_error_message}", forward_error_to=int(ADMIN_USER_ID))
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
        ("logc", log_channel_command), ("testlog", test_log_command), ("forcecheck", force_check_command)
    ]]
    
    application.add_handlers(conv_handlers + cmd_handlers)
    application.add_handler(CallbackQueryHandler(manage_account_menu, pattern="^manage_"))
    application.add_handler(CallbackQueryHandler(my_accounts_command, pattern="^myaccounts$"))
    application.add_handler(CallbackQueryHandler(button_callback_handler))

    async def run_monitor_in_loop():
        await asyncio.sleep(15)
        await monitor_loop(application.bot)

    monitor_thread = threading.Thread(target=lambda: asyncio.run(run_monitor_in_loop()))
    monitor_thread.daemon = True; monitor_thread.start()
    logger.info("Instagram monitoring loop started.")

    logger.info("Telegram bot is polling for updates...")
    application.run_polling()

if __name__ == "__main__":
    main()
