#  ========================================================================================
#  =====                       COMPLETE INSTAGRAM TELEGRAM BOT                        =====
#  ========================================================================================
#  This script combines all functionality into a single file for deployment.
#  It includes a web server for Render, a background Instagram monitor,
#  and a full-featured Telegram bot for control.
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
topics_collection = db.topics # For topic mode

# --- User Management ---
def db_add_user(user_id):
    users_collection.update_one({"user_id": user_id}, {"$setOnInsert": {"user_id": user_id}}, upsert=True)

def db_set_log_channel(user_id, channel_id):
    users_collection.update_one({"user_id": user_id}, {"$set": {"log_channel": channel_id}})

def db_get_log_channel(user_id):
    user = users_collection.find_one({"user_id": user_id})
    return user.get("log_channel") if user else None

def db_get_all_accounts():
    return list(accounts_collection.find({}))

# --- Instagram Account Management ---
def db_add_or_update_account(user_id, ig_username, ig_session_id):
    accounts_collection.update_one(
        {"owner_id": user_id, "ig_username": ig_username},
        {"$set": {"ig_session_id": ig_session_id, "last_check": datetime.now(), "interval_minutes": 60, "is_active": True}},
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

# --- Seen Reels Management ---
def db_add_seen_reel(ig_username, reel_pk, message_id):
    seen_reels_collection.insert_one({"ig_username": ig_username, "reel_pk": reel_pk, "message_id": message_id})

def db_has_seen_reel(ig_username, reel_pk):
    return seen_reels_collection.count_documents({"ig_username": ig_username, "reel_pk": reel_pk}) > 0

def db_clear_seen_reels(ig_username):
    seen_reels_collection.delete_many({"ig_username": ig_username})

# --- Topic Management ---
def db_get_or_create_topic(chat_id, topic_name):
    topic = topics_collection.find_one({"chat_id": chat_id, "name": topic_name})
    if topic:
        return topic.get("topic_id")
    return None

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
            self.client.user_info_by_username(self.username) # Validate session
            self.client.dump_settings(session_path)
            logger.info(f"Successfully logged into Instagram as {self.username}")
        except (LoginRequired, ChallengeRequired, PrivateError) as e:
            logger.warning(f"Session for {self.username} is invalid. Re-logging in. Error: {e}")
            self.client.login_by_sessionid(session_id)
            self.client.dump_settings(session_path)
        except Exception as e:
            logger.error(f"An unexpected error occurred for {self.username}: {e}")
            raise

    def get_new_reels_from_dms(self):
        new_reels = []
        threads = self.client.direct_threads(amount=20)
        for thread in threads:
            messages = self.client.direct_messages(thread.id, amount=20)
            for message in messages:
                if message.item_type == "clip" and not db_has_seen_reel(self.username, message.clip.pk):
                    reel_info = {
                        "reel_pk": message.clip.pk,
                        "message_id": message.id,
                        "ig_chat_name": thread.thread_title,
                        "from_user": message.user.username,
                        "caption": message.clip.caption_text
                    }
                    new_reels.append(reel_info)
        return new_reels

    def download_reel(self, reel_pk):
        download_path = "/tmp/downloads"
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        filepath = self.client.video_download_to_path(reel_pk, folder=download_path)
        return filepath

# ========================================================================================
# =====                            TELEGRAM HANDLERS LOGIC                           =====
# ========================================================================================

# --- Conversation States for Handlers ---
(
    AWAIT_SESSION_ID, AWAIT_IG_USERNAME_TO_MANAGE, AWAIT_TARGET_CHAT_ID,
    AWAIT_INTERVAL, AWAIT_LOG_CHANNEL_ID, AWAIT_IG_USERNAME_TO_CLEAR, AWAIT_CONFIRM_REMOVE
) = range(7)

async def log_to_channel(bot: Bot, user_id, message: str):
    log_channel_id = db_get_log_channel(user_id)
    if log_channel_id:
        try:
            await bot.send_message(chat_id=log_channel_id, text=f"LOG: {message}")
        except Exception as e:
            logger.error(f"Failed to send log to channel {log_channel_id}: {e}")

# --- Command Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_add_user(user.id)
    welcome_text = (
        f"👋 **Hi {user.mention_html()}!**\n\n"
        "I am your personal Instagram DM Reels Bot. I monitor your DMs and automatically forward any reels to you here.\n\n"
        "**Features:**\n"
        "🔗 Link multiple Instagram accounts via `/addaccount`.\n"
        "🎯 Set a target chat (group or channel) for uploads.\n"
        "🔄 Customize the check interval for each account.\n"
        "📂 Enable **Topic Mode** in supergroups to sort reels by chat.\n\n"
        "**Available Commands:**\n"
        "`/addaccount` - Link a new Instagram account.\n"
        "`/myaccounts` - View and manage your linked accounts.\n"
        "`/logc <ID>` - Set a channel for bot logs.\n"
        "`/ping` - Check if the bot is alive.\n"
        "`/status` - View bot status.\n"
        "`/restart` - (Admin only) Restart the bot.\n"
        "`/help` - Show this welcome message again."
    )
    await update.message.reply_html(welcome_text, disable_web_page_preview=True)

async def add_account_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Please send the `sessionid` cookie from your Instagram account. This is required for a stable login.\n\nSend /cancel to stop.")
    return AWAIT_SESSION_ID

async def add_account_get_session_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_id = update.message.text
    user_id = update.effective_user.id
    try:
        await update.message.reply_text("Validating session ID and logging in...")
        temp_client = Client()
        temp_client.login_by_sessionid(session_id)
        ig_username = temp_client.user_info(temp_client.user_id).username
        
        db_add_or_update_account(user_id, ig_username, session_id)
        await update.message.reply_text(f"✅ Success! Instagram account **{ig_username}** has been linked. Use `/myaccounts` to configure it.")
        return ConversationHandler.END
    except Exception as e:
        await update.message.reply_text(f"❌ Login failed: {e}. Please check your session ID and try again, or /cancel.")
        return AWAIT_SESSION_ID

async def my_accounts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    accounts = db_get_user_accounts(user_id)
    if not accounts:
        await update.message.reply_text("You haven't linked any Instagram accounts yet. Use /addaccount to start.")
        return

    keyboard = [[InlineKeyboardButton(acc['ig_username'], callback_data=f"manage_{acc['ig_username']}")] for acc in accounts]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message_text = "Select an Instagram account to manage:"
    if update.callback_query:
        await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(message_text, reply_markup=reply_markup)

async def manage_account_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    ig_username = query.data.split("_")[1]
    
    account = db_get_account(ig_username)
    if not account:
        await query.edit_message_text("This account no longer exists.")
        return

    target_chat = account.get('target_chat_id', 'Not Set')
    interval = account.get('interval_minutes', '60')
    topic_mode = "✅ Enabled" if account.get('topic_mode') else "❌ Disabled"

    text = (
        f"**Managing: {ig_username}**\n\n"
        f"Target Chat: `{target_chat}`\n"
        f"Check Interval: `{interval}` minutes\n"
        f"Topic Mode: `{topic_mode}`"
    )
    
    keyboard = [
        [InlineKeyboardButton("🎯 Set Target Chat", callback_data=f"settarget_{ig_username}")],
        [InlineKeyboardButton("⏰ Set Interval", callback_data=f"setinterval_{ig_username}")],
        [InlineKeyboardButton("📂 Toggle Topic Mode", callback_data=f"toggletopic_{ig_username}")],
        [InlineKeyboardButton("🗑️ Clear Seen Data", callback_data=f"cleardata_{ig_username}")],
        [InlineKeyboardButton("❌ Remove Account", callback_data=f"remove_{ig_username}")],
        [InlineKeyboardButton("⬅️ Back to Accounts", callback_data="myaccounts")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong! 🏓 I am alive and running.")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot is running. Instagram monitor loop is active.")

async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ You are not authorized to use this command.")
        return
    await update.message.reply_text("Bot is restarting... This can take a minute on Render.")
    # A proper restart is handled by the deployment platform. This command is a stub.
    # For standalone use, you could use: os.execv(sys.executable, ['python'] + sys.argv)

async def log_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ You are not authorized to use this command.")
        return
    try:
        channel_id = int(context.args[0])
        db_set_log_channel(user_id, channel_id)
        await update.message.reply_text(f"✅ Log channel has been set to `{channel_id}`.")
        await log_to_channel(context.bot, user_id, "Log channel has been configured.")
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /logc <channel_id>")

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END

# --- Callback Query Handlers for Menus ---
async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    action, ig_username = query.data.split("_", 1)

    if action == "settarget":
        context.user_data['ig_username_to_manage'] = ig_username
        await query.message.reply_text(f"Please send the ID of the target chat for **{ig_username}**.")
        return AWAIT_TARGET_CHAT_ID
        
    elif action == "setinterval":
        context.user_data['ig_username_to_manage'] = ig_username
        await query.message.reply_text(f"Please send the check interval in minutes for **{ig_username}** (e.g., 30).")
        return AWAIT_INTERVAL

    elif action == "toggletopic":
        account = db_get_account(ig_username)
        new_mode = not account.get('topic_mode', False)
        db_set_topic_mode(ig_username, new_mode)
        await query.message.reply_text(f"Topic mode for {ig_username} has been {'enabled' if new_mode else 'disabled'}.")
        await manage_account_menu(update, context) # Refresh menu

    elif action == "cleardata":
        db_clear_seen_reels(ig_username)
        await query.message.reply_text(f"✅ Seen reels data for **{ig_username}** has been cleared.")
        await manage_account_menu(update, context)

    elif action == "remove":
        keyboard = [
            [
                InlineKeyboardButton("YES, REMOVE IT", callback_data=f"confirmremove_{ig_username}"),
                InlineKeyboardButton("NO, CANCEL", callback_data=f"manage_{ig_username}")
            ]
        ]
        await query.edit_message_text(f"⚠️ Are you sure you want to remove **{ig_username}**? This cannot be undone.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    elif action == "confirmremove":
        db_remove_account(update.effective_user.id, ig_username)
        await query.edit_message_text(f"✅ Account **{ig_username}** has been removed.")

# --- Conversation Handlers for setting values ---
async def get_target_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ig_username = context.user_data['ig_username_to_manage']
    try:
        chat_id = int(update.message.text)
        db_set_target_chat(ig_username, chat_id)
        await update.message.reply_text(f"✅ Target chat for **{ig_username}** set to `{chat_id}`.")
    except ValueError:
        await update.message.reply_text("Invalid ID. Please provide a numerical chat ID.")
    del context.user_data['ig_username_to_manage']
    return ConversationHandler.END

async def get_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ig_username = context.user_data['ig_username_to_manage']
    try:
        interval = int(update.message.text)
        if interval < 5:
            await update.message.reply_text("Interval must be at least 5 minutes.")
            return AWAIT_INTERVAL
        db_set_periodic_interval(ig_username, interval)
        await update.message.reply_text(f"✅ Check interval for **{ig_username}** set to `{interval}` minutes.")
    except ValueError:
        await update.message.reply_text("Invalid number. Please provide an interval in minutes.")
    del context.user_data['ig_username_to_manage']
    return ConversationHandler.END

# ========================================================================================
# =====                          BACKGROUND MONITOR LOGIC                            =====
# ========================================================================================

async def send_log_async(bot: Bot, user_id, message: str):
    log_channel_id = db_get_log_channel(user_id)
    if log_channel_id:
        try:
            await bot.send_message(chat_id=log_channel_id, text=message)
        except Exception as e:
            logger.error(f"Async Log Error: {e}")

async def monitor_and_upload(bot: Bot):
    while True:
        logger.info("Starting new Instagram check cycle...")
        accounts_to_check = db_get_all_accounts()
        
        for account in accounts_to_check:
            try:
                # Check if it's time to run for this account
                last_check = account.get('last_check', datetime.min)
                interval = timedelta(minutes=account.get('interval_minutes', 60))
                if datetime.now() - last_check < interval:
                    continue

                owner_id = account['owner_id']
                ig_username = account['ig_username']
                session_id = account['ig_session_id']
                target_chat_id = account.get('target_chat_id')
                topic_mode = account.get('topic_mode', False)
                
                if not target_chat_id:
                    continue

                await send_log_async(bot, owner_id, f"🔍 Checking DMs for {ig_username}...")
                ig_client = InstagramClient(ig_username, session_id)
                new_reels = ig_client.get_new_reels_from_dms()
                db_update_last_check(ig_username)
                
                if not new_reels:
                    await send_log_async(bot, owner_id, f"✅ No new reels found for {ig_username}.")
                    continue

                await send_log_async(bot, owner_id, f"Found {len(new_reels)} new reel(s) for {ig_username}.")
                for reel in new_reels:
                    filepath = ig_client.download_reel(reel['reel_pk'])
                    caption = f"🎬 Reel from **{reel['from_user']}**\n" \
                              f"💬 In chat: *{reel['ig_chat_name']}*\n\n"
                    
                    message_thread_id = None
                    if topic_mode:
                        try:
                            topic_name = reel['ig_chat_name']
                            topic_id = db_get_or_create_topic(target_chat_id, topic_name)
                            if topic_id:
                                message_thread_id = topic_id
                            else: # Create topic on the fly
                                new_topic = await bot.create_forum_topic(chat_id=target_chat_id, name=topic_name)
                                message_thread_id = new_topic.message_thread_id
                                db_save_topic(target_chat_id, topic_name, message_thread_id, ig_username)
                        except Exception as e:
                            logger.error(f"Could not manage topic for {ig_username}: {e}")
                            await send_log_async(bot, owner_id, f"⚠️ Could not create/find topic for '{reel['ig_chat_name']}'. Sending to main group.")


                    with open(filepath, 'rb') as video_file:
                        await bot.send_video(
                            chat_id=target_chat_id,
                            video=video_file,
                            caption=caption,
                            parse_mode='Markdown',
                            message_thread_id=message_thread_id
                        )
                    
                    db_add_seen_reel(ig_username, reel['reel_pk'], reel['message_id'])
                    os.remove(filepath)
                    await send_log_async(bot, owner_id, f"✅ Sent reel from {reel['from_user']} to chat {target_chat_id}.")
            
            except Exception as e:
                logger.error(f"CRITICAL ERROR processing {account.get('ig_username', 'N/A')}: {e}")
                if 'owner_id' in account:
                    await send_log_async(bot, account['owner_id'], f"❌ ERROR for {account.get('ig_username', 'N/A')}: {e}")
        
        await asyncio.sleep(60) # Check every minute to see if any account's interval is up

# ========================================================================================
# =====                             MAIN APPLICATION SETUP                           =====
# ========================================================================================

def main():
    # --- Start the Web Server (for Render) ---
    web_thread = threading.Thread(target=lambda: http.server.HTTPServer(("", int(os.environ.get("PORT", 8080))), http.server.SimpleHTTPRequestHandler).serve_forever())
    web_thread.daemon = True
    web_thread.start()
    logger.info(f"Web server started on port {os.environ.get('PORT', 8080)}.")

    # --- Setup Telegram Bot ---
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Conversation Handlers
    add_account_handler = ConversationHandler(
        entry_points=[CommandHandler("addaccount", add_account_start)],
        states={AWAIT_SESSION_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_account_get_session_id)]},
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    )
    manage_account_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_callback_handler, pattern="^settarget_")],
        states={AWAIT_TARGET_CHAT_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_target_chat_id)]},
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    )
    set_interval_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_callback_handler, pattern="^setinterval_")],
        states={AWAIT_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_interval)]},
        fallbacks=[CommandHandler("cancel", cancel_conversation)]
    )
    
    # Command Handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", start_command))
    application.add_handler(CommandHandler("ping", ping_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("restart", restart_command))
    application.add_handler(CommandHandler("myaccounts", my_accounts_command))
    application.add_handler(CommandHandler("logc", log_channel_command))
    
    # Add Conversation Handlers
    application.add_handler(add_account_handler)
    application.add_handler(manage_account_handler)
    application.add_handler(set_interval_handler)

    # Callback Query Handlers (for menus)
    application.add_handler(CallbackQueryHandler(manage_account_menu, pattern="^manage_"))
    application.add_handler(CallbackQueryHandler(my_accounts_command, pattern="^myaccounts$"))
    application.add_handler(CallbackQueryHandler(button_callback_handler, pattern="^toggletopic_"))
    application.add_handler(CallbackQueryHandler(button_callback_handler, pattern="^cleardata_"))
    application.add_handler(CallbackQueryHandler(button_callback_handler, pattern="^remove_"))
    application.add_handler(CallbackQueryHandler(button_callback_handler, pattern="^confirmremove_"))

    # --- Start Background Monitor ---
    async def run_monitor():
        await monitor_and_upload(application.bot)

    monitor_thread = threading.Thread(target=lambda: asyncio.run(run_monitor()))
    monitor_thread.daemon = True
    monitor_thread.start()
    logger.info("Instagram monitoring loop started in a background thread.")

    # --- Run Bot ---
    logger.info("Telegram bot is now polling for updates...")
    application.run_polling()

if __name__ == "__main__":
    main()
