import os, json, time, random, sys, datetime, ast
from dotenv import load_dotenv
from instagrapi import Client
from instagrapi.exceptions import LoginRequired
import telegram
import asyncio
import threading
import http.server
import socketserver

load_dotenv()
username = os.environ.get("IG_USERNAME")
# password = os.environ.get("IG_PASSWORD") # Not needed if using sessionid
login_only = ast.literal_eval(os.environ.get("LOGIN_ONLY", "False"))


def start_web_server():
    # Get the port from the environment variable Render provides
    PORT = int(os.environ.get("PORT", 8080))
    Handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"[{get_now()}] Web server started on port {PORT}")
        httpd.serve_forever()


def authenticate(client, session_file):
    if os.path.exists(session_file):
        client.load_settings(session_file)
        client.login(username, "") # Will use the session file
    else:
        session_id = os.environ.get("IG_SESSION_ID")
        if not session_id:
            raise ValueError("IG_SESSION_ID not found in .env. This is required for the first run.")
        client.login_by_sessionid(session_id)
        client.dump_settings(session_file)
        print(f"[{get_now()}] Successfully created new session file from session ID.")


def load_seen_messages(file):
    if os.path.exists(file):
        with open(file, "r") as f:
            return set(json.load(f))
    else:
        return set()


def save_seen_messages(file, messages):
    with open(file, "w") as f:
        json.dump(list(messages), f)


def get_now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sleep_countdown():
    sleep_time = random.randint(30 * 60, 60 * 60)
    print(f"[{get_now()}] Timeout duration: {sleep_time} seconds.")
    for remaining_time in range(sleep_time, 0, -1):
        sys.stdout.write(f"\r[{get_now()}] Time remaining: {remaining_time} second(s).")
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\n")


def upload_to_telegram(file_path):
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not bot_token or not chat_id:
        print(f"[{get_now()}] Telegram credentials not found. Skipping upload.")
        return

    async def send_video_async():
        bot = telegram.Bot(token=bot_token)
        with open(file_path, 'rb') as video_file:
            await bot.send_video(chat_id=chat_id, video=video_file)

    try:
        asyncio.run(send_video_async())
        print(f"[{get_now()}] Uploaded {os.path.basename(file_path)} to Telegram.")
    except Exception as e:
        print(f"[{get_now()}] An error occurred while uploading to Telegram: {e}")


def download_clip(client, clip_pk):
    print(f"[{get_now()}] Downloading reel {clip_pk}")
    cwd = os.getcwd()
    download_path = os.path.join(cwd, "download")

    if not os.path.exists(download_path):
        os.makedirs(download_path)
    
    video_path = client.video_download(clip_pk, "download")
    print(f"[{get_now()}] Downloaded {clip_pk}")
    upload_to_telegram(video_path)

    if os.path.exists(video_path):
        os.remove(video_path)
        print(f"[{get_now()}] Deleted local file: {video_path}")


def main():
    # Start the web server in a separate thread
    web_thread = threading.Thread(target=start_web_server)
    web_thread.daemon = True
    web_thread.start()

    cl = Client()
    cl.delay_range = [1, 3]

    session_file = "session.json"
    seen_messages_file = "seen_messages.json"
    authenticate(cl, session_file)

    user_id = cl.user_id_from_username(username)
    print(f"[{get_now()}] Logged in as user ID {user_id}")

    if login_only:
        print(f"[{get_now()}] LOGIN_ONLY is set to true, script ends here.")
        return

    seen_message_ids = load_seen_messages(seen_messages_file)
    print(f"[{get_now()}] Loaded seen messages.")

    while True:
        try:
            threads = cl.direct_threads()
            for thread in threads:
                messages = cl.direct_messages(thread.id)
                for message in messages:
                    if message.id not in seen_message_ids:
                        if message.item_type == "clip":
                            print(f"[{get_now()}] New reel found: {message.clip.pk}")
                            try:
                                download_clip(cl, message.clip.pk)
                            except Exception as e:
                                print(f"Error during download/upload: {e}")
                        
                        seen_message_ids.add(message.id)
                        save_seen_messages(seen_messages_file, seen_message_ids)

        except Exception as e:
            print(f"[{get_now()}] A critical error occurred: {e}")
            if "Login required" in str(e) and os.path.exists(session_file):
                os.remove(session_file)
                print("Session file deleted due to login error. Restarting...")
            sleep_countdown()
            os.execv(sys.executable, ["python"] + sys.argv)

        sleep_countdown()


if __name__ == "__main__":
    main()
