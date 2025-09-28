import os, json, time, random, sys, datetime, ast
from dotenv import load_dotenv
from instagrapi import Client
from instagrapi.exceptions import LoginRequired, PrivateError
import telegram
import asyncio
import threading
import http.server
import socketserver

load_dotenv()
username = os.environ.get("IG_USERNAME")
login_only = ast.literal_eval(os.environ.get("LOGIN_ONLY", "False"))


def start_web_server():
    PORT = int(os.environ.get("PORT", 8080))
    Handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"[{get_now()}] Web server started on port {PORT}")
        httpd.serve_forever()


def authenticate(client, session_file):
    if os.path.exists(session_file):
        try:
            client.load_settings(session_file)
            # Use a simple, non-aggressive check to validate the session
            client.user_info_by_username(username)
            print(f"[{get_now()}] Session file is valid and loaded.")
        except Exception as e:
            print(f"[{get_now()}] Session file is invalid: {e}. Re-logging in via session ID.")
            os.remove(session_file)
            # Fallback to session_id login if the session file is broken
            session_id = os.environ.get("IG_SESSION_ID")
            if not session_id:
                raise ValueError("Session is invalid and IG_SESSION_ID is not set.")
            client.login_by_sessionid(session_id)
            client.dump_settings(session_file)
    else:
        # If no session file, create one using the sessionid from your .env
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
    return datetime.datetime.now().strftime("%Y-m-d %H:%M:%S")


def sleep_countdown(duration_minutes=5):
    sleep_time = duration_minutes * 60
    print(f"[{get_now()}] Cooling down for {duration_minutes} minutes.")
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
    print(f"[{get_now()}] Loaded seen messages. Starting main loop.")

    while True:
        try:
            threads = cl.direct_threads(amount=20)
            for thread in threads:
                messages = cl.direct_messages(thread.id, amount=20)
                for message in messages:
                    if message.id not in seen_message_ids:
                        if message.item_type == "clip":
                            print(f"[{get_now()}] New reel found: {message.clip.pk}")
                            download_clip(cl, message.clip.pk)
                        
                        seen_message_ids.add(message.id)
                        save_seen_messages(seen_messages_file, seen_message_ids)
            
            # Use a longer, randomized sleep after a successful run
            sleep_duration = random.randint(30, 60)
            print(f"[{get_now()}] Successful check. Sleeping for {sleep_duration} minutes.")
            sleep_countdown(sleep_duration)

        except PrivateError as e:
            # This catches JSONDecodeError and other similar issues
            print(f"[{get_now()}] Instagram API Error (PrivateError): {e}. Cooling down.")
            sleep_countdown(15) # Wait 15 minutes before retrying
        except Exception as e:
            print(f"[{get_now()}] A critical error occurred: {e}")
            if "Login required" in str(e) and os.path.exists(session_file):
                os.remove(session_file)
                print("Session file deleted. Restarting after countdown to re-authenticate.")
            sleep_countdown(10) # Wait 10 minutes before a full restart
            os.execv(sys.executable, ["python"] + sys.argv)


if __name__ == "__main__":
    main()
