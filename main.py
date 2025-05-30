

import os
import shutil
import asyncio
from time import time

import psutil
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from pyrogram import Client, filters
from pyrogram.errors import PeerIdInvalid, BadRequest, FloodWait
from pyleaves import Leaves
from PIL import Image

from helpers.utils import (
    getChatMsgID,
    processMediaGroup,
    get_parsed_msg,
    fileSizeLimit,
    progressArgs,
    # send_media, # This helper seems unused, removing import
    get_readable_file_size,
    get_readable_time,
    get_media_info,
    get_video_thumbnail,
)

from config import PyroConf
from logger import LOGGER

# Initialize the bot client
bot = Client(
    "media_bot",
    api_id=PyroConf.API_ID,
    api_hash=PyroConf.API_HASH,
    bot_token=PyroConf.BOT_TOKEN,
    workers=1000,
    parse_mode=ParseMode.MARKDOWN,
)

# Client for user session
user = Client("user_session", workers=1000, session_string=PyroConf.SESSION_STRING)

# Dictionary to track ongoing tasks per user
ongoing_tasks = {}  # Key: user_id, Value: {"cancel": False, "message": status_message_object (optional), "flood_stop": False}


@bot.on_message(filters.command("start") & filters.private)
async def start(_, message: Message):
    welcome_text = (
        "**👋 Welcome to the Media Downloader Bot!**\n\n"
        "This bot helps you download media from Restricted channel\n"
        "Use /help for more information on how to use this bot."
    )
    await message.reply(welcome_text)


@bot.on_message(filters.command("help") & filters.private)
async def help_command(_, message: Message):
    help_text = (
        "💡 **How to Use the Bot**\n\n"
        "1. Send the command `/dl post URL` to download media from a specific message.\n"
        "2. Send the command `/dl post_URL start_ID end_ID` to download a range of messages.\n"
        "3. Add a channel ID at the end to forward content: `/dl post_URL [end_ID] channel_ID`\n"
        "4. Use `/cancel` to stop any ongoing download/forwarding task initiated by you.\n"
        "5. The bot will download the media (photos, videos, audio, or documents) or copy messages.\n"
        "6. Make sure the bot and the user client are part of the source chat to download the media.\n"
        "7. **Flood Errors:** If the bot encounters repeated Telegram flood limits, the task will be automatically stopped.\n\n"
        "**Example (Single Post)**: `/dl https://t.me/itsSmartDev/547`\n"
        "**Example (Range)**: `/dl https://t.me/c/2572510647/120 150`\n"
        "**Example (Forward to Channel)**: `/dl https://t.me/c/2572510647/120 -1002694175455`\n"
        "**Example (Range & Forward)**: `/dl https://t.me/c/2572510647/120 150 -1002694175455`"
    )
    await message.reply(help_text)


@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_command(_, message: Message):
    user_id = message.from_user.id
    if user_id in ongoing_tasks and not ongoing_tasks[user_id]["cancel"]:
        ongoing_tasks[user_id]["cancel"] = True
        await message.reply("**Attempting to cancel your ongoing task...**")
        status_msg = ongoing_tasks[user_id].get("message")
        if status_msg:
            try:
                await status_msg.edit("**⚠️ Task cancellation requested...**")
            except Exception as e:
                LOGGER(__name__).warning(f"Could not edit status message during cancel: {e}")
    else:
        await message.reply("**You have no active task to cancel.**")


async def handle_flood_wait(fw: FloodWait, user_id: int, message: Message, status_message: Message = None):
    """Handles FloodWait exceptions by notifying the user and stopping the task."""
    LOGGER(__name__).error(f"Flood wait encountered for user {user_id}: {fw}")
    wait_time = fw.value
    error_text = f"**🛑 Flood Limit Error!**\nTelegram requires a wait of {wait_time} seconds. The current task has been automatically stopped to prevent further issues. Please try again later."
    
    # Try editing status message first, then reply to original command message
    if status_message:
        try:
            await status_message.edit(error_text)
        except Exception:
            await message.reply(error_text)
    else:
        await message.reply(error_text)
        
    # Mark task for cancellation/stop
    if user_id in ongoing_tasks:
        ongoing_tasks[user_id]["cancel"] = True
        ongoing_tasks[user_id]["flood_stop"] = True # Mark specifically as flood stopped


@bot.on_message(filters.command("dl") & filters.private)
async def download_media(bot: Client, message: Message):
    user_id = message.from_user.id

    if user_id in ongoing_tasks:
        await message.reply("**You already have an ongoing task. Please wait for it to complete or use /cancel.**")
        return

    if len(message.command) < 2:
        await message.reply("**Provide a post URL after the /dl command. Use /help for details.**")
        return

    post_url = message.command[1]
    end_message_id = None
    forward_chat_id = None

    # Parse arguments: URL [End_ID] [Forward_ID]
    if len(message.command) >= 3:
        try:
            potential_id = int(message.command[2])
            if potential_id < 0:
                forward_chat_id = potential_id
            else:
                end_message_id = potential_id
        except ValueError:
            await message.reply("**Invalid End Message ID or Forward Channel ID. Please provide valid numbers.**")
            return

    if len(message.command) >= 4:
        try:
            if end_message_id is not None:
                 forward_chat_id = int(message.command[3])
            else:
                 await message.reply("**Invalid command structure. Use /help for details.**")
                 return
        except (ValueError, IndexError):
            await message.reply("**Invalid Forward Channel ID. Please provide a valid number.**")
            return

    if forward_chat_id:
         LOGGER(__name__).info(f"Content will be forwarded to channel/group ID: {forward_chat_id}")

    try:
        chat_id, start_message_id = getChatMsgID(post_url)

        # Initialize task tracking
        ongoing_tasks[user_id] = {"cancel": False, "message": None, "flood_stop": False}

        if end_message_id is None:
            await download_single_message(bot, message, user, chat_id, start_message_id, forward_chat_id, user_id)
        else:
            await download_message_range(bot, message, user, chat_id, start_message_id, end_message_id, forward_chat_id, user_id)

    except FloodWait as fw:
        # Catch flood wait during initial setup (e.g., getChatMsgID if it uses API)
        await handle_flood_wait(fw, user_id, message)
    except (PeerIdInvalid, BadRequest, KeyError):
        await message.reply("**Make sure the user client is part of the chat.**")
    except Exception as e:
        error_message = f"**❌ An error occurred: {str(e)}**"
        await message.reply(error_message)
        LOGGER(__name__).error(f"Error in /dl command for user {user_id}: {e}", exc_info=True)
    finally:
        # Clean up task tracking only if it exists
        if user_id in ongoing_tasks:
            del ongoing_tasks[user_id]

async def download_single_message(bot: Client, message: Message, user: Client, chat_id, message_id, forward_chat_id, user_id):
    if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
        LOGGER(__name__).info(f"Task cancelled by user {user_id} before processing message {message_id}")
        # Don't send another message if it was a flood stop
        if not ongoing_tasks[user_id].get("flood_stop", False):
            await message.reply("**Task cancelled.**")
        return False
        
    try:
        chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
        if not chat_message:
            await message.reply(f"**Message with ID {message_id} not found.**")
            return False

        LOGGER(__name__).info(f"Processing single message ID: {message_id} for user {user_id}")
        return await process_message(bot, message, user, chat_message, forward_chat_id, user_id)
        
    except FloodWait as fw:
        await handle_flood_wait(fw, user_id, message)
        return False # Indicate failure due to flood wait
    except Exception as e:
        LOGGER(__name__).error(f"Error downloading single message {message_id} for user {user_id}: {str(e)}")
        await message.reply(f"**Error processing message {message_id}: {str(e)}**")
        return False

async def download_message_range(bot: Client, message: Message, user: Client, chat_id, start_id, end_id, forward_chat_id, user_id):
    if start_id > end_id:
        await message.reply("**Start message ID must be less than or equal to end message ID.**")
        return

    status_message = None
    try:
        status_message = await message.reply(f"**📥 Preparing to download messages from {start_id} to {end_id}...**")
        if user_id in ongoing_tasks:
            ongoing_tasks[user_id]["message"] = status_message
    except FloodWait as fw_status:
        # Handle flood wait even when sending the initial status message
        await handle_flood_wait(fw_status, user_id, message)
        return # Stop the task immediately
    except Exception as e_status:
        LOGGER(__name__).error(f"Error sending initial status message for user {user_id}: {e_status}")
        await message.reply(f"**Error starting task: {e_status}**")
        # Ensure task is marked as stopped if it was created
        if user_id in ongoing_tasks:
             ongoing_tasks[user_id]["cancel"] = True 
        return

    success_count = 0
    failed_count = 0
    skipped_count = 0
    cancelled = False # Tracks user cancel or flood stop

    for msg_id in range(start_id, end_id + 1):
        # Check for cancellation/stop at the start of each iteration
        if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
            LOGGER(__name__).info(f"Task stopped/cancelled by user {user_id} during range processing at message {msg_id}")
            cancelled = True
            break
            
        try:
            # Fetch message
            chat_message = await user.get_messages(chat_id=chat_id, message_ids=msg_id)

            if not chat_message:
                LOGGER(__name__).info(f"Message {msg_id} not found, skipping.")
                skipped_count += 1
                await asyncio.sleep(0.5) # Shorter sleep for skipped messages
                continue

            LOGGER(__name__).info(f"Processing message ID: {msg_id} in range for user {user_id}")

            # Update status periodically
            if msg_id % 10 == 0 or msg_id == start_id: # Update less frequently
                if cancelled: break
                try:
                    await status_message.edit(
                        f"**📥 Downloading messages {start_id} to {end_id}...**\n"
                        f"**Current: {msg_id}/{end_id}**\n"
                        f"**Success: {success_count} | Failed: {failed_count} | Skipped: {skipped_count}**"
                    )
                except FloodWait as fw_edit:
                    # If editing status message gets flood waited, log it but continue the main task
                    LOGGER(__name__).warning(f"Flood wait editing status message for user {user_id}: {fw_edit}. Pausing edit.")
                    await asyncio.sleep(fw_edit.value + 2)
                except Exception as edit_err:
                    LOGGER(__name__).warning(f"Could not edit status message for user {user_id}: {edit_err}")

            # Process message
            if cancelled: break
            result = await process_message(bot, message, user, chat_message, forward_chat_id, user_id)
            
            # Check if process_message caused a flood stop
            if user_id in ongoing_tasks and ongoing_tasks[user_id].get("flood_stop", False):
                 LOGGER(__name__).info(f"Flood stop detected after process_message for user {user_id} at msg {msg_id}")
                 cancelled = True
                 break # Exit loop immediately after flood stop
                 
            if result:
                success_count += 1
            else:
                # If process_message returned False but wasn't a flood stop, count as failed/skipped
                failed_count += 1
            
            # Check cancellation status again before sleeping
            if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
                cancelled = True
                break
                
            await asyncio.sleep(PyroConf.SLEEP_TIMER) # Use configured sleep timer

        except FloodWait as fw:
            await handle_flood_wait(fw, user_id, message, status_message)
            cancelled = True # Mark as cancelled to stop the loop
            break # Exit loop immediately
        except Exception as e:
            LOGGER(__name__).error(f"Error processing message {msg_id} in range for user {user_id}: {str(e)}")
            failed_count += 1
            await asyncio.sleep(2) # Short sleep on general error
            continue # Continue to next message if possible

    # Final status update
    if status_message:
        is_flood_stop = ongoing_tasks.get(user_id, {}).get("flood_stop", False)
        final_prefix = "🛑 Task Stopped (Flood Error)" if is_flood_stop else ("⚠️ Task Cancelled" if cancelled else "✅ Task Completed")
        final_text = f"**{final_prefix} for messages {start_id} to {end_id}**\n"
        final_text += f"**Success: {success_count} | Failed: {failed_count} | Skipped: {skipped_count}**"
        try:
            await status_message.edit(final_text)
        except Exception as final_edit_err:
             LOGGER(__name__).warning(f"Could not edit final status message for user {user_id}: {final_edit_err}")
             # Send as new message if editing failed, but avoid double notification on flood stop
             if not is_flood_stop:
                 await message.reply(final_text)
    elif cancelled:
         # If status message failed initially but task was cancelled later
         is_flood_stop = ongoing_tasks.get(user_id, {}).get("flood_stop", False)
         if not is_flood_stop:
              await message.reply(f"**⚠️ Task Cancelled for messages {start_id} to {end_id}**\n**Success: {success_count} | Failed: {failed_count} | Skipped: {skipped_count}**")

async def process_message(bot: Client, message: Message, user: Client, chat_message, forward_chat_id, user_id):
    media_path = None
    thumb_path = None
    progress_message = None
    
    try:
        # Check for cancellation before processing
        if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
            LOGGER(__name__).info(f"Task cancelled by user {user_id} before processing message {chat_message.id}")
            return False

        if chat_message.document or chat_message.video or chat_message.audio:
            file_size = (
                chat_message.document.file_size if chat_message.document else
                chat_message.video.file_size if chat_message.video else
                chat_message.audio.file_size
            )
            if not await fileSizeLimit(file_size, message, "download", user.me.is_premium):
                return False

        parsed_caption = await get_parsed_msg(chat_message.caption or "", chat_message.caption_entities)
        parsed_text = await get_parsed_msg(chat_message.text or "", chat_message.entities)
        target_chat_id = forward_chat_id if forward_chat_id else message.chat.id

        # --- Media Group Processing --- 
        if chat_message.media_group_id:
            if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
                return False
            LOGGER(__name__).info(f"Processing media group: {chat_message.media_group_id} for user {user_id}")
            # Ensure processMediaGroup handles FloodWait and cancellation internally
            if not await processMediaGroup(chat_message, bot, message, target_chat_id, user_id, ongoing_tasks):
                 # Check if failure was due to flood stop
                 if user_id in ongoing_tasks and ongoing_tasks[user_id].get("flood_stop", False):
                     return False # Already handled
                 await message.reply("**Could not process the media group (possibly cancelled or failed).**")
                 return False
            return True

        # --- Single Media Processing --- 
        elif chat_message.media:
            if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
                return False
                
            start_time = time()
            try:
                progress_message = await message.reply("**📥 Preparing Download...**") 
            except FloodWait as fw_prog:
                 await handle_flood_wait(fw_prog, user_id, message)
                 return False # Stop task
            except Exception as e_prog:
                 LOGGER(__name__).error(f"Error sending progress message for user {user_id}: {e_prog}")
                 await message.reply(f"**Error starting download: {e_prog}**")
                 if user_id in ongoing_tasks: ongoing_tasks[user_id]["cancel"] = True
                 return False # Stop task

            try:
                 media_path = await chat_message.download(
                    progress=Leaves.progress_for_pyrogram,
                    progress_args=progressArgs("📥 Downloading", progress_message, start_time)
                 )
            except FloodWait as fw_dl:
                 await handle_flood_wait(fw_dl, user_id, message, progress_message)
                 return False # Stop task
            except Exception as download_err:
                 LOGGER(__name__).error(f"Error during media download for message {chat_message.id} user {user_id}: {download_err}")
                 try: await progress_message.edit(f"**❌ Download Failed: {download_err}**")
                 except Exception: pass
                 return False

            # Check cancellation after download
            if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
                LOGGER(__name__).info(f"Task cancelled by user {user_id} after downloading message {chat_message.id}")
                try: await progress_message.edit("**Task Cancelled after download.**")
                except Exception: pass
                # Cleanup handled in finally block
                return False

            LOGGER(__name__).info(f"Downloaded media: {media_path}")
            try: await progress_message.edit("**📤 Preparing Upload...**")
            except Exception: pass

            media_type = (
                "photo" if chat_message.photo else
                "video" if chat_message.video else
                "audio" if chat_message.audio else
                "document"
            )

            # Send media
            thumb = None
            try:
                if media_type == "photo":
                    await bot.send_photo(chat_id=target_chat_id, photo=media_path, caption=parsed_caption or "",
                                         progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
                elif media_type == "video":
                    thumb_path = "Assets/video_thumb.jpg" # Define here for finally block
                    if os.path.exists(thumb_path):
                        try: os.remove(thumb_path)
                        except OSError: pass
                    duration = (await get_media_info(media_path))[0]
                    thumb = await get_video_thumbnail(media_path, duration)
                    width, height = chat_message.video.width, chat_message.video.height
                    if (not width or not height) and thumb and thumb != "none":
                        try:
                            with Image.open(thumb) as img: width, height = img.size
                        except Exception as img_err: LOGGER(__name__).warning(f"Could not read thumb dimensions: {img_err}")
                    if not width: width = 640
                    if not height: height = 360
                    if thumb == "none": thumb = None

                    await bot.send_video(chat_id=target_chat_id, video=media_path, duration=duration, width=width, height=height, thumb=thumb, caption=parsed_caption or "",
                                         progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
                elif media_type == "audio":
                    duration, artist, title = await get_media_info(media_path)
                    await bot.send_audio(chat_id=target_chat_id, audio=media_path, duration=duration, performer=artist, title=title, caption=parsed_caption or "",
                                         progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
                elif media_type == "document":
                    await bot.send_document(chat_id=target_chat_id, document=media_path, caption=parsed_caption or "",
                                            progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
            except FloodWait as fw_send:
                 await handle_flood_wait(fw_send, user_id, message, progress_message)
                 # Cleanup handled in finally block
                 return False # Stop task
            except Exception as send_err:
                 LOGGER(__name__).error(f"Error sending media for message {chat_message.id} user {user_id}: {send_err}")
                 try: await progress_message.edit(f"**❌ Upload Failed: {send_err}**")
                 except Exception: pass
                 # Cleanup handled in finally block
                 return False # Indicate failure

            try: await progress_message.delete()
            except Exception: pass
            progress_message = None # Prevent deletion in finally
            return True # Success for single media

        # --- Text Message Processing --- 
        elif chat_message.text or chat_message.caption:
            if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
                return False
            try:
                await bot.send_message(chat_id=target_chat_id, text=parsed_text or parsed_caption)
                return True # Success for text message
            except FloodWait as fw_text:
                 await handle_flood_wait(fw_text, user_id, message)
                 return False # Stop task
            except Exception as text_err:
                LOGGER(__name__).error(f"Error sending text message {chat_message.id} user {user_id}: {text_err}")
                await message.reply(f"**Error sending text message {chat_message.id}: {text_err}**")
                return False # Indicate failure
        else:
            LOGGER(__name__).info(f"Message {chat_message.id} has no downloadable/forwardable content for user {user_id}.")
            return False # Indicate skipped/nothing to process

    except FloodWait as fw_outer:
        # Catch flood waits happening outside specific blocks within process_message
        await handle_flood_wait(fw_outer, user_id, message, progress_message)
        return False # Stop task
    except Exception as e:
        LOGGER(__name__).error(f"Error processing message {chat_message.id} for user {user_id}: {str(e)}", exc_info=True)
        # Avoid double error reporting if progress_message exists
        if not progress_message:
             await message.reply(f"**Error processing message {chat_message.id}: {str(e)}**")
        else:
             try: await progress_message.edit(f"**Error processing message: {str(e)}**")
             except Exception: pass
        return False # Indicate general failure
    finally:
        # Ensure cleanup of downloaded file and thumbnail
        if media_path and os.path.exists(media_path):
            try: os.remove(media_path)
            except OSError as e: LOGGER(__name__).warning(f"Error removing media file {media_path}: {e}")
        if thumb_path and os.path.exists(thumb_path):
            try: os.remove(thumb_path)
            except OSError as e: LOGGER(__name__).warning(f"Error removing thumb file {thumb_path}: {e}")
        # Try deleting progress message if it still exists and wasn't deleted after success
        if progress_message:
             try: await progress_message.delete()
             except Exception: pass


@bot.on_message(filters.command("stats") & filters.private)
async def stats(_, message: Message):
    # ... (stats command remains the same) ...
    currentTime = get_readable_time(time() - PyroConf.BOT_START_TIME)
    try:
        total, used, free = shutil.disk_usage(".")
        total = get_readable_file_size(total)
        used = get_readable_file_size(used)
        free = get_readable_file_size(free)
    except FileNotFoundError:
        total, used, free = "N/A", "N/A", "N/A"
        
    try:
        net_io = psutil.net_io_counters()
        sent = get_readable_file_size(net_io.bytes_sent)
        recv = get_readable_file_size(net_io.bytes_recv)
    except Exception:
        sent, recv = "N/A", "N/A"
        
    try:    
        cpuUsage = psutil.cpu_percent(interval=0.5)
        memory = psutil.virtual_memory().percent
        disk = psutil.disk_usage("/").percent
        process = psutil.Process(os.getpid())
        mem_used = f"{round(process.memory_info()[0] / 1024**2)} MiB"
    except Exception:
         cpuUsage, memory, disk, mem_used = "N/A", "N/A", "N/A", "N/A"

    stats = (
        "**📊 Bot Status**\n\n"
        f"**➜ Bot Uptime:** `{currentTime}`\n"
        f"**➜ CPU:** `{cpuUsage}%` | "
        f"**➜ RAM:** `{memory}%` | "
        f"**➜ DISK:** `{disk}%`\n"
        f"**➜ Memory Usage:** `{mem_used}`\n\n"
        f"**➜ Total Disk Space:** `{total}`\n"
        f"**➜ Used:** `{used}`\n"
        f"**➜ Free:** `{free}`\n\n"
        f"**➜ Upload:** `{sent}`\n"
        f"**➜ Download:** `{recv}`"
    )
    await message.reply(stats)


@bot.on_message(filters.command("logs") & filters.private)
async def logs(_, message: Message):
    # ... (logs command remains the same) ...
    log_file = "logs.txt"
    if os.path.exists(log_file):
        try:
            await message.reply_document(document=log_file, caption="**Bot Logs**")
        except Exception as e:
             LOGGER(__name__).error(f"Failed to send logs: {e}")
             await message.reply(f"**Error sending logs: {e}**")
    else:
        await message.reply("**Log file not found.**")

# Need to modify processMediaGroup in helpers/utils.py to accept user_id and ongoing_tasks
# and implement FloodWait handling with handle_flood_wait call.

if __name__ == "__main__":
    # ... (main execution block remains largely the same) ...
    if not os.path.isdir("Assets"):
        os.makedirs("Assets")
        
    try:
        import os
        from flask import Flask, jsonify
        import threading
        
        app = Flask(__name__)
        
        @app.route("/")
        def index():
            return jsonify({"status": "running"})
        
        port = int(os.environ.get("PORT", 8000))
        
        def run_flask():
            LOGGER(__name__).info(f"Starting web server on port {port}")
            try:
                app.run(host="0.0.0.0", port=port)
            except Exception as flask_err:
                 LOGGER(__name__).error(f"Flask server failed: {flask_err}", exc_info=True)
        
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        
        LOGGER(__name__).info("Bot Starting!")
        user.start()
        PyroConf.BOT_START_TIME = time() # Record start time
        bot.run()
        
    except KeyboardInterrupt:
        LOGGER(__name__).info("Bot stopping due to KeyboardInterrupt...")
    except Exception as err:
        LOGGER(__name__).critical(f"Bot failed to start or run: {err}", exc_info=True)
    finally:
        LOGGER(__name__).info("Bot Stopped")
        ongoing_tasks.clear()

