# Channel: DIPAK_KUMAR_GUPTA

import os
import shutil
import asyncio
from time import time

import psutil
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
from pyrogram import Client, filters
from pyrogram.errors import PeerIdInvalid, BadRequest, MessageNotModified
from pyleaves import Leaves
from PIL import Image

from helpers.utils import (
    getChatMsgID,
    processMediaGroup,
    get_parsed_msg,
    fileSizeLimit,
    progressArgs,
    send_media,
    get_readable_file_size,
    get_readable_time,
    get_media_info,
    get_video_thumbnail,
)

from config import PyroConf
from logger import LOGGER

# Dictionary to track active download tasks and stop requests
# Key: chat_id, Value: boolean (True if stop requested)
download_tasks = {}

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


@bot.on_message(filters.command("start") & filters.private)
async def start(_, message: Message):
    welcome_text = (
        "**👋 Welcome to the Media Downloader Bot!**\n\n"
        "This bot helps you download media from Restricted channel\n"
        "Use /help for more information on how to use this bot.\n\n"
        "Bot by: @DIPAK_KUMAR_GUPTA"
    )
    await message.reply(welcome_text)


@bot.on_message(filters.command("help") & filters.private)
async def help_command(_, message: Message):
    help_text = (
        "💡 **How to Use the Bot**\n\n"
        "1. Send the command `/dl post URL` to download media from a specific message.\n"
        "2. Send `/dl post_URL start_msg_id end_msg_id` to download a range of messages.\n"
        "3. The bot will download the media (photos, videos, audio, or documents) also can copy message.\n"
        "4. Make sure the bot and the user client are part of the chat to download the media.\n\n"
        "**Example (Single)**: `/dl https://t.me/some_channel/547`\n"
        "**Example (Range)**: `/dl https://t.me/c/123456789/100 200`\n\n"
        "**Forward to Channel**: `/dl https://t.me/c/2572510647/120 122 -1002694175455`\n"
        "Where `-1002694175455` is the channel ID to forward content to.\n\n"
        "Bot by: @DIPAK_KUMAR_GUPTA"
    )
    await message.reply(help_text)


@bot.on_message(filters.command("dl") & filters.private)
async def download_media(bot: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("**Provide a post URL after the /dl command.**")
        return

    post_url = message.command[1]
    end_message_id = None
    forward_chat_id = None
    
    # Check if a range is specified
    if len(message.command) >= 3:
        try:
            end_message_id = int(message.command[2])
        except ValueError:
            await message.reply("**Invalid end message ID. Please provide a valid number.**")
            return
    
    # Check if a forward channel ID is specified
    if len(message.command) >= 4:
        try:
            forward_chat_id = int(message.command[3])
            LOGGER(__name__).info(f"Content will be forwarded to channel/group ID: {forward_chat_id}")
        except ValueError:
            await message.reply("**Invalid channel ID. Please provide a valid number.**")
            return

    try:
        chat_id, start_message_id = getChatMsgID(post_url)
        
        # If no range specified, download single message
        if end_message_id is None:
            await download_single_message(bot, message, user, chat_id, start_message_id, forward_chat_id)
        else:
            # Download range of messages
            await download_message_range(bot, message, user, chat_id, start_message_id, end_message_id, forward_chat_id)
            
    except (PeerIdInvalid, BadRequest, KeyError):
        await message.reply("**Make sure the user client is part of the chat.**")
    except ValueError as ve:
        await message.reply(f"**Error parsing link or IDs: {ve}**")
    except Exception as e:
        error_message = f"**❌ An unexpected error occurred: {str(e)}**"
        await message.reply(error_message)
        LOGGER(__name__).error(f"Unexpected error in /dl: {e}", exc_info=True)

async def download_single_message(bot: Client, message: Message, user: Client, chat_id, message_id, forward_chat_id=None):
    try:
        chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
        if not chat_message:
            notice_msg = await message.reply(f"**Message with ID {message_id} not found.**")
            await asyncio.sleep(5) # Keep notice for 5 seconds
            await notice_msg.delete()
            return False
            
        LOGGER(__name__).info(f"Downloading media from message ID: {message_id}")
        
        return await process_message(bot, message, user, chat_message, forward_chat_id)
    except Exception as e:
        LOGGER(__name__).error(f"Error downloading single message {message_id}: {str(e)}", exc_info=True)
        await message.reply(f"**Error processing message {message_id}: {str(e)}**")
        return False

async def download_message_range(bot: Client, message: Message, user: Client, chat_id, start_id, end_id, forward_chat_id=None):
    if start_id > end_id:
        await message.reply("**Start message ID must be less than or equal to end message ID.**")
        return
        
    task_id = message.chat.id # Use chat_id as a simple task identifier
    download_tasks[task_id] = False # False means not stopped
    
    stop_button = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Stop ⏹️", callback_data=f"stop_{task_id}_{start_id}_{end_id}")
        ]]
    )
    
    status_message = await message.reply(
        f"**📥 Downloading messages from {start_id} to {end_id}...**",
        reply_markup=stop_button
    )
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    start_time = time()
    
    try:
        for msg_id in range(start_id, end_id + 1):
            # Check if stop requested
            if download_tasks.get(task_id):
                LOGGER(__name__).info(f"Stop requested for task {task_id}. Aborting download.")
                await status_message.edit(
                    f"**⏹️ Download stopped by user.**\n"
                    f"**Range: {start_id} to {end_id}**\n"
                    f"**Processed: {msg_id - start_id} | Success: {success_count} | Failed: {failed_count} | Skipped: {skipped_count}**"
                )
                break # Exit the loop
                
            try:
                chat_message = await user.get_messages(chat_id=chat_id, message_ids=msg_id)
                
                if not chat_message:
                    LOGGER(__name__).info(f"Message {msg_id} not found, skipping.")
                    skipped_count += 1
                    continue
                    
                LOGGER(__name__).info(f"Processing message ID: {msg_id}")
                
                # Update status periodically
                if (msg_id - start_id) % 5 == 0 or msg_id == start_id:
                    elapsed_time = time() - start_time
                    speed = (msg_id - start_id + 1) / elapsed_time if elapsed_time > 0 else 0
                    try:
                        await status_message.edit(
                            f"**📥 Downloading messages {start_id} to {end_id}...**\n"
                            f"**Current: {msg_id}/{end_id}**\n"
                            f"**Success: {success_count} | Failed: {failed_count} | Skipped: {skipped_count}**\n"
                            f"**Speed: {speed:.2f} msg/s**",
                            reply_markup=stop_button
                        )
                    except MessageNotModified: # Ignore if the message content is the same
                        pass
                    except Exception as edit_err:
                        LOGGER(__name__).warning(f"Failed to edit status message: {edit_err}")
                
                # Pass the task_id to process_message if needed for finer control (optional)
                result = await process_message(bot, message, user, chat_message, forward_chat_id)
                if result:
                    success_count += 1
                else:
                    # If process_message returns False, it might mean skipped or failed internally
                    # We rely on its logging for specifics, but increment failed_count here for summary
                    failed_count += 1 # Assume failure if not explicitly success
                    
            except Exception as e:
                LOGGER(__name__).error(f"Error processing message {msg_id} in range: {str(e)}", exc_info=True)
                failed_count += 1
                # Optionally send a notification about the specific error
                # await message.reply(f"**Error processing message {msg_id}: {str(e)}**")
                continue # Continue to the next message
        
        # Final status update only if the loop wasn't stopped by user
        if not download_tasks.get(task_id):
            await status_message.edit(
                f"**✅ Download completed for messages {start_id} to {end_id}**\n"
                f"**Success: {success_count} | Failed: {failed_count} | Skipped: {skipped_count}**"
            )
            
    finally:
        # Clean up the task entry
        if task_id in download_tasks:
            del download_tasks[task_id]

async def process_message(bot: Client, message: Message, user: Client, chat_message, forward_chat_id=None):
    try:
        if chat_message.document or chat_message.video or chat_message.audio:
            file_size = (
                chat_message.document.file_size
                if chat_message.document
                else chat_message.video.file_size
                if chat_message.video
                else chat_message.audio.file_size
            )

            if not await fileSizeLimit(
                file_size, message, "download", user.me.is_premium
            ):
                return False # Indicate failure/skip due to size limit

        parsed_caption = await get_parsed_msg(
            chat_message.caption or "", chat_message.caption_entities
        )
        parsed_text = await get_parsed_msg(
            chat_message.text or "", chat_message.entities
        )

        # Determine the target chat ID for sending content
        target_chat_id = forward_chat_id if forward_chat_id else message.chat.id
        
        if forward_chat_id:
            LOGGER(__name__).info(f"Forwarding content to channel/group ID: {forward_chat_id}")
            # Send a notification to the user that content is being forwarded
            # Consider making this optional or less frequent to avoid spam
            # await message.reply(f"**Forwarding content to channel/group ID: {forward_chat_id}**")

        if chat_message.media_group_id:
            # Pass target_chat_id to processMediaGroup
            if not await processMediaGroup(chat_message, bot, message, target_chat_id):
                notice_msg = await message.reply(
                    "**Could not extract any valid media from the media group.**"
                )
                await asyncio.sleep(5)
                await notice_msg.delete()
                return False # Indicate failure/skip
            return True # Indicate success

        elif chat_message.media:
            start_time = time()
            # Note: Progress message here is for individual file download/upload, not the range status
            progress_message = await message.reply("**📥 Downloading File...**") 

            media_path = None # Initialize media_path
            try:
                media_path = await chat_message.download(
                    progress=Leaves.progress_for_pyrogram,
                    progress_args=progressArgs(
                        "📥 Downloading File", progress_message, start_time
                    ),
                )
                LOGGER(__name__).info(f"Downloaded media: {media_path}")
            except Exception as download_err:
                LOGGER(__name__).error(f"Failed to download media: {download_err}", exc_info=True)
                await progress_message.edit(f"**❌ Failed to download file: {download_err}**")
                await asyncio.sleep(5)
                await progress_message.delete()
                if media_path and os.path.exists(media_path):
                    os.remove(media_path)
                return False # Indicate failure

            media_type = (
                "photo"
                if chat_message.photo
                else "video"
                if chat_message.video
                else "audio"
                if chat_message.audio
                else "document"
            )
            
            upload_success = False
            try:
                # Send media to the target chat ID
                if media_type == "photo":
                    await bot.send_photo(
                        chat_id=target_chat_id,
                        photo=media_path,
                        caption=parsed_caption or "",
                        progress=Leaves.progress_for_pyrogram,
                        progress_args=progressArgs(
                            "📤 Uploading Photo", progress_message, start_time
                        ),
                    )
                elif media_type == "video":
                    thumb_path = os.path.join("Assets", "video_thumb.jpg")
                    if os.path.exists(thumb_path):
                        try: os.remove(thumb_path)
                        except: pass
                    
                    duration = (await get_media_info(media_path))[0]
                    thumb = await get_video_thumbnail(media_path, duration)
                    
                    width = chat_message.video.width
                    height = chat_message.video.height
                    
                    if (not width or not height) and thumb is not None and thumb != "none":
                        try:
                            with Image.open(thumb) as img:
                                width, height = img.size
                        except Exception as img_err:
                             LOGGER(__name__).warning(f"Could not read thumb dimensions: {img_err}")
                    if not width: width = 640
                    if not height: height = 360
                    if thumb == "none": thumb = None

                    await bot.send_video(
                        chat_id=target_chat_id,
                        video=media_path,
                        duration=duration,
                        width=width,
                        height=height,
                        thumb=thumb,
                        caption=parsed_caption or "",
                        progress=Leaves.progress_for_pyrogram,
                        progress_args=progressArgs(
                            "📤 Uploading Video", progress_message, start_time
                        ),
                    )
                    if thumb and os.path.exists(thumb):
                        try: os.remove(thumb)
                        except: pass
                elif media_type == "audio":
                    duration, artist, title = await get_media_info(media_path)
                    await bot.send_audio(
                        chat_id=target_chat_id,
                        audio=media_path,
                        duration=duration,
                        performer=artist,
                        title=title,
                        caption=parsed_caption or "",
                        progress=Leaves.progress_for_pyrogram,
                        progress_args=progressArgs(
                            "📤 Uploading Audio", progress_message, start_time
                        ),
                    )
                elif media_type == "document":
                    await bot.send_document(
                        chat_id=target_chat_id,
                        document=media_path,
                        caption=parsed_caption or "",
                        progress=Leaves.progress_for_pyrogram,
                        progress_args=progressArgs(
                            "📤 Uploading Document", progress_message, start_time
                        ),
                    )
                upload_success = True
            except Exception as upload_err:
                 LOGGER(__name__).error(f"Failed to upload media: {upload_err}", exc_info=True)
                 await progress_message.edit(f"**❌ Failed to upload file: {upload_err}**")
                 await asyncio.sleep(5)
            finally:
                if media_path and os.path.exists(media_path):
                    os.remove(media_path)
                await progress_message.delete()
                
            return upload_success # Return True if upload succeeded, False otherwise

        elif chat_message.text or chat_message.caption:
            try:
                await bot.send_message(
                    chat_id=target_chat_id,
                    text=parsed_text or parsed_caption
                )
                return True # Indicate success
            except Exception as send_err:
                LOGGER(__name__).error(f"Failed to send text message: {send_err}", exc_info=True)
                return False # Indicate failure
        else:
            # This is the message to auto-delete
            notice_message = await message.reply("**No media or text found in the message.**")
            LOGGER(__name__).info(f"No media/text found for message ID {chat_message.id} in chat {chat_id}. Sending notice.")
            await asyncio.sleep(5) # Wait 5 seconds
            try:
                await notice_message.delete()
            except Exception as delete_err:
                LOGGER(__name__).warning(f"Could not delete notice message: {delete_err}")
            return False # Indicate skipped/no content

    except Exception as e:
        LOGGER(__name__).error(f"Error processing message {chat_message.id if chat_message else 'N/A'}: {str(e)}", exc_info=True)
        try:
            # Try to inform the user about the error
            error_notice = await message.reply(f"**❌ Error processing message: {str(e)}**")
            await asyncio.sleep(10) # Keep error message a bit longer
            await error_notice.delete()
        except Exception as notice_err:
            LOGGER(__name__).error(f"Failed to send/delete error notice: {notice_err}")
        return False # Indicate failure


@bot.on_callback_query(filters.regex("^stop_"))
async def stop_download_callback(client: Client, query: CallbackQuery):
    try:
        _, task_id_str, start_id_str, end_id_str = query.data.split("_")
        task_id = int(task_id_str)
        
        if task_id in download_tasks:
            download_tasks[task_id] = True # Set flag to stop
            LOGGER(__name__).info(f"Stop signal received for task {task_id}")
            await query.answer("Stopping download process...", show_alert=False)
            # Optionally edit the original message immediately
            try:
                await query.message.edit_text(
                     f"**⏹️ Stopping download ({start_id_str}-{end_id_str})... Please wait.**"
                )
            except MessageNotModified:
                pass
            except Exception as edit_err:
                 LOGGER(__name__).warning(f"Failed to edit message on stop signal: {edit_err}")
        else:
            await query.answer("This download task is no longer active or invalid.", show_alert=True)
            # Remove the button if the task is already finished/gone
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception as edit_err:
                 LOGGER(__name__).warning(f"Failed to remove markup from inactive task message: {edit_err}")
                 
    except Exception as e:
        LOGGER(__name__).error(f"Error in stop callback: {e}", exc_info=True)
        await query.answer("Error processing stop request.", show_alert=True)


@bot.on_message(filters.command("stats") & filters.private)
async def stats(_, message: Message):
    currentTime = get_readable_time(time() - PyroConf.BOT_START_TIME)
    try:
        total, used, free = shutil.disk_usage(".")
        total = get_readable_file_size(total)
        used = get_readable_file_size(used)
        free = get_readable_file_size(free)
    except Exception as disk_err:
        total, used, free = "N/A", "N/A", "N/A"
        LOGGER(__name__).error(f"Could not get disk usage: {disk_err}")
        
    try:
        sent = get_readable_file_size(psutil.net_io_counters().bytes_sent)
        recv = get_readable_file_size(psutil.net_io_counters().bytes_recv)
        cpuUsage = psutil.cpu_percent(interval=0.5)
        memory = psutil.virtual_memory().percent
        disk = psutil.disk_usage("/").percent
        process = psutil.Process(os.getpid())
        mem_usage = f"{round(process.memory_info()[0] / 1024**2)} MiB"
    except Exception as ps_err:
        sent, recv, cpuUsage, memory, disk, mem_usage = "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"
        LOGGER(__name__).error(f"Could not get psutil stats: {ps_err}")

    stats = (
        "**📊 Bot Status**\n\n"
        f"**➜ Bot Uptime:** `{currentTime}`\n"
        f"**➜ CPU:** `{cpuUsage}%` | **RAM:** `{memory}%` | **DISK:** `{disk}%`\n"
        f"**➜ Memory Usage:** `{mem_usage}`\n\n"
        f"**➜ Total Disk:** `{total}`\n"
        f"**➜ Used:** `{used}` | **Free:** `{free}`\n\n"
        f"**➜ Upload:** `{sent}`\n"
        f"**➜ Download:** `{recv}`\n\n"
        "Bot by: @DIPAK_KUMAR_GUPTA"
    )
    await message.reply(stats)


@bot.on_message(filters.command("logs") & filters.private)
async def logs(_, message: Message):
    log_file = "logs.txt"
    if os.path.exists(log_file):
        try:
            await message.reply_document(document=log_file, caption="**Bot Logs**")
        except Exception as e:
            await message.reply(f"**Error sending logs: {e}**")
            LOGGER(__name__).error(f"Failed to send log file: {e}")
    else:
        await message.reply("**Log file not found.**")


if __name__ == "__main__":
    try:
        # Ensure Assets directory exists
        os.makedirs("Assets", exist_ok=True)
        
        # Simple Flask app for health checks (optional, can be removed if not needed)
        # from flask import Flask, jsonify
        # import threading
        # app = Flask(__name__)
        # @app.route('/')
        # def index():
        #     return jsonify({"status": "running"})
        # port = int(os.environ.get("PORT", 8000))
        # def run_flask():
        #     LOGGER(__name__).info(f"Starting web server on port {port}")
        #     app.run(host="0.0.0.0", port=port)
        # flask_thread = threading.Thread(target=run_flask)
        # flask_thread.daemon = True
        # flask_thread.start()
        
        # Start the bot
        LOGGER(__name__).info("Starting User Client...")
        user.start()
        LOGGER(__name__).info("User Client Started!")
        LOGGER(__name__).info("Starting Bot...")
        bot.run()
        
    except KeyboardInterrupt:
        LOGGER(__name__).info("Bot stopped by user (KeyboardInterrupt)")
    except Exception as err:
        LOGGER(__name__).error(f"Bot crashed: {err}", exc_info=True)
    finally:
        LOGGER(__name__).info("Stopping User Client...")
        if user.is_connected:
            user.stop()
        LOGGER(__name__).info("Bot Stopped")

