# Copyright (C) @TheSmartBisnu
# Channel: https://t.me/itsSmartDev

import os
import shutil
from time import time

import psutil
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from pyrogram import Client, filters
from pyrogram.errors import PeerIdInvalid, BadRequest
from pyleaves import Leaves

from helpers.utils import (
    getChatMsgID,
    processMediaGroup,
    get_parsed_msg,
    fileSizeLimit,
    progressArgs,
    send_media,
    get_readable_file_size,
    get_readable_time,
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
        "2. The bot will download the media (photos, videos, audio, or documents) also can copy message.\n"
        "3. Make sure the bot and the user client are part of the chat to download the media.\n\n"
        "**Example**: `/dl https://t.me/itsSmartDev/547`"
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
    except Exception as e:
        error_message = f"**❌ {str(e)}**"
        await message.reply(error_message)
        LOGGER(__name__).error(e)

async def download_single_message(bot: Client, message: Message, user: Client, chat_id, message_id, forward_chat_id=None):
    try:
        chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
        if not chat_message:
            await message.reply(f"**Message with ID {message_id} not found.**")
            return False
            
        LOGGER(__name__).info(f"Downloading media from message ID: {message_id}")
        
        return await process_message(bot, message, user, chat_message, forward_chat_id)
    except Exception as e:
        LOGGER(__name__).error(f"Error downloading message {message_id}: {str(e)}")
        return False

async def download_message_range(bot: Client, message: Message, user: Client, chat_id, start_id, end_id, forward_chat_id=None):
    if start_id > end_id:
        await message.reply("**Start message ID must be less than or equal to end message ID.**")
        return
        
    status_message = await message.reply(f"**📥 Downloading messages from {start_id} to {end_id}...**")
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for msg_id in range(start_id, end_id + 1):
        try:
            chat_message = await user.get_messages(chat_id=chat_id, message_ids=msg_id)
            
            if not chat_message:
                LOGGER(__name__).info(f"Message {msg_id} not found, skipping.")
                skipped_count += 1
                continue
                
            LOGGER(__name__).info(f"Processing message ID: {msg_id}")
            
            # Update status periodically
            if msg_id % 5 == 0 or msg_id == start_id:
                await status_message.edit(
                    f"**📥 Downloading messages {start_id} to {end_id}...**\n"
                    f"**Current: {msg_id}/{end_id}**\n"
                    f"**Success: {success_count} | Failed: {failed_count} | Skipped: {skipped_count}**"
                )
            
            result = await process_message(bot, message, user, chat_message, forward_chat_id)
            if result:
                success_count += 1
            else:
                failed_count += 1
                
        except Exception as e:
            LOGGER(__name__).error(f"Error processing message {msg_id}: {str(e)}")
            failed_count += 1
            continue
    
    # Final status update
    await status_message.edit(
        f"**✅ Download completed for messages {start_id} to {end_id}**\n"
        f"**Success: {success_count} | Failed: {failed_count} | Skipped: {skipped_count}**"
    )

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
                return False

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
            await message.reply(f"**Forwarding content to channel/group ID: {forward_chat_id}**")

        if chat_message.media_group_id:
            if not await processMediaGroup(chat_message, bot, message, forward_chat_id):
                await message.reply(
                    "**Could not extract any valid media from the media group.**"
                )
            return True

        elif chat_message.media:
            start_time = time()
            progress_message = await message.reply("**📥 Downloading Progress...**")

            media_path = await chat_message.download(
                progress=Leaves.progress_for_pyrogram,
                progress_args=progressArgs(
                    "📥 Downloading Progress", progress_message, start_time
                ),
            )

            LOGGER(__name__).info(f"Downloaded media: {media_path}")

            media_type = (
                "photo"
                if chat_message.photo
                else "video"
                if chat_message.video
                else "audio"
                if chat_message.audio
                else "document"
            )
            
            # Send media to the target chat ID
            if media_type == "photo":
                await bot.send_photo(
                    chat_id=target_chat_id,
                    photo=media_path,
                    caption=parsed_caption or "",
                    progress=Leaves.progress_for_pyrogram,
                    progress_args=progressArgs(
                        "📤 Uploading Progress", progress_message, start_time
                    ),
                )
            elif media_type == "video":
                if os.path.exists("Assets/video_thumb.jpg"):
                    os.remove("Assets/video_thumb.jpg")
                duration = (await get_media_info(media_path))[0]
                thumb = await get_video_thumbnail(media_path, duration)
                if thumb is not None and thumb != "none":
                    with Image.open(thumb) as img:
                        width, height = img.size
                else:
                    width = 480
                    height = 320

                if thumb == "none":
                    thumb = None

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
                        "📤 Uploading Progress", progress_message, start_time
                    ),
                )
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
                        "📤 Uploading Progress", progress_message, start_time
                    ),
                )
            elif media_type == "document":
                await bot.send_document(
                    chat_id=target_chat_id,
                    document=media_path,
                    caption=parsed_caption or "",
                    progress=Leaves.progress_for_pyrogram,
                    progress_args=progressArgs(
                        "📤 Uploading Progress", progress_message, start_time
                    ),
                )

            os.remove(media_path)
            await progress_message.delete()
            return True

        elif chat_message.text or chat_message.caption:
            await bot.send_message(
                chat_id=target_chat_id,
                text=parsed_text or parsed_caption
            )
            return True
        else:
            await message.reply("**No media or text found in the message.**")
            return False

    except Exception as e:
        LOGGER(__name__).error(f"Error processing message: {str(e)}")
        await message.reply(f"**Error processing message: {str(e)}**")
        return False


@bot.on_message(filters.command("stats") & filters.private)
async def stats(_, message: Message):
    currentTime = get_readable_time(time() - PyroConf.BOT_START_TIME)
    total, used, free = shutil.disk_usage(".")
    total = get_readable_file_size(total)
    used = get_readable_file_size(used)
    free = get_readable_file_size(free)
    sent = get_readable_file_size(psutil.net_io_counters().bytes_sent)
    recv = get_readable_file_size(psutil.net_io_counters().bytes_recv)
    cpuUsage = psutil.cpu_percent(interval=0.5)
    memory = psutil.virtual_memory().percent
    disk = psutil.disk_usage("/").percent
    process = psutil.Process(os.getpid())

    stats = (
        "**≧◉◡◉≦ Bot is Up and Running successfully.**\n\n"
        f"**➜ Bot Uptime:** `{currentTime}`\n"
        f"**➜ Total Disk Space:** `{total}`\n"
        f"**➜ Used:** `{used}`\n"
        f"**➜ Free:** `{free}`\n"
        f"**➜ Memory Usage:** `{round(process.memory_info()[0] / 1024**2)} MiB`\n\n"
        f"**➜ Upload:** `{sent}`\n"
        f"**➜ Download:** `{recv}`\n\n"
        f"**➜ CPU:** `{cpuUsage}%` | "
        f"**➜ RAM:** `{memory}%` | "
        f"**➜ DISK:** `{disk}%`"
    )
    await message.reply(stats)


@bot.on_message(filters.command("logs") & filters.private)
async def logs(_, message: Message):
    if os.path.exists("logs.txt"):
        await message.reply_document(document="logs.txt", caption="**Logs**")
    else:
        await message.reply("**Not exists**")


if __name__ == "__main__":
    try:
        import os
        from flask import Flask, jsonify
        import threading
        
        # Create a simple Flask app for health checks
        app = Flask(__name__)
        
        @app.route('/')
        def index():
            return jsonify({"status": "running"})
        
        # Get port from environment variable or use default
        port = int(os.environ.get("PORT", 8000))
        
        # Start Flask in a separate thread instead of the bot
        # This fixes the "signal only works in main thread" error
        def run_flask():
            LOGGER(__name__).info(f"Starting web server on port {port}")
            app.run(host="0.0.0.0", port=port)
        
        flask_thread = threading.Thread(target=run_flask)
        flask_thread.daemon = True
        flask_thread.start()
        
        # Start the bot in the main thread
        LOGGER(__name__).info("Bot Started!")
        user.start()
        bot.run()
        
    except KeyboardInterrupt:
        pass
    except Exception as err:
        LOGGER(__name__).error(err)
    finally:
        LOGGER(__name__).info("Bot Stopped")
