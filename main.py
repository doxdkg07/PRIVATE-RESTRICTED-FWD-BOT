
import os
import shutil
from time import time
import asyncio # Added for concurrency

import psutil
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from pyrogram import Client, filters
from pyrogram.errors import PeerIdInvalid, BadRequest, FloodWait # Added FloodWait
# from pyleaves import Leaves # Removed as progress is handled differently or removed
# from PIL import Image # No longer needed here if relying on pyrogram defaults

from helpers.utils import (
    getChatMsgID,
    # processMediaGroup, # Will be modified/called differently
    get_parsed_msg,
    fileSizeLimit,

    # send_media, # Removed
    get_readable_file_size,
    get_readable_time,
    # get_media_info, # Removed if not needed for fallback
    # get_video_thumbnail, # Removed
    processMediaGroup # Keep the import, will modify the function itself later
)

from config import PyroConf
from logger import LOGGER

# --- Configuration --- 
# Default worker count if not specified in config (can be overridden by env var if needed)
DEFAULT_WORKERS = int(os.getenv("PYROGRAM_WORKERS", "200"))

# Initialize the bot client
bot = Client(
    "media_bot",
    api_id=PyroConf.API_ID,
    api_hash=PyroConf.API_HASH,
    bot_token=PyroConf.BOT_TOKEN,
    workers=DEFAULT_WORKERS, # Use default worker count
    parse_mode=ParseMode.MARKDOWN,
)

# Client for user session
user = Client(
    "user_session",
    api_id=PyroConf.API_ID, # Use same API creds
    api_hash=PyroConf.API_HASH,
    workers=DEFAULT_WORKERS, # Use default worker count
    session_string=PyroConf.SESSION_STRING
)


@bot.on_message(filters.command("start") & filters.private)
async def start(_, message: Message):
    welcome_text = (
        "**👋 Welcome to the Optimized Media Forwarder Bot!**\n\n"
        "This bot helps you forward media from restricted channels faster.\n"
        "It prioritizes direct copying and uses concurrent processing for ranges.\n\n"
        "Use /help for more information."
    )
    await message.reply(welcome_text)


@bot.on_message(filters.command("help") & filters.private)
async def help_command(_, message: Message):
    help_text = (
        "💡 **How to Use the Bot**\n\n"
        "1. Send `/dl post_URL [end_URL_or_ID] [target_chat_id]`\n"
        "   - `post_URL`: Link to the first message.\n"
        "   - `end_URL_or_ID` (Optional): Link or ID of the last message for a range.\n"
        "   - `target_chat_id` (Optional): Channel/Group ID to forward to (e.g., -100xxxxxxxxxx). If omitted, sends to you.\n\n"
        "2. The bot will copy/forward the specified message(s).\n"
        "3. Make sure the **User Account** (not the bot) is a member of the source chat.\n\n"
        "**Examples:**\n"
        "  `/dl https://t.me/some/channel/123` (Single message to you)\n"
        "  `/dl https://t.me/c/12345/10 20` (Messages 10-20 to you)\n"
        "  `/dl https://t.me/c/12345/10 https://t.me/c/12345/20 -100987654321` (Range 10-20 forwarded)\n"
        "  `/dl https://t.me/c/12345/10 -100987654321` (Single message forwarded)"

    )
    await message.reply(help_text)


@bot.on_message(filters.command("dl") & filters.private)
async def download_media_command(bot: Client, message: Message):
    if len(message.command) < 2:
        await message.reply("**Usage:** `/dl post_URL [end_URL_or_ID] [target_chat_id]`")
        return

    start_url = message.command[1]
    end_ref = None
    forward_chat_id = None
    target_chat_id_int = None # Parsed target chat ID

    # Parse arguments flexibly
    if len(message.command) >= 3:
        # Is the 3rd arg a target chat ID (starts with -)?
        if message.command[2].startswith("-") and message.command[2][1:].isdigit():
            forward_chat_id = message.command[2]
        else:
            end_ref = message.command[2] # Assume it's the end message ref

    if len(message.command) >= 4:
         # If we already have end_ref, 4th arg must be target_chat_id
        if end_ref and message.command[3].startswith("-") and message.command[3][1:].isdigit():
             forward_chat_id = message.command[3]
        # If we didn't have end_ref, but 3rd arg wasn't target_id, then 3rd was end_ref and 4th is target_id
        elif not end_ref and not forward_chat_id and message.command[3].startswith("-") and message.command[3][1:].isdigit():
             end_ref = message.command[2] # Re-assign 3rd arg as end_ref
             forward_chat_id = message.command[3]
        else:
             await message.reply("**Invalid argument order.** Use `/dl start [end] [target_id]`")
             return

    # Validate and parse target_chat_id
    if forward_chat_id:
        try:
            target_chat_id_int = int(forward_chat_id)
            LOGGER(__name__).info(f"Forwarding requested to target chat ID: {target_chat_id_int}")
        except ValueError:
            await message.reply("**Invalid target_chat_id. Must be a number (usually starting with -100).**")
            return
    else:
        # Default to user's chat if no target_chat_id provided
        target_chat_id_int = message.chat.id
        LOGGER(__name__).info(f"No target chat ID provided. Sending to user chat: {target_chat_id_int}")


    try:
        start_chat_id, start_message_id = getChatMsgID(start_url)
        end_message_id = None

        if end_ref:
            # Try parsing end_ref as URL first
            try:
                 _, end_message_id = getChatMsgID(end_ref)
            except ValueError:
                 # If not a valid URL, try parsing as a direct message ID
                 try:
                     end_message_id = int(end_ref)
                 except ValueError:
                     await message.reply("**Invalid end_message reference. Use a message URL or ID.**")
                     return

        # Ensure chat IDs match if end_ref was a URL
        # (getChatMsgID doesn't return chat_id if input is just an ID)
        # We assume the user provides IDs from the same chat as the start_url

        if end_message_id is not None and end_message_id < start_message_id:
             await message.reply("**End message ID must be greater than or equal to start message ID.**")
             return

        # --- Trigger Processing ---
        if end_message_id is None:
            # Single message processing
            await process_single_message(bot, message, user, start_chat_id, start_message_id, target_chat_id_int)
        else:
            # Range message processing (now concurrent)
            await process_message_range(bot, message, user, start_chat_id, start_message_id, end_message_id, target_chat_id_int)

    except (ValueError, PeerIdInvalid, BadRequest) as e:
        await message.reply(f"**Error parsing link or accessing chat:** {e}. Make sure the link is correct and the **User Account** is in the source chat.")
    except FloodWait as e:
        await message.reply(f"**Rate limit hit. Please wait {e.value} seconds and try again.**")
        await asyncio.sleep(e.value)
    except Exception as e:
        error_message = f"**❌ An unexpected error occurred: {str(e)}**"
        await message.reply(error_message)
        LOGGER(__name__).error(f"Unexpected error in download_media_command: {e}", exc_info=True)


# Renamed from download_single_message for clarity
async def process_single_message(bot: Client, cmd_message: Message, user: Client, chat_id, message_id, target_chat_id):
    """Fetches and processes a single message."""
    log_prefix = f"[MsgID: {message_id}]"
    try:
        # Use user client to get message from restricted channel
        chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
        if not chat_message:
            await cmd_message.reply(f"**{log_prefix} Message not found.**")
            return False

        LOGGER(__name__).info(f"{log_prefix} Processing single message.")
        return await process_message_content(bot, cmd_message, user, chat_message, target_chat_id)

    except FloodWait as e:
        LOGGER(__name__).warning(f"{log_prefix} FloodWait: waiting {e.value}s")
        await cmd_message.reply(f"**Rate limit hit. Waiting {e.value} seconds...**")
        await asyncio.sleep(e.value)
        return await process_single_message(bot, cmd_message, user, chat_id, message_id, target_chat_id) # Retry
    except Exception as e:
        LOGGER(__name__).error(f"{log_prefix} Error fetching/processing single message: {str(e)}", exc_info=True)
        await cmd_message.reply(f"**{log_prefix} Error processing message: {str(e)}**")
        return False


# Renamed and refactored for concurrency
async def process_message_range(bot: Client, cmd_message: Message, user: Client, chat_id, start_id, end_id, target_chat_id):
    """Fetches and processes a range of messages concurrently."""
    if start_id > end_id:
        await cmd_message.reply("**Start message ID must be less than or equal to end message ID.**")
        return

    total_messages = end_id - start_id + 1
    status_message = await cmd_message.reply(f"**🚀 Starting processing for {total_messages} messages ({start_id} to {end_id})...**")

    success_count = 0
    failed_count = 0
    skipped_count = 0 # Not actively used currently, failure covers skips
    processed_count = 0

    # --- Concurrency Control ---
    CONCURRENCY_LIMIT = 10 # Adjust as needed based on performance/rate limits
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    tasks = []
    message_ids_to_fetch = list(range(start_id, end_id + 1))

    # --- Media Group Tracking --- 
    # To avoid processing/copying each item of a group individually multiple times
    processed_media_groups = set()

    async def process_with_semaphore(msg_id):
        """Wrapper to manage semaphore and process message content."""
        nonlocal success_count, failed_count, processed_count, processed_media_groups
        log_prefix = f"[MsgID: {msg_id}]"
        async with semaphore:
            try:
                # Fetch message using user client
                # TODO: Consider fetching messages in batches for efficiency?
                chat_message = await user.get_messages(chat_id=chat_id, message_ids=msg_id)
                
                if not chat_message:
                    LOGGER(__name__).info(f"{log_prefix} Message not found or skipped.")
                    failed_count += 1 # Count as failed if not found
                    
                # --- Media Group Handling within Range --- 
                elif chat_message.media_group_id:
                    group_id = chat_message.media_group_id
                    if group_id in processed_media_groups:
                        LOGGER(__name__).info(f"{log_prefix} Belongs to already processed media group {group_id}. Skipping.")
                        # Don't increment success/fail here, it was handled by the first message of the group
                    else:
                        LOGGER(__name__).info(f"{log_prefix} First message of media group {group_id}. Processing group.")
                        # Process the entire media group using the dedicated function
                        # This function should attempt copy_media_group first
                        result = await processMediaGroup(chat_message, bot, cmd_message, user, target_chat_id)
                        if result:
                            success_count += 1 # Count group as one success
                        else:
                            failed_count += 1 # Count group as one failure
                        processed_media_groups.add(group_id) # Mark group as processed
                
                # --- Process Single Message (Non-Media Group) --- 
                else:
                    LOGGER(__name__).info(f"{log_prefix} Processing non-group message content.")
                    result = await process_message_content(bot, cmd_message, user, chat_message, target_chat_id)
                    if result:
                        success_count += 1
                    else:
                        failed_count += 1

            except FloodWait as e:
                LOGGER(__name__).warning(f"{log_prefix} FloodWait during range processing: waiting {e.value}s")
                await asyncio.sleep(e.value + 1) # Wait a bit longer
                # For simplicity, count as failed and let user retry range if needed.
                failed_count += 1
            except Exception as e:
                LOGGER(__name__).error(f"{log_prefix} Error processing message in range: {str(e)}", exc_info=True)
                failed_count += 1
            finally:
                processed_count += 1
                # --- Update Status Periodically --- 
                # Update more frequently initially, then less often
                update_interval = 10 if processed_count < 100 else 50 if processed_count < 1000 else 100
                if processed_count % update_interval == 0 or processed_count == total_messages:
                     try:
                         # Calculate remaining tasks more accurately
                         remaining_tasks = total_messages - processed_count
                         await status_message.edit(
                             f"**Processing: {processed_count}/{total_messages} messages ({start_id}-{end_id})**\n"
                             f"**Target: `{target_chat_id}`**\n"
                             f"**Success: {success_count} | Failed: {failed_count}**\n"
                             f"*(~{remaining_tasks} remaining, {CONCURRENCY_LIMIT} concurrent)*"
                         )
                     except FloodWait as e:
                         LOGGER(__name__).warning(f"FloodWait while editing status: {e.value}s")
                         await asyncio.sleep(e.value + 1)
                     except Exception as edit_err:
                         # Ignore frequent errors like MessageNotModified
                         if "MessageNotModified" not in str(edit_err):
                              LOGGER(__name__).error(f"Error editing status message: {edit_err}")


    # Create tasks for all message IDs
    tasks = [asyncio.create_task(process_with_semaphore(msg_id)) for msg_id in message_ids_to_fetch]

    # Wait for all tasks to complete
    await asyncio.gather(*tasks)

    # Final status update
    final_text = (
        f"**✅ Processing completed for messages {start_id} to {end_id}**\n"
        f"**Target: `{target_chat_id}`**\n"
        f"**Total Attempted: {total_messages} | Success: {success_count} | Failed: {failed_count}**"
        # Note: Success/Failed counts might not sum to total if groups are involved
    )
    try:
        await status_message.edit(final_text)
    except Exception as final_edit_err:
        LOGGER(__name__).error(f"Error editing final status: {final_edit_err}")
        await cmd_message.reply(final_text) # Send as new message if edit fails


# Renamed from process_message, handles the actual content forwarding/copying
async def process_message_content(bot: Client, cmd_message: Message, user: Client, chat_message: Message, target_chat_id: int):
    """Processes the content of a single fetched message (copy or download/upload fallback).
       This function should NOT be called directly for media group messages; 
       process_message_range handles routing to processMediaGroup.
    """
    log_prefix = f"[MsgID: {chat_message.id}]"
    
    # Double check it's not a media group message if called directly
    if chat_message.media_group_id:
        LOGGER(__name__).warning(f"{log_prefix} process_message_content called directly for media group message. This should be handled by processMediaGroup. Skipping.")
        return False # Avoid duplicate processing

    try:
        # --- Optimization 1: Attempt Direct Copy using User Client --- 
        copied_message = None
        if target_chat_id and (chat_message.media or chat_message.text):
            try:
                LOGGER(__name__).info(f"{log_prefix} Attempting direct copy to {target_chat_id}...")
                copied_message = await user.copy_message(
                    chat_id=target_chat_id,
                    from_chat_id=chat_message.chat.id,
                    message_id=chat_message.id,
                )
                if copied_message:
                    LOGGER(__name__).info(f"{log_prefix} Direct copy successful.")
                    await asyncio.sleep(0.5) # Small delay after copy
                    return True # Successfully copied
                else:
                    # copy_message returning None usually indicates failure
                    LOGGER(__name__).warning(f"{log_prefix} Direct copy attempt returned None/False.")

            except FloodWait as e:
                LOGGER(__name__).warning(f"{log_prefix} FloodWait during copy: waiting {e.value}s")
                await asyncio.sleep(e.value + 1) # Wait a bit longer
                # Retry copy once after flood wait
                try:
                    LOGGER(__name__).info(f"{log_prefix} Retrying direct copy after FloodWait...")
                    copied_message = await user.copy_message(
                        chat_id=target_chat_id,
                        from_chat_id=chat_message.chat.id,
                        message_id=chat_message.id,
                    )
                    if copied_message:
                        LOGGER(__name__).info(f"{log_prefix} Direct copy successful after retry.")
                        await asyncio.sleep(0.5)
                        return True
                    else:
                         LOGGER(__name__).warning(f"{log_prefix} Direct copy retry returned None/False.")
                except FloodWait as e2:
                     LOGGER(__name__).error(f"{log_prefix} FloodWait again after retry: {e2.value}s. Giving up on copy.")
                except Exception as retry_err:
                     LOGGER(__name__).error(f"{log_prefix} Direct copy retry failed: {retry_err}. Falling back.")

            except Exception as copy_err:
                LOGGER(__name__).warning(f"{log_prefix} Direct copy failed: {type(copy_err).__name__} - {copy_err}. Falling back.")

        # --- Optimization 2: Skip Download/Upload if Copy Succeeded --- 
        if copied_message:
             return True # Already handled

        # --- Fallback: Download and Upload using Bot Client --- 
        # Only use this if copy failed
        if chat_message.media or chat_message.text:
            LOGGER(__name__).info(f"{log_prefix} Proceeding with download/upload fallback.")

            # File size check (important for download)
            file_size = getattr(chat_message.document or chat_message.video or chat_message.audio or chat_message.photo, "file_size", 0)
            if file_size and not await fileSizeLimit(file_size, cmd_message, "forward (fallback)", user.me.is_premium):
                return False # Skip this message due to size

            start_time = time()
            media_path = None
            try:
                if chat_message.media:
                    LOGGER(__name__).info(f"{log_prefix} Downloading media for fallback upload... (User: {user.me.id})")
                    # Download using USER client, as bot might not have access
                    media_path = await user.download_media(
                        message=chat_message,
                        # file_name= # Optional: Define a structured path
                        # progress= # Disable progress for fallback?
                    )
                    if not media_path or not os.path.exists(media_path):
                         raise Exception(f"Download failed or returned invalid path: {media_path}")
                    LOGGER(__name__).info(f"{log_prefix} Downloaded to: {media_path}")

                    # Upload using BOT client
                    parsed_caption = await get_parsed_msg(chat_message.caption or "", chat_message.caption_entities)
                    LOGGER(__name__).info(f"{log_prefix} Uploading media via bot... (Bot: {bot.me.id})")

                    # Use specific send methods, relying on Pyrogram defaults
                    if chat_message.photo:
                        await bot.send_photo(
                            chat_id=target_chat_id, photo=media_path, caption=parsed_caption or ""
                        )
                    elif chat_message.video:
                        await bot.send_video(
                            chat_id=target_chat_id, video=media_path, caption=parsed_caption or ""
                        )
                    elif chat_message.audio:
                        await bot.send_audio(
                            chat_id=target_chat_id, audio=media_path, caption=parsed_caption or ""
                        )
                    elif chat_message.document:
                        await bot.send_document(
                            chat_id=target_chat_id, document=media_path, caption=parsed_caption or ""
                        )
                    else:
                         raise Exception("Unknown media type during fallback upload.")

                    LOGGER(__name__).info(f"{log_prefix} Fallback upload successful.")
                    await asyncio.sleep(0.5) # Small delay after upload

                elif chat_message.text:
                    LOGGER(__name__).info(f"{log_prefix} Sending text message (fallback).")
                    parsed_text = await get_parsed_msg(chat_message.text or "", chat_message.entities)
                    await bot.send_message(chat_id=target_chat_id, text=parsed_text)
                    await asyncio.sleep(0.2)

                return True

            except FloodWait as e:
                 LOGGER(__name__).warning(f"{log_prefix} FloodWait during fallback download/upload: waiting {e.value}s")
                 await asyncio.sleep(e.value + 1)
                 return False # Count as failed
            except Exception as down_up_err:
                 LOGGER(__name__).error(f"{log_prefix} Download/Upload fallback failed: {down_up_err}", exc_info=True)
                 return False
            finally:
                if media_path and os.path.exists(media_path):
                    try:
                        os.remove(media_path)
                        LOGGER(__name__).info(f"{log_prefix} Cleaned up fallback file: {media_path}")
                    except OSError as rm_err:
                         LOGGER(__name__).error(f"{log_prefix} Error removing fallback file {media_path}: {rm_err}")
        else:
            # This case means: copy failed AND it wasn't media/text (e.g., service message?)
            LOGGER(__name__).warning(f"{log_prefix} Message has no processable content or copy failed.")
            return False # Treat as failure/skip

    except Exception as e:
        LOGGER(__name__).error(f"{log_prefix} Unexpected error in process_message_content: {str(e)}", exc_info=True)
        return False

    # Should not be reached
    return False


# --- Stats and Logs Commands (Keep existing logic but update text/paths if needed) ---
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

    stats_text = (
        "**📊 Bot Statistics**\n\n"
        f"**➜ Bot Uptime:** `{currentTime}`\n"
        f"**➜ CPU:** `{cpuUsage}%` | **RAM:** `{memory}%` | **DISK:** `{disk}%`\n"
        f"**➜ Memory Usage:** `{round(process.memory_info()[0] / 1024**2)} MiB`\n\n"
        f"**➜ Total Disk:** `{total}` | **Used:** `{used}` | **Free:** `{free}`\n"
        f"**➜ Network Up:** `{sent}` | **Down:** `{recv}`"
    )
    await message.reply(stats_text)


@bot.on_message(filters.command("logs") & filters.private)
async def logs(_, message: Message):
    log_file = "logs.txt" # Make sure logger is configured to write here
    if os.path.exists(log_file):
        try:
            await message.reply_document(document=log_file, caption="**Bot Logs**")
        except Exception as log_err:
             await message.reply(f"**Error sending logs:** {log_err}")
             LOGGER(__name__).error(f"Error sending log file: {log_err}")
    else:
        await message.reply("**Log file not found.**")


# --- Main Execution --- 
if __name__ == "__main__":
    # Make 'Assets' directory if it doesn't exist (used by original code, keep for now)
    if not os.path.isdir("Assets"):
        os.makedirs("Assets")
        
    # Ensure download directory exists (Pyrogram default is 'downloads/')
    if not os.path.isdir("downloads"):
        os.makedirs("downloads")

    try:
        # Flask health check remains the same
        from flask import Flask, jsonify
        import threading

        app = Flask(__name__)
        @app.route('/')
        def index():
            return jsonify({"status": "running", "bot": "media_forwarder_optimized"})

        port = int(os.environ.get("PORT", 8000))

        def run_flask():
            LOGGER(__name__).info(f"Starting health check server on port {port}")
            try:
                 # Use waitress or similar for production
                 from waitress import serve
                 serve(app, host="0.0.0.0", port=port)
                 # app.run(host="0.0.0.0", port=port) # Development server
            except Exception as flask_err:
                 LOGGER(__name__).error(f"Flask server error: {flask_err}")

        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()

        # Start Clients (User client first is often recommended)
        LOGGER(__name__).info("Starting User Client...")
        user.start()
        user_me = user.get_me()
        LOGGER(__name__).info(f"User Client started as: {user_me.first_name} (ID: {user_me.id}, Premium: {user_me.is_premium})")

        LOGGER(__name__).info("Starting Bot Client...")
        bot.start()
        bot_me = bot.get_me()
        LOGGER(__name__).info(f"Bot Client started as: @{bot_me.username} (ID: {bot_me.id})")

        # Store start time in config or global variable if needed for stats uptime
        if not hasattr(PyroConf, 'BOT_START_TIME'):
             PyroConf.BOT_START_TIME = time()

        LOGGER(__name__).info("Bot is now running! Use /dl command.")
        # Keep main thread alive using Pyrogram's idle
        from pyrogram import idle
        idle()

    except KeyboardInterrupt:
        LOGGER(__name__).info("Shutdown signal received.")
    except Exception as err:
        LOGGER(__name__).error(f"Error during startup or main loop: {err}", exc_info=True)
    finally:
        LOGGER(__name__).info("Stopping clients...")
        if user.is_connected:
            user.stop()
        if bot.is_connected:
            bot.stop()
        LOGGER(__name__).info("Clients stopped. Exiting.")

