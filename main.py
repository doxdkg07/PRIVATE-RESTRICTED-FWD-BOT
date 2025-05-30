
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
    get_readable_file_size,
    get_readable_time,
    get_media_info,
    get_video_thumbnail,
    FloodWaitDetected # Import the custom exception
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
ongoing_tasks = {}  # Key: user_id, Value: {"cancel": False, "message": status_message_object, "flood_stop": False, "processed_files": set()}

# --- Helper Functions --- #

async def send_and_delete(message: Message, text: str, delay: int = 3):
    """Sends a message and deletes it after a specified delay."""
    try:
        sent_message = await message.reply_text(text, quote=True)
        await asyncio.sleep(delay)
        await sent_message.delete()
    except FloodWait as fw:
        LOGGER.warning(f"Flood wait ({fw.value}s) encountered while sending/deleting temp message: {text}")
        await asyncio.sleep(fw.value)
        # Don't retry deleting, just log
    except Exception as e:
        LOGGER.warning(f"Error sending/deleting temp message ", exc_info=True)

async def handle_flood_wait(fw: FloodWait, user_id: int, message: Message, status_message: Message = None):
    """Handles FloodWait exceptions by notifying the user and stopping the task."""
    LOGGER.error(f"Flood wait encountered for user {user_id}: {fw}")
    wait_time = fw.value
    error_text = f"**🛑 Flood Limit Error!**\nTelegram requires a wait of {wait_time} seconds. The current task has been automatically stopped to prevent further issues. Please try again later."
    
    try:
        if status_message:
            await status_message.edit(error_text)
        else:
            await message.reply(error_text)
    except Exception as e_notify:
         LOGGER.error(f"Failed to notify user {user_id} about flood wait: {e_notify}")
         # Try replying to original message as fallback if status edit failed
         if status_message:
             try: await message.reply(error_text)
             except Exception as e_reply_fallback: LOGGER.error(f"Fallback reply failed for flood wait notification user {user_id}: {e_reply_fallback}")
        
    # Mark task for cancellation/stop
    if user_id in ongoing_tasks:
        ongoing_tasks[user_id]["cancel"] = True
        ongoing_tasks[user_id]["flood_stop"] = True # Mark specifically as flood stopped

# --- Bot Commands --- #

@bot.on_message(filters.command("start") & filters.private)
async def start(_, message: Message):
    welcome_text = (
        "**👋 Welcome to the Media Downloader Bot!**\n\n"
        "This bot helps you download media from Restricted channels/chats.\n"
        "Use /help for more information."
    )
    await message.reply(welcome_text)


@bot.on_message(filters.command("help") & filters.private)
async def help_command(_, message: Message):
    help_text = (
        "💡 **How to Use the Bot**\n\n"
        "1. Send `/dl <post URL>` to download/copy a single message.\n"
        "2. Send `/dl <post URL> <end_ID>` to download/copy a range of messages.\n"
        "3. Add a `channel_ID` at the end to forward content: `/dl <post URL> [end_ID] <channel_ID>`\n"
        "4. Use `/cancel` to stop any ongoing task initiated by you.\n"
        "5. Use `/stats` to check bot status.\n"
        "6. Use `/logs` to get the bot log file.\n\n"
        "**Features:**\n"
        "   - Downloads media (photos, videos, audio, documents).\n"
        "   - Copies text messages/captions.\n"
        "   - Handles single posts and message ranges.\n"
        "   - Forwards content to a specified chat ID.\n"
        "   - Skips duplicate media files within the same task.\n"
        "   - Auto-stops on Telegram flood errors.\n"
        "   - Skip/Error notifications are auto-deleted after 3 seconds.\n\n"
        "**Important:** Ensure the user account (`SESSION_STRING`) is a member of the source chat.\n\n"
        "**Examples:**\n"
        "   `/dl https://t.me/some_channel/123`
"
        "   `/dl https://t.me/c/1234567890/100 150`
"
        "   `/dl https://t.me/public_group/456 -100987654321`
"
        "   `/dl https://t.me/c/1234567890/200 250 -1001122334455`"
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
                LOGGER.warning(f"Could not edit status message during cancel: {e}")
    else:
        await message.reply("**You have no active task to cancel.**")


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
    try:
        if len(message.command) >= 3:
            potential_id_1 = int(message.command[2])
            if potential_id_1 < 0: # Treat as Forward_ID
                forward_chat_id = potential_id_1
            else: # Treat as End_ID
                end_message_id = potential_id_1

        if len(message.command) >= 4:
            potential_id_2 = int(message.command[3])
            if end_message_id is not None: # If End_ID was set, this must be Forward_ID
                if potential_id_2 < 0:
                    forward_chat_id = potential_id_2
                else:
                    raise ValueError("Invalid Forward Channel ID (must be negative).")
            else: # If only Forward_ID was set earlier, this is an error
                 raise ValueError("Invalid command structure after Forward ID.")
                 
    except (ValueError, IndexError) as e:
        await message.reply(f"**Invalid command arguments. Use /help for details. Error: {e}**")
        return

    if forward_chat_id:
         LOGGER.info(f"User {user_id}: Content will be forwarded to channel/group ID: {forward_chat_id}")

    task_start_time = time()
    try:
        chat_id, start_message_id = getChatMsgID(post_url)

        # Initialize task tracking with processed_files set
        ongoing_tasks[user_id] = {
            "cancel": False, 
            "message": None, 
            "flood_stop": False, 
            "processed_files": set() # Initialize set for duplicate tracking
        }

        if end_message_id is None:
            await download_single_message(bot, message, user, chat_id, start_message_id, forward_chat_id, user_id)
        else:
            await download_message_range(bot, message, user, chat_id, start_message_id, end_message_id, forward_chat_id, user_id)

    except FloodWait as fw:
        await handle_flood_wait(fw, user_id, message)
    except (PeerIdInvalid, BadRequest, KeyError) as e:
        await message.reply(f"**Error accessing chat/message:** Make sure the user account ({user.me.username}) is a member of the source chat. Details: {e}")
    except ValueError as e: # Catch parsing errors from getChatMsgID
         await message.reply(f"**Error parsing link:** {e}")
    except Exception as e:
        error_message = f"**❌ An unexpected error occurred: {str(e)}**"
        await message.reply(error_message)
        LOGGER.error(f"Error in /dl command for user {user_id}: {e}", exc_info=True)
    finally:
        # Clean up task tracking only if it exists
        if user_id in ongoing_tasks:
            task_duration = get_readable_time(time() - task_start_time)
            LOGGER.info(f"Task for user {user_id} finished or removed. Duration: {task_duration}")
            del ongoing_tasks[user_id]

async def download_single_message(bot: Client, message: Message, user: Client, chat_id, message_id, forward_chat_id, user_id):
    if user_id not in ongoing_tasks: return False # Task was removed prematurely
    if ongoing_tasks[user_id]["cancel"]:
        LOGGER.info(f"Task cancelled by user {user_id} before processing message {message_id}")
        if not ongoing_tasks[user_id].get("flood_stop", False):
            await message.reply("**Task cancelled.**")
        return False
        
    processed_files_set = ongoing_tasks[user_id]["processed_files"]
    
    try:
        chat_message = await user.get_messages(chat_id=chat_id, message_ids=message_id)
        if not chat_message:
            # Use send_and_delete for skip messages
            await send_and_delete(message, f"**Message with ID {message_id} not found. Skipping.**")
            return False # Indicate skipped

        LOGGER.info(f"Processing single message ID: {message_id} for user {user_id}")
        return await process_message(bot, message, user, chat_message, forward_chat_id, user_id, processed_files_set)
        
    except FloodWait as fw:
        await handle_flood_wait(fw, user_id, message)
        return False # Indicate failure due to flood wait
    except Exception as e:
        LOGGER.error(f"Error downloading single message {message_id} for user {user_id}: {str(e)}")
        await message.reply(f"**Error processing message {message_id}: {str(e)}**")
        return False

async def download_message_range(bot: Client, message: Message, user: Client, chat_id, start_id, end_id, forward_chat_id, user_id):
    if start_id > end_id:
        await message.reply("**Start message ID must be less than or equal to end message ID.**")
        return
        
    if user_id not in ongoing_tasks: return # Task was removed prematurely

    status_message = None
    processed_files_set = ongoing_tasks[user_id]["processed_files"]
    
    try:
        status_message = await message.reply(f"**📥 Preparing to process messages from {start_id} to {end_id}...**")
        if user_id in ongoing_tasks: # Check again as it might have been cancelled
            ongoing_tasks[user_id]["message"] = status_message
        else:
             await status_message.delete() # Delete status if task cancelled during creation
             return
             
    except FloodWait as fw_status:
        await handle_flood_wait(fw_status, user_id, message)
        return # Stop the task immediately
    except Exception as e_status:
        LOGGER.error(f"Error sending initial status message for user {user_id}: {e_status}")
        await message.reply(f"**Error starting task: {e_status}**")
        if user_id in ongoing_tasks: ongoing_tasks[user_id]["cancel"] = True 
        return

    success_count = 0
    failed_count = 0
    skipped_not_found_count = 0
    skipped_duplicate_count = 0
    cancelled = False # Tracks user cancel or flood stop

    for msg_id in range(start_id, end_id + 1):
        # Check for cancellation/stop at the start of each iteration
        if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
            LOGGER.info(f"Task stopped/cancelled by user {user_id} during range processing at message {msg_id}")
            cancelled = True
            break
            
        try:
            # Fetch message
            chat_message = await user.get_messages(chat_id=chat_id, message_ids=msg_id)

            if not chat_message:
                LOGGER.info(f"Message {msg_id} not found, skipping.")
                skipped_not_found_count += 1
                # Don't send notification for every not found message in range to avoid spam/limits
                # Consider logging or a single summary message at the end if needed.
                await asyncio.sleep(0.2) # Shorter sleep for skipped messages
                continue

            LOGGER.info(f"Processing message ID: {msg_id} in range for user {user_id}")

            # Update status periodically
            if msg_id % 10 == 0 or msg_id == start_id: # Update less frequently
                if cancelled: break
                try:
                    status_text = (
                        f"**📥 Processing messages {start_id} to {end_id}...**\n"
                        f"**Current:** {msg_id}/{end_id}\n"
                        f"**Success:** {success_count} | **Failed:** {failed_count} | **Skipped (Dup):** {skipped_duplicate_count} | **Skipped (NF):** {skipped_not_found_count}"
                    )
                    await status_message.edit(status_text)
                except FloodWait as fw_edit:
                    LOGGER.warning(f"Flood wait editing status message for user {user_id}: {fw_edit}. Pausing edit.")
                    await asyncio.sleep(fw_edit.value + 2)
                except Exception as edit_err:
                    LOGGER.warning(f"Could not edit status message for user {user_id}: {edit_err}")

            # Process message
            if cancelled: break
            result = await process_message(bot, message, user, chat_message, forward_chat_id, user_id, processed_files_set)
            
            # Check if process_message caused a flood stop
            if user_id not in ongoing_tasks or ongoing_tasks[user_id].get("flood_stop", False):
                 LOGGER.info(f"Flood stop detected after process_message for user {user_id} at msg {msg_id}")
                 cancelled = True
                 break # Exit loop immediately after flood stop
                 
            if result == "success":
                success_count += 1
            elif result == "skipped_duplicate":
                 skipped_duplicate_count += 1
                 # Notification handled within process_message
            else: # Includes general failure, skipped_not_found (handled above), file size limit etc.
                failed_count += 1
            
            # Check cancellation status again before sleeping
            if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
                cancelled = True
                break
                
            await asyncio.sleep(PyroConf.SLEEP_TIMER) # Use configured sleep timer

        except FloodWait as fw:
            await handle_flood_wait(fw, user_id, message, status_message)
            cancelled = True # Mark as cancelled to stop the loop
            break # Exit loop immediately
        except Exception as e:
            LOGGER.error(f"Error processing message {msg_id} in range for user {user_id}: {str(e)}", exc_info=True)
            failed_count += 1
            await asyncio.sleep(1) # Short sleep on general error
            continue # Continue to next message if possible

    # Final status update
    if status_message:
        is_flood_stop = ongoing_tasks.get(user_id, {}).get("flood_stop", False)
        final_prefix = "🛑 Task Stopped (Flood Error)" if is_flood_stop else ("⚠️ Task Cancelled" if cancelled else "✅ Task Completed")
        final_text = (
             f"**{final_prefix} for messages {start_id} to {end_id}**\n"
             f"**Success:** {success_count} | **Failed:** {failed_count} | **Skipped (Dup):** {skipped_duplicate_count} | **Skipped (NF):** {skipped_not_found_count}"
        )
        try:
            await status_message.edit(final_text)
        except Exception as final_edit_err:
             LOGGER.warning(f"Could not edit final status message for user {user_id}: {final_edit_err}")
             if not is_flood_stop:
                 try: await message.reply(final_text)
                 except Exception: pass # Ignore if final reply fails
                 
    elif cancelled:
         is_flood_stop = ongoing_tasks.get(user_id, {}).get("flood_stop", False)
         if not is_flood_stop:
              final_text = (
                  f"**⚠️ Task Cancelled for messages {start_id} to {end_id}**\n"
                  f"**Success:** {success_count} | **Failed:** {failed_count} | **Skipped (Dup):** {skipped_duplicate_count} | **Skipped (NF):** {skipped_not_found_count}"
              )
              try: await message.reply(final_text)
              except Exception: pass

async def process_message(bot: Client, message: Message, user: Client, chat_message: Message, forward_chat_id: int, user_id: int, processed_files: set):
    media_path = None
    thumb_path = None
    progress_message = None
    file_unique_id = None
    media = None
    
    # --- Identify Media and Unique ID --- #
    if chat_message.photo:
        media = chat_message.photo
    elif chat_message.video:
        media = chat_message.video
    elif chat_message.audio:
        media = chat_message.audio
    elif chat_message.document:
        media = chat_message.document
    elif chat_message.sticker:
         media = chat_message.sticker # Stickers also have unique IDs
    # Add other types like animation, voice if needed
    
    if media and hasattr(media, "file_unique_id"):
        file_unique_id = media.file_unique_id
        
    # --- Check for Duplicates --- #
    if file_unique_id and file_unique_id in processed_files:
        LOGGER.info(f"Skipping duplicate media (ID: {file_unique_id}) for message {chat_message.id}, user {user_id}")
        await send_and_delete(message, f"⏩ Skipping duplicate media from message {chat_message.id}.")
        return "skipped_duplicate"
        
    try:
        # Check for cancellation before processing
        if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
            LOGGER.info(f"Task cancelled by user {user_id} before processing message {chat_message.id}")
            return "failure"

        # --- File Size Limit Check --- #
        if media and hasattr(media, "file_size") and media.file_size:
            is_premium = user.me.is_premium if user.me else False # Check if user client is premium
            if not await fileSizeLimit(media.file_size, message, "download", is_premium):
                await send_and_delete(message, f"File in message {chat_message.id} too large. Skipping.")
                return "failure" # Limit exceeded

        parsed_caption = await get_parsed_msg(chat_message.caption or "", chat_message.caption_entities)
        parsed_text = await get_parsed_msg(chat_message.text or "", chat_message.entities)
        target_chat_id = forward_chat_id if forward_chat_id else message.chat.id

        # --- Media Group Processing --- #
        if chat_message.media_group_id:
            if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
                return "failure"
            LOGGER.info(f"Processing media group: {chat_message.media_group_id} for user {user_id}")
            try:
                # Pass the processed_files set to the helper function
                group_result = await processMediaGroup(chat_message, bot, message, target_chat_id, user_id, ongoing_tasks, processed_files)
                return "success" if group_result else "failure"
            except FloodWaitDetected:
                 # Flood wait handled within processMediaGroup/handle_flood_wait
                 return "failure" # Task should be stopped
            except Exception as group_err:
                 LOGGER.error(f"Error in processMediaGroup for user {user_id}: {group_err}", exc_info=True)
                 await message.reply(f"**Error processing media group {chat_message.media_group_id}: {group_err}**")
                 return "failure"

        # --- Single Media / Text Processing --- #
        elif media or parsed_text:
             # Add file_unique_id to set *before* processing/downloading
             if file_unique_id:
                 processed_files.add(file_unique_id)
                 
             if media: # Process single media item
                 if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
                     return "failure"
                 
                 start_time = time()
                 try:
                     progress_message = await message.reply("**📥 Preparing Download...**") 
                 except FloodWait as fw_prog:
                      await handle_flood_wait(fw_prog, user_id, message)
                      return "failure" # Stop task
                 except Exception as e_prog:
                      LOGGER.error(f"Error sending progress message for user {user_id}: {e_prog}")
                      await message.reply(f"**Error starting download: {e_prog}**")
                      if user_id in ongoing_tasks: ongoing_tasks[user_id]["cancel"] = True
                      return "failure" # Stop task

                 # --- Download --- #
                 try:
                      media_path = await chat_message.download(
                         progress=Leaves.progress_for_pyrogram,
                         progress_args=progressArgs("📥 Downloading", progress_message, start_time)
                      )
                 except FloodWait as fw_dl:
                      await handle_flood_wait(fw_dl, user_id, message, progress_message)
                      return "failure" # Stop task
                 except Exception as download_err:
                      LOGGER.error(f"Error during media download for message {chat_message.id} user {user_id}: {download_err}")
                      try: await progress_message.edit(f"**❌ Download Failed: {download_err}**")
                      except Exception: pass
                      # Don't return failure here, cleanup happens in finally
                      raise # Re-raise to be caught by outer try/except and trigger finally cleanup

                 # Check cancellation after download
                 if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
                     LOGGER.info(f"Task cancelled by user {user_id} after downloading message {chat_message.id}")
                     try: await progress_message.edit("**Task Cancelled after download.**")
                     except Exception: pass
                     return "failure"

                 LOGGER.info(f"Downloaded media: {media_path}")
                 try: await progress_message.edit("**📤 Preparing Upload...**")
                 except Exception: pass

                 media_type = chat_message.media.value # e.g., "photo", "video"

                 # --- Upload --- #
                 thumb = None
                 try:
                     if media_type == "photo":
                         await bot.send_photo(chat_id=target_chat_id, photo=media_path, caption=parsed_caption or "",
                                              progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
                     elif media_type == "video":
                         thumb_path = f"Assets/video_thumb_{os.path.basename(media_path)}.jpg" # Unique thumb path
                         duration, _, _, width, height = await get_media_info(media_path)
                         thumb = await get_video_thumbnail(media_path, duration)
                         # Use dimensions from message if available, else from ffprobe/thumb
                         width = chat_message.video.width or width
                         height = chat_message.video.height or height
                         if (not width or not height) and thumb and thumb != "none":
                             try:
                                 with Image.open(thumb) as img: width, height = img.size
                             except Exception as img_err: LOGGER.warning(f"Could not read thumb dimensions: {img_err}")
                         if not width: width = 640
                         if not height: height = 360
                         if thumb == "none": thumb = None

                         await bot.send_video(chat_id=target_chat_id, video=media_path, duration=duration or 0, width=width, height=height, thumb=thumb, caption=parsed_caption or "",
                                              progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
                     elif media_type == "audio":
                         duration, artist, title, _, _ = await get_media_info(media_path)
                         await bot.send_audio(chat_id=target_chat_id, audio=media_path, duration=duration or 0, performer=artist, title=title, caption=parsed_caption or "",
                                              progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
                     elif media_type == "document":
                         await bot.send_document(chat_id=target_chat_id, document=media_path, caption=parsed_caption or "",
                                                 progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
                     elif media_type == "sticker":
                          await bot.send_sticker(chat_id=target_chat_id, sticker=media_path,
                                                 progress=Leaves.progress_for_pyrogram, progress_args=progressArgs("📤 Uploading", progress_message, start_time))
                     # Add other types like animation, voice if needed
                     else:
                          LOGGER.warning(f"Unsupported media type for upload: {media_type} in message {chat_message.id}")
                          await send_and_delete(message, f"Unsupported media type ", 3)
                          return "failure"
                          
                 except FloodWait as fw_send:
                      await handle_flood_wait(fw_send, user_id, message, progress_message)
                      return "failure" # Stop task
                 except Exception as send_err:
                      LOGGER.error(f"Error sending media for message {chat_message.id} user {user_id}: {send_err}", exc_info=True)
                      try: await progress_message.edit(f"**❌ Upload Failed: {send_err}**")
                      except Exception: pass
                      raise # Re-raise to be caught by outer try/except and trigger finally cleanup

                 try: await progress_message.delete()
                 except Exception: pass
                 progress_message = None # Prevent deletion in finally
                 return "success" # Success for single media

             elif parsed_text: # Process text message
                 if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
                     return "failure"
                 try:
                     await bot.send_message(chat_id=target_chat_id, text=parsed_text)
                     return "success" # Success for text message
                 except FloodWait as fw_text:
                      await handle_flood_wait(fw_text, user_id, message)
                      return "failure" # Stop task
                 except Exception as text_err:
                     LOGGER.error(f"Error sending text message {chat_message.id} user {user_id}: {text_err}")
                     await message.reply(f"**Error sending text message {chat_message.id}: {text_err}**")
                     return "failure" # Indicate failure
             else:
                 # Should not happen if media or text was detected earlier
                 LOGGER.warning(f"Message {chat_message.id} has no media or text after initial check for user {user_id}.")
                 return "failure"
                 
        else:
            # No media, no text, no caption - skip
            LOGGER.info(f"Message {chat_message.id} has no downloadable/forwardable content for user {user_id}.")
            # Don't send notification for every skipped item unless it's a single request
            # await send_and_delete(message, f"Message {chat_message.id} has no content. Skipping.")
            return "failure" # Indicate skipped/nothing to process

    except FloodWait as fw_outer:
        await handle_flood_wait(fw_outer, user_id, message, progress_message)
        return "failure" # Stop task
    except Exception as e:
        LOGGER.error(f"Error processing message {chat_message.id} for user {user_id}: {str(e)}", exc_info=True)
        error_text = f"**Error processing message {chat_message.id}: {str(e)}**"
        if progress_message:
             try: await progress_message.edit(error_text)
             except Exception: pass
        else:
             try: await message.reply(error_text)
             except Exception: pass
        return "failure" # Indicate general failure
    finally:
        # Ensure cleanup of downloaded file and thumbnail
        if media_path and os.path.exists(media_path):
            try: os.remove(media_path)
            except OSError as e: LOGGER.warning(f"Error removing media file {media_path}: {e}")
        if thumb_path and os.path.exists(thumb_path):
            try: os.remove(thumb_path)
            except OSError as e: LOGGER.warning(f"Error removing thumb file {thumb_path}: {e}")
        # Try deleting progress message if it still exists and wasn't deleted after success/handled error
        if progress_message:
             try: await progress_message.delete()
             except Exception: pass

# --- Stats and Logs Commands (Modified for Robustness) --- #

@bot.on_message(filters.command("stats") & filters.private)
async def stats(_, message: Message):
    bot_uptime = "N/A"
    if hasattr(PyroConf, 'BOT_START_TIME'):
        bot_uptime = get_readable_time(time() - PyroConf.BOT_START_TIME)
        
    total, used, free = "N/A", "N/A", "N/A"
    try:
        total, used, free = shutil.disk_usage(".")
        total = get_readable_file_size(total)
        used = get_readable_file_size(used)
        free = get_readable_file_size(free)
    except Exception as e:
        LOGGER.warning(f"Could not get disk usage: {e}")
        
    sent, recv = "N/A", "N/A"
    try:
        net_io = psutil.net_io_counters()
        sent = get_readable_file_size(net_io.bytes_sent)
        recv = get_readable_file_size(net_io.bytes_recv)
    except Exception as e:
        LOGGER.warning(f"Could not get network stats: {e}")
        
    cpuUsage, memory, disk, mem_used = "N/A", "N/A", "N/A", "N/A"
    try:    
        cpuUsage = psutil.cpu_percent(interval=0.5)
        memory = psutil.virtual_memory().percent
        disk = psutil.disk_usage("/").percent
        process = psutil.Process(os.getpid())
        mem_used = f"{round(process.memory_info().rss / 1024**2)} MiB"
    except Exception as e:
         LOGGER.warning(f"Could not get process/system stats: {e}")

    stats_text = (
        "**📊 Bot Status**\n\n"
        f"**➜ Bot Uptime:** `{bot_uptime}`\n"
        f"**➜ CPU:** `{cpuUsage}%` | **RAM:** `{memory}%` | **DISK:** `{disk}%`\n"
        f"**➜ Memory Usage:** `{mem_used}`\n\n"
        f"**➜ Disk Space:** `{total}` (Total) | `{used}` (Used) | `{free}` (Free)\n"
        f"**➜ Network:** `{sent}` (Upload) | `{recv}` (Download)"
    )
    await message.reply(stats_text)


@bot.on_message(filters.command("logs") & filters.private)
async def logs(_, message: Message):
    log_file = "logs.txt"
    if os.path.exists(log_file):
        try:
            await message.reply_document(document=log_file, caption="**Bot Logs**")
        except FloodWait as fw:
             await asyncio.sleep(fw.value)
             try: await message.reply_document(document=log_file, caption="**Bot Logs**") # Retry after wait
             except Exception as e_retry:
                  LOGGER.error(f"Failed to send logs after flood wait: {e_retry}")
                  await message.reply(f"**Error sending logs after wait: {e_retry}**")
        except Exception as e:
             LOGGER.error(f"Failed to send logs: {e}")
             await message.reply(f"**Error sending logs: {e}**")
    else:
        await message.reply("**Log file not found.**")

# --- Main Execution --- #

if __name__ == "__main__":
    if not os.path.isdir("Assets"):
        try:
            os.makedirs("Assets")
        except OSError as e:
             LOGGER.critical(f"Could not create Assets directory: {e}. Exiting.")
             exit(1)
        
    try:
        # Simple Flask health check (optional, can be removed if not needed)
        from flask import Flask, jsonify
        import threading
        
        app = Flask(__name__)
        @app.route("/")
        def index():
            return jsonify({"status": "running"})
        port = int(os.environ.get("PORT", 8000))
        
        def run_flask():
            LOGGER.info(f"Starting health check server on port {port}")
            try:
                app.run(host="0.0.0.0", port=port)
            except Exception as flask_err:
                 LOGGER.error(f"Flask server failed: {flask_err}", exc_info=True)
        
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        
        # Start the bot
        LOGGER.info("Starting User Client...")
        user.start()
        LOGGER.info(f"User Client started as {user.me.username}")
        PyroConf.BOT_START_TIME = time() # Record start time after clients are ready
        LOGGER.info("Starting Bot Client...")
        bot.run()
        
    except (KeyboardInterrupt, SystemExit):
        LOGGER.info("Bot stopping...")
    except Exception as err:
        LOGGER.critical(f"Bot failed to start or run: {err}", exc_info=True)
    finally:
        LOGGER.info("Stopping clients...")
        if user.is_connected:
             user.stop()
        # Bot client stops automatically on bot.run() exit
        ongoing_tasks.clear()
        LOGGER.info("Bot Stopped")

