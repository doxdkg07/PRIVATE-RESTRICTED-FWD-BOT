

import os
import asyncio
from time import time
from typing import Optional
from asyncio.subprocess import PIPE
from asyncio import create_subprocess_exec, create_subprocess_shell, wait_for

from PIL import Image
from pyleaves import Leaves
from pyrogram.parser import Parser
from pyrogram.utils import get_channel_id
from pyrogram.types import (
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
    Voice,
    Message # Added for type hinting
)
from pyrogram.errors import FloodWait
from pyrogram import Client # Added for type hinting

from logger import LOGGER

SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]

# --- Flood Wait Handling (Imported or defined if not available globally) ---
# Assuming handle_flood_wait is defined in main.py or accessible globally.
# If not, it needs to be passed or redefined here.
# For simplicity, let's assume it's accessible via a shared module or passed.
# We will need to adjust the function signature to accept it or the necessary components.

# Placeholder: Define handle_flood_wait if it's not passed
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
            try: # Fallback to replying to original message
                 await message.reply(error_text)
            except Exception as e_reply:
                 LOGGER(__name__).error(f"Failed to notify user {user_id} about flood wait: {e_reply}")
    else:
        try:
            await message.reply(error_text)
        except Exception as e_reply:
            LOGGER(__name__).error(f"Failed to notify user {user_id} about flood wait: {e_reply}")
        
    # Mark task for cancellation/stop (Accessing global ongoing_tasks is problematic)
    # This function should ideally signal back to the caller to update ongoing_tasks
    # Returning a specific value or raising a custom exception might be better.
    # For now, we assume the caller checks the return value or catches a specific exception.
    # Let's modify processMediaGroup to return a specific status or raise.
    # We won't modify ongoing_tasks directly here.
    pass # Let the caller handle task state modification

# --- Utility Functions --- 

def get_readable_file_size(size_in_bytes: Optional[float]) -> str:
    if size_in_bytes is None or size_in_bytes < 0:
        return "0B"
    index = 0
    while size_in_bytes >= 1024 and index < len(SIZE_UNITS) - 1:
        size_in_bytes /= 1024
        index += 1
    return f"{size_in_bytes:.2f} {SIZE_UNITS[index]}"

def get_readable_time(seconds: int) -> str:
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days != 0:
        result += f"{days}d " # Added space
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours != 0:
        result += f"{hours}h " # Added space
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes != 0:
        result += f"{minutes}m " # Added space
    seconds = int(seconds)
    result += f"{seconds}s"
    return result.strip() # Remove trailing space


async def fileSizeLimit(file_size, message: Message, action_type="download", is_premium=False):
    MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024 if is_premium else 2 * 1024 * 1024 * 1024 # 4GB/2GB
    if file_size > MAX_FILE_SIZE:
        try:
            await message.reply(
                f"The file size ({get_readable_file_size(file_size)}) exceeds the {get_readable_file_size(MAX_FILE_SIZE)} limit and cannot be {action_type}ed."
            )
        except Exception as e:
             LOGGER(__name__).error(f"Failed to notify user about file size limit: {e}")
        return False
    return True


async def get_parsed_msg(text, entities):
    # Use html parser for better compatibility if needed, but stick to markdown for now
    return Parser.unparse(text, entities or [], is_html=False)


# Progress bar template
PROGRESS_BAR = """
**{action}**
**Percentage:** {percentage:.2f}% | {current}/{total}
**Speed:** {speed}/s | **ETA:** {est_time}
"""

def progressArgs(action: str, progress_message: Message, start_time: float):
    # Ensure progress bar format is compatible with Leaves
    return (action, progress_message, start_time)


def getChatMsgID(link: str):
    # Simplified and potentially more robust parsing
    try:
        linkps = link.strip().rstrip("/").split("/")
        if len(linkps) < 5 or linkps[2] != "t.me":
            raise ValueError("Invalid Telegram link format")

        if linkps[3] == "c": # Private channel/supergroup format: https://t.me/c/12345/6789
            if len(linkps) < 6:
                 raise ValueError("Invalid private link format")
            chat_id = get_channel_id(int(linkps[4]))
            message_id = int(linkps[5])
        else: # Public channel/group format: https://t.me/username/1234
            chat_id = linkps[3]
            message_id = int(linkps[4])
            # Handle potential thread ID if present (e.g., /username/thread_id/msg_id)
            # This bot logic doesn't seem to use thread_id, so ignore for now.
            # if len(linkps) == 6: # Assuming format /username/thread/message
            #     message_id = int(linkps[5])
            #     # thread_id = int(linkps[4]) # If needed later

        return chat_id, message_id
    except (ValueError, IndexError, TypeError) as e:
        LOGGER(__name__).error(f"Error parsing link ", exc_info=True)
        raise ValueError(f"Could not parse link: {link}. Ensure it's a valid Telegram message link.") from e


async def cmd_exec(cmd, shell=False):
    if shell:
        proc = await create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE)
    else:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    stdout = stdout.decode(errors="ignore").strip()
    stderr = stderr.decode(errors="ignore").strip()
    return stdout, stderr, proc.returncode


async def get_media_info(path):
    try:
        stdout, stderr, retcode = await cmd_exec(
            [
                "ffprobe",
                "-hide_banner",
                "-loglevel", "error", # Use error level
                "-print_format", "json",
                "-show_format",
                "-show_streams", # Also show streams for width/height
                path,
            ]
        )
        if retcode != 0:
            LOGGER(__name__).error(f"ffprobe error for {path}: {stderr}")
            return 0, None, None, None, None # duration, artist, title, width, height
            
        data = eval(stdout) # Use json.loads for safety if possible, but eval is common here
        format_info = data.get("format", {})
        duration = round(float(format_info.get("duration", 0)))
        tags = format_info.get("tags", {})
        artist = tags.get("artist") or tags.get("ARTIST") or tags.get("Artist")
        title = tags.get("title") or tags.get("TITLE") or tags.get("Title")
        
        # Get width/height from video stream if available
        width, height = None, None
        video_stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
        if video_stream:
            width = video_stream.get("width")
            height = video_stream.get("height")
            
        return duration, artist, title, width, height
        
    except FileNotFoundError:
         LOGGER(__name__).error(f"ffprobe not found. Ensure ffmpeg is installed.")
         return 0, None, None, None, None
    except Exception as e:
        LOGGER(__name__).error(f"Get Media Info error for {path}: {e}")
        return 0, None, None, None, None


async def get_video_thumbnail(video_file, duration):
    output = os.path.join("Assets", f"video_thumb_{os.path.basename(video_file)}.jpg") # Unique name
    if os.path.exists(output):
         try: os.remove(output) # Clean previous attempt
         except OSError: pass
         
    if duration is None or duration == 0:
        duration_info = (await get_media_info(video_file))[0]
        duration = duration_info if duration_info > 0 else 3 # Use fetched duration or default
        
    thumb_time = duration // 2
    if thumb_time < 1: thumb_time = 1 # Ensure time is at least 1s
        
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-ss", f"{thumb_time}",
        "-i", video_file,
        "-vf", "thumbnail,scale=320:-1", # Added scaling for smaller thumbs
        "-q:v", "2", # Good quality
        "-frames:v", "1",
        output,
    ]
    try:
        _, stderr, retcode = await wait_for(cmd_exec(cmd), timeout=60)
        if retcode != 0 or not os.path.exists(output):
            LOGGER(__name__).error(f"ffmpeg error extracting thumbnail for {video_file}: {stderr}")
            return None
    except asyncio.TimeoutError:
        LOGGER(__name__).error(f"ffmpeg timeout extracting thumbnail for {video_file}")
        return None
    except Exception as e:
         LOGGER(__name__).error(f"ffmpeg error extracting thumbnail for {video_file}: {e}")
         return None
         
    return output

# Removed unused send_media function as main.py handles sending directly

# --- processMediaGroup --- #

class FloodWaitDetected(Exception):
    "Custom exception to signal a flood wait occurred." 
    pass

async def processMediaGroup(chat_message: Message, bot: Client, user_message: Message, target_chat_id: int, user_id: int, ongoing_tasks: dict):
    """Downloads and sends a media group, handling cancellation and flood waits."""
    
    media_group_messages = []
    try:
        # Check cancellation before fetching group
        if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
            LOGGER(__name__).info(f"Task cancelled by user {user_id} before fetching media group {chat_message.media_group_id}")
            return False
            
        media_group_messages = await chat_message.get_media_group()
        if not media_group_messages:
             LOGGER(__name__).warning(f"get_media_group returned empty list for {chat_message.media_group_id}")
             return False # Nothing to process
             
    except FloodWait as fw:
        LOGGER(__name__).error(f"Flood wait getting media group {chat_message.media_group_id} for user {user_id}: {fw}")
        await handle_flood_wait(fw, user_id, user_message)
        raise FloodWaitDetected() # Signal flood wait occurred
    except Exception as e:
        LOGGER(__name__).error(f"Error getting media group {chat_message.media_group_id} for user {user_id}: {e}")
        try: await user_message.reply(f"**Error fetching media group: {e}**")
        except Exception: pass
        return False

    valid_media_to_send = []
    downloaded_paths = []
    progress_message = None
    start_time = time()

    try:
        # Send initial progress message
        try:
            progress_message = await user_message.reply(f"**📥 Downloading media group ({len(media_group_messages)} items)...**")
        except FloodWait as fw_prog:
            await handle_flood_wait(fw_prog, user_id, user_message)
            raise FloodWaitDetected()
        except Exception as e_prog:
            LOGGER(__name__).error(f"Error sending progress message for media group (user {user_id}): {e_prog}")
            # Continue without progress message if sending failed?
            pass 

        LOGGER(__name__).info(f"Downloading media group {chat_message.media_group_id} ({len(media_group_messages)} items) for user {user_id}")

        for i, msg in enumerate(media_group_messages):
            # Check cancellation before each download
            if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
                LOGGER(__name__).info(f"Task cancelled by user {user_id} during media group download at item {i+1}")
                return False # Indicate cancellation

            media_path = None
            try:
                # Update progress message
                if progress_message and (i % 5 == 0 or i == 0):
                     try:
                         await progress_message.edit(f"**📥 Downloading media group item {i+1}/{len(media_group_messages)}...**")
                     except Exception: pass # Ignore edit errors
                     
                # Determine media type and download
                if msg.photo or msg.video or msg.document or msg.audio:
                    media_path = await msg.download(
                        progress=Leaves.progress_for_pyrogram,
                        progress_args=progressArgs(f"📥 Downloading item {i+1}", progress_message, start_time)
                    )
                    downloaded_paths.append(media_path)

                    # Prepare InputMedia object
                    caption = await get_parsed_msg(msg.caption or "", msg.caption_entities)
                    if msg.photo:
                        valid_media_to_send.append(InputMediaPhoto(media=media_path, caption=caption))
                    elif msg.video:
                        # Note: Thumbnails for videos in media groups might not be handled automatically by send_media_group.
                        # Pyrogram might generate them, or they might be omitted.
                        valid_media_to_send.append(InputMediaVideo(media=media_path, caption=caption))
                    elif msg.document:
                        valid_media_to_send.append(InputMediaDocument(media=media_path, caption=caption))
                    elif msg.audio:
                        valid_media_to_send.append(InputMediaAudio(media=media_path, caption=caption))
                else:
                     LOGGER(__name__).info(f"Skipping non-media message in group: {msg.id}")
                     
            except FloodWait as fw_dl:
                LOGGER(__name__).error(f"Flood wait downloading media group item {i+1} for user {user_id}: {fw_dl}")
                await handle_flood_wait(fw_dl, user_id, user_message, progress_message)
                raise FloodWaitDetected()
            except Exception as e_dl:
                LOGGER(__name__).error(f"Error downloading media group item {i+1} (msg_id: {msg.id}) for user {user_id}: {e_dl}")
                # Don't stop the whole group for one failed item, just log and continue
                if progress_message:
                     try: await progress_message.edit(f"**⚠️ Error downloading item {i+1}. Skipping.**")
                     except Exception: pass
                await asyncio.sleep(1) # Pause briefly after error
                continue # Skip to next item
                
            # Optional sleep between downloads
            await asyncio.sleep(0.1) # Small delay

        # --- Sending Phase --- # 
        LOGGER(__name__).info(f"Downloaded {len(valid_media_to_send)} valid media items for group {chat_message.media_group_id} (user {user_id})")

        if not valid_media_to_send:
            if progress_message: await progress_message.delete()
            await user_message.reply("**❌ No valid media could be downloaded from the media group.**")
            return False

        # Check cancellation before sending
        if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
            LOGGER(__name__).info(f"Task cancelled by user {user_id} before sending media group {chat_message.media_group_id}")
            return False

        if progress_message:
             try: await progress_message.edit(f"**📤 Uploading media group ({len(valid_media_to_send)} items)...**")
             except Exception: pass
             
        try:
            # Use BOT client to send to the target chat
            await bot.send_media_group(chat_id=target_chat_id, media=valid_media_to_send)
            if progress_message: await progress_message.delete()
            progress_message = None # Mark as deleted
            LOGGER(__name__).info(f"Successfully sent media group {chat_message.media_group_id} to {target_chat_id} for user {user_id}")
            return True # Success
            
        except FloodWait as fw_send:
            LOGGER(__name__).error(f"Flood wait sending media group {chat_message.media_group_id} for user {user_id}: {fw_send}")
            await handle_flood_wait(fw_send, user_id, user_message, progress_message)
            raise FloodWaitDetected()
            
        except Exception as e_send:
            # Handle potential failure to send as a group (e.g., mixed types not supported by target client)
            LOGGER(__name__.error(f"Failed to send media group {chat_message.media_group_id} as a whole for user {user_id}: {e_send}. Trying individual uploads."))
            if progress_message:
                 try: await progress_message.edit("**⚠️ Failed to send as group. Trying individual uploads...**")
                 except Exception: pass
            else: # If no progress message, inform user
                 try: await user_message.reply("**⚠️ Failed to send as group. Trying individual uploads...**")
                 except Exception: pass
                 
            # --- Fallback: Send Individually --- #
            success_count = 0
            for i, media_input in enumerate(valid_media_to_send):
                 # Check cancellation before each individual send
                 if user_id in ongoing_tasks and ongoing_tasks[user_id]["cancel"]:
                     LOGGER(__name__).info(f"Task cancelled by user {user_id} during individual fallback send at item {i+1}")
                     return False # Indicate cancellation
                     
                 media_path = media_input.media
                 caption = media_input.caption
                 media_type = type(media_input)
                 
                 try:
                     if progress_message:
                          try: await progress_message.edit(f"**📤 Uploading item {i+1}/{len(valid_media_to_send)} individually...**")
                          except Exception: pass
                          
                     # Use appropriate send method based on type
                     if media_type is InputMediaPhoto:
                         await bot.send_photo(chat_id=target_chat_id, photo=media_path, caption=caption)
                     elif media_type is InputMediaVideo:
                         await bot.send_video(chat_id=target_chat_id, video=media_path, caption=caption)
                     elif media_type is InputMediaDocument:
                         await bot.send_document(chat_id=target_chat_id, document=media_path, caption=caption)
                     elif media_type is InputMediaAudio:
                         await bot.send_audio(chat_id=target_chat_id, audio=media_path, caption=caption)
                     # Add other types like Voice if necessary
                     # elif isinstance(media_input, Voice):
                     #     await bot.send_voice(chat_id=target_chat_id, voice=media_path, caption=caption)
                     else:
                          LOGGER(__name__).warning(f"Unsupported media type in fallback send: {media_type}")
                          continue # Skip unsupported type
                          
                     success_count += 1
                     await asyncio.sleep(0.5) # Small delay between individual sends
                     
                 except FloodWait as fw_ind:
                     LOGGER(__name__).error(f"Flood wait sending individual media item {i+1} for user {user_id}: {fw_ind}")
                     await handle_flood_wait(fw_ind, user_id, user_message, progress_message)
                     raise FloodWaitDetected()
                 except Exception as e_ind:
                     LOGGER(__name__).error(f"Failed to upload individual media item {i+1} (path: {media_path}) for user {user_id}: {e_ind}")
                     if progress_message:
                          try: await progress_message.edit(f"**❌ Failed item {i+1}. Skipping.**")
                          except Exception: pass
                     await asyncio.sleep(1)
                     continue # Skip failed item
                     
            # Final status for fallback
            if progress_message: await progress_message.delete()
            progress_message = None
            if success_count > 0:
                 await user_message.reply(f"**✅ Sent {success_count}/{len(valid_media_to_send)} items individually.**")
                 return True # Partial success is still success overall for the group processing
            else:
                 await user_message.reply(f"**❌ Failed to send any media items individually.**")
                 return False # Complete failure

    finally:
        # Cleanup downloaded files
        for path in downloaded_paths:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError as e:
                    LOGGER(__name__).warning(f"Error removing temp file {path}: {e}")
        # Cleanup progress message if it still exists
        if progress_message:
            try: await progress_message.delete()
            except Exception: pass

