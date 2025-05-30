
import os
import asyncio
from time import time
from typing import Optional, Set # Added Set for type hinting
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

# --- Helper: Send and Delete --- #
async def send_and_delete(message: Message, text: str, delay: int = 3):
    """Sends a message and deletes it after a specified delay."""
    # This helper is now defined in main.py, assuming it's accessible if needed
    # For utils.py, we'll rely on main.py calling this for notifications
    pass

# --- Flood Wait Handling --- #
# Defined in main.py and passed or handled by raising FloodWaitDetected

class FloodWaitDetected(Exception):
    "Custom exception to signal a flood wait occurred." 
    pass

async def handle_flood_wait(fw: FloodWait, user_id: int, message: Message, status_message: Message = None):
    """Placeholder: Actual implementation is in main.py."""
    # This function in utils just serves as a placeholder for the signature
    # The actual handling logic resides in main.py
    LOGGER.error(f"[utils] Flood wait encountered for user {user_id}: {fw}")
    # We raise the custom exception to signal the caller (in main.py) to handle it
    raise FloodWaitDetected()

# --- Utility Functions --- #

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
    seconds = int(seconds)
    (days, remainder) = divmod(seconds, 86400)
    if days > 0:
        result += f"{days}d "
    (hours, remainder) = divmod(remainder, 3600)
    if hours > 0:
        result += f"{hours}h "
    (minutes, seconds) = divmod(remainder, 60)
    if minutes > 0:
        result += f"{minutes}m "
    result += f"{seconds}s"
    return result.strip() or "0s"

async def fileSizeLimit(file_size, message: Message, action_type="download", is_premium=False):
    MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024 if is_premium else 2 * 1024 * 1024 * 1024 # 4GB/2GB
    if file_size > MAX_FILE_SIZE:
        # Notification should be handled by the caller (main.py) using send_and_delete
        LOGGER.warning(f"File size {get_readable_file_size(file_size)} exceeds limit.")
        return False
    return True

async def get_parsed_msg(text, entities):
    return Parser.unparse(text, entities or [], is_html=False)

PROGRESS_BAR = """
**{action}**
**Percentage:** {percentage:.2f}% | {current}/{total}
**Speed:** {speed}/s | **ETA:** {est_time}
"""

def progressArgs(action: str, progress_message: Message, start_time: float):
    return (action, progress_message, start_time)

def getChatMsgID(link: str):
    try:
        linkps = link.strip().rstrip("/").split("/")
        if len(linkps) < 5 or linkps[2] != "t.me":
            raise ValueError("Invalid Telegram link format")
        if linkps[3] == "c":
            if len(linkps) < 6: raise ValueError("Invalid private link format")
            chat_id = get_channel_id(int(linkps[4]))
            message_id = int(linkps[5])
        else:
            chat_id = linkps[3]
            message_id = int(linkps[4])
        return chat_id, message_id
    except (ValueError, IndexError, TypeError) as e:
        LOGGER.error(f"Error parsing link ", exc_info=True)
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
        stdout, stderr, retcode = await cmd_exec([
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-print_format", "json", "-show_format", "-show_streams", path
        ])
        if retcode != 0: raise RuntimeError(f"ffprobe error: {stderr}")
        data = eval(stdout)
        format_info = data.get("format", {})
        duration = round(float(format_info.get("duration", 0)))
        tags = format_info.get("tags", {})
        artist = tags.get("artist") or tags.get("ARTIST") or tags.get("Artist")
        title = tags.get("title") or tags.get("TITLE") or tags.get("Title")
        width, height = None, None
        video_stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
        if video_stream:
            width = video_stream.get("width")
            height = video_stream.get("height")
        return duration, artist, title, width, height
    except FileNotFoundError:
         LOGGER.error(f"ffprobe not found. Ensure ffmpeg is installed.")
    except Exception as e:
        LOGGER.error(f"Get Media Info error for {path}: {e}")
    return 0, None, None, None, None

async def get_video_thumbnail(video_file, duration):
    output = os.path.join("Assets", f"video_thumb_{os.path.basename(video_file)}.jpg")
    if os.path.exists(output): try: os.remove(output); except OSError: pass
    if duration is None or duration == 0:
        duration_info = (await get_media_info(video_file))[0]
        duration = duration_info if duration_info > 0 else 3
    thumb_time = max(1, duration // 2)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-ss", f"{thumb_time}",
        "-i", video_file, "-vf", "thumbnail,scale=320:-1", "-q:v", "2",
        "-frames:v", "1", output
    ]
    try:
        _, stderr, retcode = await wait_for(cmd_exec(cmd), timeout=60)
        if retcode != 0 or not os.path.exists(output):
            raise RuntimeError(f"ffmpeg error: {stderr}")
        return output
    except asyncio.TimeoutError:
        LOGGER.error(f"ffmpeg timeout extracting thumbnail for {video_file}")
    except Exception as e:
         LOGGER.error(f"ffmpeg error extracting thumbnail for {video_file}: {e}")
    return None

# --- processMediaGroup --- #

async def processMediaGroup(chat_message: Message, bot: Client, user_message: Message, target_chat_id: int, user_id: int, ongoing_tasks: dict, processed_files: Set[str]):
    """Downloads and sends a media group, handling cancellation, flood waits, and duplicates."""
    
    media_group_messages = []
    media_group_id = chat_message.media_group_id
    
    try:
        # Check cancellation before fetching group
        if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
            LOGGER.info(f"Task cancelled by user {user_id} before fetching media group {media_group_id}")
            return False
            
        media_group_messages = await chat_message.get_media_group()
        if not media_group_messages:
             LOGGER.warning(f"get_media_group returned empty list for {media_group_id}")
             return False # Nothing to process
             
    except FloodWait as fw:
        LOGGER.error(f"Flood wait getting media group {media_group_id} for user {user_id}: {fw}")
        await handle_flood_wait(fw, user_id, user_message) # Raises FloodWaitDetected
    except Exception as e:
        LOGGER.error(f"Error getting media group {media_group_id} for user {user_id}: {e}")
        try: await user_message.reply(f"**Error fetching media group: {e}**")
        except Exception: pass
        return False

    valid_media_to_send = []
    downloaded_paths = []
    progress_message = None
    start_time = time()
    skipped_duplicate_in_group = 0

    try:
        # Send initial progress message
        try:
            progress_message = await user_message.reply(f"**📥 Downloading media group ({len(media_group_messages)} items)...**")
        except FloodWait as fw_prog:
            await handle_flood_wait(fw_prog, user_id, user_message) # Raises FloodWaitDetected
        except Exception as e_prog:
            LOGGER.error(f"Error sending progress message for media group (user {user_id}): {e_prog}")
            pass 

        LOGGER.info(f"Downloading media group {media_group_id} ({len(media_group_messages)} items) for user {user_id}")

        for i, msg in enumerate(media_group_messages):
            # Check cancellation before each download
            if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
                LOGGER.info(f"Task cancelled by user {user_id} during media group download at item {i+1}")
                return False # Indicate cancellation

            media_path = None
            file_unique_id = None
            media = None
            
            # --- Identify Media and Unique ID --- #
            if msg.photo: media = msg.photo
            elif msg.video: media = msg.video
            elif msg.audio: media = msg.audio
            elif msg.document: media = msg.document
            # Add sticker, animation, voice if needed
            
            if media and hasattr(media, "file_unique_id"):
                file_unique_id = media.file_unique_id
            
            # --- Check for Duplicates --- #
            if file_unique_id and file_unique_id in processed_files:
                LOGGER.info(f"Skipping duplicate media in group (ID: {file_unique_id}) for message {msg.id}, user {user_id}")
                skipped_duplicate_in_group += 1
                # No individual notification for skips within a group to avoid spam
                continue # Skip to next item
                
            try:
                # Update progress message
                if progress_message and (i % 5 == 0 or i == 0):
                     try: await progress_message.edit(f"**📥 Downloading group item {i+1}/{len(media_group_messages)}...**")
                     except Exception: pass
                     
                # Determine media type and download
                if media:
                    # Add to processed set *before* download attempt
                    if file_unique_id:
                         processed_files.add(file_unique_id)
                         
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
                        valid_media_to_send.append(InputMediaVideo(media=media_path, caption=caption))
                    elif msg.document:
                        valid_media_to_send.append(InputMediaDocument(media=media_path, caption=caption))
                    elif msg.audio:
                        valid_media_to_send.append(InputMediaAudio(media=media_path, caption=caption))
                else:
                     LOGGER.info(f"Skipping non-media message in group: {msg.id}")
                     
            except FloodWait as fw_dl:
                LOGGER.error(f"Flood wait downloading media group item {i+1} for user {user_id}: {fw_dl}")
                await handle_flood_wait(fw_dl, user_id, user_message, progress_message) # Raises FloodWaitDetected
            except Exception as e_dl:
                LOGGER.error(f"Error downloading media group item {i+1} (msg_id: {msg.id}) for user {user_id}: {e_dl}")
                if progress_message: try: await progress_message.edit(f"**⚠️ Error downloading item {i+1}. Skipping.**"); except Exception: pass
                # Remove from processed_files if download failed?
                # if file_unique_id and file_unique_id in processed_files:
                #      processed_files.remove(file_unique_id)
                await asyncio.sleep(1)
                continue
                
            await asyncio.sleep(0.1) # Small delay

        # --- Sending Phase --- # 
        LOGGER.info(f"Downloaded {len(valid_media_to_send)} valid media items for group {media_group_id} (user {user_id}). Skipped duplicates: {skipped_duplicate_in_group}")

        if not valid_media_to_send:
            if progress_message: await progress_message.delete()
            # Notify if *all* items were skipped as duplicates or failed
            if skipped_duplicate_in_group > 0 and skipped_duplicate_in_group == len(media_group_messages):
                 await user_message.reply("**⏩ All items in the media group were duplicates and skipped.**")
            elif not media_group_messages: # Should not happen if initial check passed
                 pass # No message if group was empty initially
            else:
                 await user_message.reply("**❌ No valid media could be downloaded/processed from the media group.**")
            return False

        # Check cancellation before sending
        if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
            LOGGER.info(f"Task cancelled by user {user_id} before sending media group {media_group_id}")
            return False

        if progress_message:
             try: await progress_message.edit(f"**📤 Uploading media group ({len(valid_media_to_send)} items)...**")
             except Exception: pass
             
        try:
            # Use BOT client to send to the target chat
            await bot.send_media_group(chat_id=target_chat_id, media=valid_media_to_send)
            if progress_message: await progress_message.delete()
            progress_message = None # Mark as deleted
            LOGGER.info(f"Successfully sent media group {media_group_id} to {target_chat_id} for user {user_id}")
            return True # Success
            
        except FloodWait as fw_send:
            LOGGER.error(f"Flood wait sending media group {media_group_id} for user {user_id}: {fw_send}")
            await handle_flood_wait(fw_send, user_id, user_message, progress_message) # Raises FloodWaitDetected
            
        except Exception as e_send:
            LOGGER.error(f"Failed to send media group {media_group_id} as a whole for user {user_id}: {e_send}. Trying individual uploads.")
            if progress_message: try: await progress_message.edit("**⚠️ Failed to send as group. Trying individual uploads...**"); except Exception: pass
            else: try: await user_message.reply("**⚠️ Failed to send as group. Trying individual uploads...**"); except Exception: pass
                 
            # --- Fallback: Send Individually --- #
            success_count = 0
            for i, media_input in enumerate(valid_media_to_send):
                 if user_id not in ongoing_tasks or ongoing_tasks[user_id]["cancel"]:
                     LOGGER.info(f"Task cancelled by user {user_id} during individual fallback send at item {i+1}")
                     return False
                     
                 media_path = media_input.media
                 caption = media_input.caption
                 media_type = type(media_input)
                 
                 try:
                     if progress_message: try: await progress_message.edit(f"**📤 Uploading item {i+1}/{len(valid_media_to_send)} individually...**"); except Exception: pass
                          
                     if media_type is InputMediaPhoto: await bot.send_photo(chat_id=target_chat_id, photo=media_path, caption=caption)
                     elif media_type is InputMediaVideo: await bot.send_video(chat_id=target_chat_id, video=media_path, caption=caption)
                     elif media_type is InputMediaDocument: await bot.send_document(chat_id=target_chat_id, document=media_path, caption=caption)
                     elif media_type is InputMediaAudio: await bot.send_audio(chat_id=target_chat_id, audio=media_path, caption=caption)
                     else: LOGGER.warning(f"Unsupported media type in fallback send: {media_type}"); continue
                          
                     success_count += 1
                     await asyncio.sleep(0.5)
                     
                 except FloodWait as fw_ind:
                     LOGGER.error(f"Flood wait sending individual media item {i+1} for user {user_id}: {fw_ind}")
                     await handle_flood_wait(fw_ind, user_id, user_message, progress_message) # Raises FloodWaitDetected
                 except Exception as e_ind:
                     LOGGER.error(f"Failed to upload individual media item {i+1} (path: {media_path}) for user {user_id}: {e_ind}")
                     if progress_message: try: await progress_message.edit(f"**❌ Failed item {i+1}. Skipping.**"); except Exception: pass
                     await asyncio.sleep(1)
                     continue
                     
            # Final status for fallback
            if progress_message: await progress_message.delete()
            progress_message = None
            if success_count > 0:
                 await user_message.reply(f"**✅ Sent {success_count}/{len(valid_media_to_send)} items individually.**")
                 return True
            else:
                 await user_message.reply(f"**❌ Failed to send any media items individually.**")
                 return False

    finally:
        # Cleanup downloaded files
        for path in downloaded_paths:
            if os.path.exists(path):
                try: os.remove(path)
                except OSError as e: LOGGER.warning(f"Error removing temp file {path}: {e}")
        # Cleanup progress message
        if progress_message: try: await progress_message.delete(); except Exception: pass

