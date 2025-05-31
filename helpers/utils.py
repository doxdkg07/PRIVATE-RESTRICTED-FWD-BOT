
import asyncio
from asyncio.subprocess import PIPE
import os
from time import time
from typing import Optional, List
# from asyncio import create_subprocess_exec, create_subprocess_shell, wait_for # No longer needed
# from PIL import Image # No longer needed
# from pyleaves import Leaves # No longer needed if progress removed
from pyrogram.parser import Parser
from pyrogram.utils import get_channel_id
from pyrogram import Client # Added Client type hint
from pyrogram.errors import FloodWait # Added
from pyrogram.types import (
    Message, # Added Message type hint
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio,
    # Voice, # Not handled in original, keep it out for now
)

from logger import LOGGER

SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]


def get_readable_file_size(size_in_bytes: Optional[float]) -> str:
    if size_in_bytes is None or size_in_bytes < 0:
        return "0B"

    index = 0
    while size_in_bytes >= 1024 and index < len(SIZE_UNITS) - 1:
        size_in_bytes /= 1024
        index += 1
    return f"{size_in_bytes:.2f} {SIZE_UNITS[index]}" if index < len(SIZE_UNITS) else "File too large"


def get_readable_time(seconds: int) -> str:
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days != 0:
        result += f"{days}d"
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours != 0:
        result += f"{hours}h"
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes != 0:
        result += f"{minutes}m"
    seconds = int(seconds)
    result += f"{seconds}s"
    return result


async def fileSizeLimit(file_size, message: Message, action_type="download", is_premium=False):
    # Consider making MAX_FILE_SIZE configurable
    MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024 if is_premium else 2 * 1024 * 1024 * 1024 # 4GB/2GB
    if file_size > MAX_FILE_SIZE:
        try:
            await message.reply(
                f"File size ({get_readable_file_size(file_size)}) exceeds the {get_readable_file_size(MAX_FILE_SIZE)} limit and cannot be {action_type}ed."
            )
        except Exception as e:
             LOGGER(__name__).error(f"Error replying about file size limit: {e}")
        return False
    return True


async def get_parsed_msg(text, entities):
    # Handles parsing message text/captions with entities
    return Parser.unparse(text, entities or [], is_html=False)


# Removed PROGRESS_BAR and progressArgs as progress reporting is simplified/removed

def getChatMsgID(link: str):
    """Parses Telegram message links (t.me/c/../.. or t.me/../..) to extract chat_id and message_id."""
    linkps = link.split("/")
    chat_id_str = None
    message_id = None

    try:
        if linkps[2] != "t.me":
            raise ValueError("Invalid domain")

        if len(linkps) >= 6 and linkps[3] == "c":
            # Private channel/group link: https://t.me/c/1234567890/123
            # Or with thread ID: https://t.me/c/1234567890/123/456 (ignore thread ID for now)
            chat_id_str = get_channel_id(int(linkps[4])) # Convert to peer id -100... format
            message_id = int(linkps[5])
        elif len(linkps) >= 5:
            # Public channel/group link: https://t.me/public_username/123
            # Or with thread ID: https://t.me/public_username/123/456 (ignore thread ID for now)
            chat_id_str = linkps[3]
            message_id = int(linkps[4])
        else:
            raise ValueError("Link format not recognized")

        if not chat_id_str or not message_id:
             raise ValueError("Could not extract chat_id or message_id")

        return chat_id_str, message_id

    except (IndexError, ValueError, TypeError) as e:
        LOGGER(__name__).error(f"Error parsing link 	'{link}	': {e}")
        raise ValueError(f"Invalid Telegram message link format: {link}") from e

# Removed cmd_exec, get_media_info, get_video_thumbnail, send_media

# --- Optimized processMediaGroup --- 
async def processMediaGroup(first_message: Message, bot: Client, cmd_message: Message, user: Client, target_chat_id: int):
    """Processes a media group, prioritizing copy_media_group, with a concurrent download/upload fallback."""
    group_id = first_message.media_group_id
    log_prefix = f"[GrpID: {group_id}]"
    
    # --- Optimization 1: Attempt Direct Copy using User Client --- 
    try:
        LOGGER(__name__).info(f"{log_prefix} Attempting direct copy_media_group to {target_chat_id}...")
        copied_messages = await user.copy_media_group(
            chat_id=target_chat_id,
            from_chat_id=first_message.chat.id,
            message_id=first_message.id, # Pyrogram handles finding the group from one message
        )
        if copied_messages:
            LOGGER(__name__).info(f"{log_prefix} Direct copy_media_group successful ({len(copied_messages)} messages).")
            await asyncio.sleep(0.8) # Slightly longer delay for groups
            return True # Successfully copied
        else:
            LOGGER(__name__).warning(f"{log_prefix} Direct copy_media_group attempt returned None/False/Empty List.")

    except FloodWait as e:
        LOGGER(__name__).warning(f"{log_prefix} FloodWait during copy_media_group: waiting {e.value}s")
        await asyncio.sleep(e.value + 1)
        # Retry copy once after flood wait
        try:
            LOGGER(__name__).info(f"{log_prefix} Retrying copy_media_group after FloodWait...")
            copied_messages = await user.copy_media_group(
                chat_id=target_chat_id,
                from_chat_id=first_message.chat.id,
                message_id=first_message.id,
            )
            if copied_messages:
                LOGGER(__name__).info(f"{log_prefix} Direct copy_media_group successful after retry ({len(copied_messages)} messages).")
                await asyncio.sleep(0.8)
                return True
            else:
                 LOGGER(__name__).warning(f"{log_prefix} Direct copy_media_group retry returned None/False/Empty List.")
        except FloodWait as e2:
             LOGGER(__name__).error(f"{log_prefix} FloodWait again on copy_media_group retry: {e2.value}s. Giving up on copy.")
        except Exception as retry_err:
             LOGGER(__name__).error(f"{log_prefix} Direct copy_media_group retry failed: {retry_err}. Falling back.")

    except Exception as copy_err:
        # Handle cases where copy_media_group might not be supported (e.g., across different DC restrictions?)
        LOGGER(__name__).warning(f"{log_prefix} Direct copy_media_group failed: {type(copy_err).__name__} - {copy_err}. Falling back to download/upload.")

    # --- Fallback: Concurrent Download (User) + Upload (Bot) --- 
    LOGGER(__name__).info(f"{log_prefix} Proceeding with download/upload fallback for media group.")
    media_group_messages: List[Message] = []
    try:
        # Fetch all messages in the media group using the user client
        media_group_messages = await user.get_media_group(first_message.chat.id, first_message.id)
        if not media_group_messages:
             LOGGER(__name__).error(f"{log_prefix} Failed to fetch media group messages for fallback.")
             return False
        LOGGER(__name__).info(f"{log_prefix} Fetched {len(media_group_messages)} messages for fallback processing.")
    except Exception as fetch_err:
         LOGGER(__name__).error(f"{log_prefix} Error fetching media group messages: {fetch_err}")
         return False

    # --- Concurrent Download --- 
    CONCURRENCY_LIMIT = 5 # Lower limit for downloads
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    download_tasks = []
    downloaded_items = {} # Store {msg_id: (path, InputMedia type)} 
    failed_downloads = []
    temp_paths_to_clean = []

    async def download_item(msg: Message):
        nonlocal downloaded_items, failed_downloads, temp_paths_to_clean
        item_log_prefix = f"{log_prefix}[MsgID: {msg.id}]"
        media_path = None
        async with semaphore:
            try:
                # Check file size before download
                file_size = getattr(msg.document or msg.video or msg.audio or msg.photo, "file_size", 0)
                if file_size and not await fileSizeLimit(file_size, cmd_message, f"download group item (fallback)", user.me.is_premium):
                    failed_downloads.append(msg.id)
                    return

                LOGGER(__name__).info(f"{item_log_prefix} Downloading media for group fallback...")
                # Download using USER client
                media_path = await user.download_media(message=msg)
                if not media_path or not os.path.exists(media_path):
                    raise Exception(f"Download failed or returned invalid path: {media_path}")
                
                temp_paths_to_clean.append(media_path) # Add path for cleanup
                parsed_caption = await get_parsed_msg(msg.caption or "", msg.caption_entities)
                
                # Create InputMedia object
                input_media = None
                if msg.photo:
                    input_media = InputMediaPhoto(media=media_path, caption=parsed_caption)
                elif msg.video:
                    # Note: Pyrogram might handle thumb/duration automatically for InputMediaVideo
                    input_media = InputMediaVideo(media=media_path, caption=parsed_caption)
                elif msg.audio:
                    input_media = InputMediaAudio(media=media_path, caption=parsed_caption)
                elif msg.document:
                    input_media = InputMediaDocument(media=media_path, caption=parsed_caption)
                
                if input_media:
                    downloaded_items[msg.id] = input_media
                    LOGGER(__name__).info(f"{item_log_prefix} Downloaded and prepared InputMedia: {media_path}")
                else:
                    raise Exception("Unsupported media type in group fallback.")

            except FloodWait as e:
                LOGGER(__name__).warning(f"{item_log_prefix} FloodWait during group download: waiting {e.value}s")
                await asyncio.sleep(e.value + 1)
                failed_downloads.append(msg.id)
            except Exception as down_err:
                LOGGER(__name__).error(f"{item_log_prefix} Group download fallback failed: {down_err}", exc_info=True)
                failed_downloads.append(msg.id)
                # Clean up partially downloaded file if path exists
                if media_path and os.path.exists(media_path) and media_path not in temp_paths_to_clean:
                     temp_paths_to_clean.append(media_path)

    # Create and run download tasks
    for msg in media_group_messages:
        if msg.media: # Only process messages with media
            download_tasks.append(asyncio.create_task(download_item(msg)))
    
    if download_tasks:
        await asyncio.gather(*download_tasks)

    # --- Prepare Media Group for Upload --- 
    # Order matters for send_media_group, use original message order
    media_to_upload = []
    for msg in media_group_messages:
        if msg.id in downloaded_items:
            media_to_upload.append(downloaded_items[msg.id])

    # --- Upload Media Group using Bot Client --- 
    upload_successful = False
    if media_to_upload:
        LOGGER(__name__).info(f"{log_prefix} Attempting to upload {len(media_to_upload)} downloaded items as a group using bot...")
        try:
            await bot.send_media_group(chat_id=target_chat_id, media=media_to_upload)
            LOGGER(__name__).info(f"{log_prefix} Media group fallback upload successful.")
            upload_successful = True
            await asyncio.sleep(0.8) # Delay after successful group upload
        except FloodWait as e:
            LOGGER(__name__).warning(f"{log_prefix} FloodWait during group upload: waiting {e.value}s")
            await asyncio.sleep(e.value + 1)
            # Don't retry group upload automatically, mark as failed
        except Exception as upload_err:
            LOGGER(__name__).error(f"{log_prefix} Failed to send media group (fallback): {upload_err}", exc_info=True)
            # Optional: Could try sending individually here, but adds complexity
            try:
                 await cmd_message.reply(f"**{log_prefix} Failed to send media group via fallback: {upload_err}**")
            except Exception: pass
    elif failed_downloads:
         LOGGER(__name__).warning(f"{log_prefix} No media successfully downloaded for group fallback. Failed IDs: {failed_downloads}")
         try:
             await cmd_message.reply(f"**{log_prefix} Failed to download items for media group fallback.**")
         except Exception: pass
    else:
         LOGGER(__name__).info(f"{log_prefix} No media found in the fetched group messages to process.")

    # --- Cleanup --- 
    LOGGER(__name__).info(f"{log_prefix} Cleaning up {len(temp_paths_to_clean)} temporary group files...")
    cleaned_count = 0
    for path in temp_paths_to_clean:
        try:
            if os.path.exists(path):
                os.remove(path)
                cleaned_count += 1
        except OSError as rm_err:
            LOGGER(__name__).error(f"{log_prefix} Error removing temp file {path}: {rm_err}")
    LOGGER(__name__).info(f"{log_prefix} Cleaned up {cleaned_count} files.")

    return upload_successful

