# Copyright (C) @TheSmartBisnu
# Channel: https://t.me/itsSmartDev

from asyncio.subprocess import PIPE
import os
import asyncio
from time import time
from typing import Optional, List, Dict, Any
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
    Message
)

from logger import LOGGER

SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]


def get_readable_file_size(size_in_bytes: Optional[float]) -> str:
    if size_in_bytes is None or size_in_bytes < 0:
        return "0B"

    for unit in SIZE_UNITS:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

    return "File too large"


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


async def fileSizeLimit(file_size, message, action_type="download", is_premium=False):
    MAX_FILE_SIZE = 2 * 2097152000 if is_premium else 2097152000
    if file_size > MAX_FILE_SIZE:
        await message.reply(
            f"The file size exceeds the {get_readable_file_size(MAX_FILE_SIZE)} limit and cannot be {action_type}ed."
        )
        return False
    return True


async def get_parsed_msg(text, entities):
    return Parser.unparse(text, entities or [], is_html=False)


# Progress bar template
PROGRESS_BAR = """
Percentage: {percentage:.2f}% | {current}/{total}
Speed: {speed}/s
Estimated Time Left: {est_time} seconds
"""


def getChatMsgID(link: str):
    linkps = link.split("/")
    chat_id, message_thread_id, message_id = None, None, None
    if len(linkps) == 7 and linkps[3] == "c":
        # https://t.me/c/1192302355/322/487
        chat_id = get_channel_id(int(linkps[4]))
        message_thread_id = int(linkps[5])
        message_id = int(linkps[6])
    elif len(linkps) == 6:
        if linkps[3] == "c":
            # https://t.me/c/1387666944/609282
            chat_id = get_channel_id(int(linkps[4]))
            message_id = int(linkps[5])
        else:
            # https://t.me/TheForum/322/487
            chat_id = linkps[3]
            message_thread_id = int(linkps[4])
            message_id = int(linkps[5])

    elif len(linkps) == 5:
        # https://t.me/pyrogramchat/609282
        chat_id = linkps[3]
        if chat_id == "m":
            raise ValueError("Invalid ClientType used to parse this message link")
        message_id = int(linkps[4])

    return chat_id, message_id


async def cmd_exec(cmd, shell=False):
    if shell:
        proc = await create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE)
    else:
        proc = await create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = await proc.communicate()
    try:
        stdout = stdout.decode().strip()
    except:
        stdout = "Unable to decode the response!"
    try:
        stderr = stderr.decode().strip()
    except:
        stderr = "Unable to decode the error!"
    return stdout, stderr, proc.returncode


async def get_media_info(path):
    try:
        result = await cmd_exec(
            [
                "ffprobe",
                "-hide_banner",
                "-loglevel",
                "error",
                "-print_format",
                "json",
                "-show_format",
                path,
            ]
        )
    except Exception as e:
        print(f"Get Media Info: {e}. Mostly File not found! - File: {path}")
        return 0, None, None
    if result[0] and result[2] == 0:
        fields = eval(result[0]).get("format")
        if fields is None:
            print(f"get_media_info: {result}")
            return 0, None, None
        duration = round(float(fields.get("duration", 0)))
        tags = fields.get("tags", {})
        artist = tags.get("artist") or tags.get("ARTIST") or tags.get("Artist")
        title = tags.get("title") or tags.get("TITLE") or tags.get("Title")
        return duration, artist, title
    return 0, None, None


async def get_video_thumbnail(video_file, duration):
    output = os.path.join("Assets", "video_thumb.jpg")
    if duration is None:
        duration = (await get_media_info(video_file))[0]
    if duration == 0:
        duration = 3
    duration = duration // 2
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{duration}",
        "-i",
        video_file,
        "-vf",
        "thumbnail",
        "-q:v",
        "1",
        "-frames:v",
        "1",
        "-threads",
        f"{os.cpu_count() // 2}",
        output,
    ]
    try:
        _, err, code = await wait_for(cmd_exec(cmd), timeout=60)
        if code != 0 or not os.path.exists(output):
            print(
                f"Error while extracting thumbnail from video. Name: {video_file} stderr: {err}"
            )
            return None
    except:
        print(
            f"Error while extracting thumbnail from video. Name: {video_file}. Error: Timeout some issues with ffmpeg with specific arch!"
        )
        return None
    return output


# Generate progress bar for downloading/uploading
def progressArgs(action: str, progress_message, start_time):
    return (action, progress_message, start_time, PROGRESS_BAR, "▓", "░")


async def send_media(
    bot, message, media_path, media_type, caption, progress_message, start_time
):
    file_size = os.path.getsize(media_path)

    if not await fileSizeLimit(file_size, message, "upload"):
        return

    progress_args = progressArgs("📥 Uploading Progress", progress_message, start_time)
    LOGGER(__name__).info(f"Uploading media: {media_path} ({media_type})")

    if media_type == "photo":
        await message.reply_photo(
            media_path,
            caption=caption or "",
            progress=Leaves.progress_for_pyrogram,
            progress_args=progress_args,
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

        await message.reply_video(
            media_path,
            duration=duration,
            width=width,
            height=height,
            thumb=thumb,
            caption=caption or "",
            progress=Leaves.progress_for_pyrogram,
            progress_args=progress_args,
        )
    elif media_type == "audio":
        duration, artist, title = await get_media_info(media_path)
        await message.reply_audio(
            media_path,
            duration=duration,
            performer=artist,
            title=title,
            caption=caption or "",
            progress=Leaves.progress_for_pyrogram,
            progress_args=progress_args,
        )
    elif media_type == "document":
        await message.reply_document(
            media_path,
            caption=caption or "",
            progress=Leaves.progress_for_pyrogram,
            progress_args=progress_args,
        )


async def processMediaGroup(chat_message, bot, message, forward_chat_id=None):
    media_group_messages = await chat_message.get_media_group()
    valid_media = []
    temp_paths = []
    invalid_paths = []

    start_time = time()
    progress_message = await message.reply("📥 Downloading media group...")
    LOGGER(__name__).info(
        f"Downloading media group with {len(media_group_messages)} items..."
    )

    # Determine the target chat ID for sending content
    target_chat_id = forward_chat_id if forward_chat_id else message.chat.id
    
    if forward_chat_id:
        LOGGER(__name__).info(f"Media group will be forwarded to channel/group ID: {forward_chat_id}")

    # Use a semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(3)  # Allow 3 concurrent downloads
    download_tasks = []

    for msg in media_group_messages:
        if msg.photo or msg.video or msg.document or msg.audio:
            # Create a task for each media download
            task = asyncio.create_task(
                download_media_item(
                    msg, semaphore, progress_message, start_time, temp_paths, invalid_paths
                )
            )
            download_tasks.append(task)

    # Wait for all downloads to complete
    await asyncio.gather(*download_tasks)

    # Process downloaded media
    for msg, media_path in zip(media_group_messages, temp_paths):
        if not media_path or not os.path.exists(media_path):
            continue

        try:
            if msg.photo:
                valid_media.append(
                    InputMediaPhoto(
                        media=media_path,
                        caption=await get_parsed_msg(
                            msg.caption or "", msg.caption_entities
                        ),
                    )
                )
            elif msg.video:
                valid_media.append(
                    InputMediaVideo(
                        media=media_path,
                        caption=await get_parsed_msg(
                            msg.caption or "", msg.caption_entities
                        ),
                    )
                )
            elif msg.document:
                valid_media.append(
                    InputMediaDocument(
                        media=media_path,
                        caption=await get_parsed_msg(
                            msg.caption or "", msg.caption_entities
                        ),
                    )
                )
            elif msg.audio:
                valid_media.append(
                    InputMediaAudio(
                        media=media_path,
                        caption=await get_parsed_msg(
                            msg.caption or "", msg.caption_entities
                        ),
                    )
                )
        except Exception as e:
            LOGGER(__name__).error(f"Error processing downloaded media: {e}")
            if media_path and os.path.exists(media_path):
                invalid_paths.append(media_path)

    LOGGER(__name__).info(f"Valid media count: {len(valid_media)}")

    if valid_media:
        try:
            await bot.send_media_group(chat_id=target_chat_id, media=valid_media)
            await progress_message.delete()
        except Exception as e:
            LOGGER(__name__).error(f"Failed to send media group: {e}")
            await message.reply(
                "**❌ Failed to send media group, trying individual uploads**"
            )
            for media in valid_media:
                try:
                    if isinstance(media, InputMediaPhoto):
                        await bot.send_photo(
                            chat_id=target_chat_id,
                            photo=media.media,
                            caption=media.caption,
                        )
                    elif isinstance(media, InputMediaVideo):
                        await bot.send_video(
                            chat_id=target_chat_id,
                            video=media.media,
                            caption=media.caption,
                        )
                    elif isinstance(media, InputMediaDocument):
                        await bot.send_document(
                            chat_id=target_chat_id,
                            document=media.media,
                            caption=media.caption,
                        )
                    elif isinstance(media, InputMediaAudio):
                        await bot.send_audio(
                            chat_id=target_chat_id,
                            audio=media.media,
                            caption=media.caption,
                        )
                    elif isinstance(media, Voice):
                        await bot.send_voice(
                            chat_id=target_chat_id,
                            voice=media.media,
                            caption=media.caption,
                        )
                except Exception as individual_e:
                    await message.reply(
                        f"Failed to upload individual media: {individual_e}"
                    )

            await progress_message.delete()

        for path in temp_paths:
            if os.path.exists(path):
                os.remove(path)
        for path in invalid_paths:
            if os.path.exists(path):
                os.remove(path)

        return True

    await progress_message.delete()
    await message.reply("❌ No valid media found in the media group.")
    for path in invalid_paths:
        if os.path.exists(path):
            os.remove(path)
    return False


async def download_media_item(msg, semaphore, progress_message, start_time, temp_paths, invalid_paths):
    """Download a single media item with semaphore control for concurrency"""
    async with semaphore:
        try:
            if msg.video or (msg.document and msg.document.mime_type == "application/pdf"):
                # Use optimized download for videos and PDFs
                media_path = await download_media_with_speed_optimization(
                    msg,
                    progress_message=progress_message,
                    start_time=start_time
                )
            else:
                # Use standard download for other media types
                media_path = await msg.download(
                    progress=Leaves.progress_for_pyrogram,
                    progress_args=progressArgs(
                        "📥 Downloading Progress", progress_message, start_time
                    ),
                )
            
            temp_paths.append(media_path)
            return media_path
        except Exception as e:
            LOGGER(__name__).error(f"Error downloading media: {e}")
            invalid_paths.append(None)
            return None


async def download_media_with_speed_optimization(message, progress_message=None, start_time=None):
    """
    Optimized download function for large files like videos and PDFs
    Uses stream_media with larger chunk size and concurrent processing
    """
    try:
        # Create a unique filename for the download
        file_name = f"downloads/{int(time())}"
        os.makedirs("downloads", exist_ok=True)
        
        # Determine file extension
        if message.video:
            file_name += ".mp4"
        elif message.document:
            mime_type = message.document.mime_type
            if mime_type == "application/pdf":
                file_name += ".pdf"
            else:
                # Get extension from file name if available
                original_name = message.document.file_name
                if original_name and "." in original_name:
                    extension = original_name.split(".")[-1]
                    file_name += f".{extension}"
                else:
                    file_name += ".bin"  # Default binary extension
        else:
            # Default extension for other types
            file_name += ".bin"
        
        # Get file size for progress reporting
        file_size = 0
        if message.video:
            file_size = message.video.file_size
        elif message.document:
            file_size = message.document.file_size
        
        # Prepare for chunked download
        CHUNK_SIZE = 1024 * 1024 * 4  # 4MB chunks for faster download
        MAX_CONCURRENT_CHUNKS = 5  # Number of chunks to download concurrently
        
        # Calculate number of chunks
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        # Create file and pre-allocate space
        with open(file_name, "wb") as f:
            f.seek(file_size - 1)
            f.write(b'\0')
        
        # Set up semaphore for concurrent downloads
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHUNKS)
        
        # Last progress update time to avoid too frequent updates
        last_progress_time = [time()]  # Use list for mutable reference
        downloaded_bytes = [0]  # Track total downloaded bytes
        
        async def download_chunk(chunk_index):
            """Download a specific chunk of the file"""
            nonlocal file_name, CHUNK_SIZE, file_size
            
            async with semaphore:
                offset = chunk_index * CHUNK_SIZE
                limit = min(CHUNK_SIZE, file_size - offset)
                
                # Skip if beyond file size
                if offset >= file_size:
                    return
                
                try:
                    # Download chunk
                    chunk = await message.download(
                        file_name=None,  # Return bytes instead of saving
                        in_memory=True,
                        block=False,
                        offset=offset,
                        limit=limit
                    )
                    
                    # Write chunk to file at correct position
                    with open(file_name, "r+b") as f:
                        f.seek(offset)
                        f.write(chunk)
                    
                    # Update progress
                    downloaded_bytes[0] += len(chunk)
                    
                    # Update progress message (not too frequently)
                    current_time = time()
                    if progress_message and (current_time - last_progress_time[0] > 1.0):
                        last_progress_time[0] = current_time
                        elapsed = current_time - start_time
                        speed = downloaded_bytes[0] / elapsed if elapsed > 0 else 0
                        percentage = (downloaded_bytes[0] / file_size) * 100 if file_size > 0 else 0
                        
                        # Estimate time remaining
                        if speed > 0:
                            eta = (file_size - downloaded_bytes[0]) / speed
                        else:
                            eta = 0
                        
                        progress_text = f"📥 Downloading: {percentage:.1f}%\n"
                        progress_text += f"Speed: {get_readable_file_size(speed)}/s\n"
                        progress_text += f"ETA: {get_readable_time(int(eta))}"
                        
                        await progress_message.edit(progress_text)
                        
                except Exception as e:
                    LOGGER(__name__).error(f"Error downloading chunk {chunk_index}: {e}")
                    raise
        
        # Create download tasks for all chunks
        tasks = []
        for i in range(total_chunks):
            task = asyncio.create_task(download_chunk(i))
            tasks.append(task)
        
        # Wait for all chunks to download
        await asyncio.gather(*tasks)
        
        # Final progress update
        if progress_message:
            await progress_message.edit("📥 Download complete! Processing...")
        
        return file_name
        
    except Exception as e:
        LOGGER(__name__).error(f"Error in optimized download: {e}")
        # Fall back to regular download if optimized method fails
        return await message.download(
            progress=Leaves.progress_for_pyrogram if progress_message else None,
            progress_args=progressArgs(
                "📥 Downloading Progress", progress_message, start_time
            ) if progress_message else None,
        )
