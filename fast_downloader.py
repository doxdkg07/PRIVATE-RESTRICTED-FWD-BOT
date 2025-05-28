import os
import asyncio
import aiofiles
from time import time
from typing import Optional, Tuple, Dict, Any
from pyrogram.types import Message

# For progress reporting
from helpers.utils import get_readable_file_size, LOGGER

class FastDownloader:
    """
    Optimized downloader for MP4 videos with concurrent chunked downloads
    """
    
    def __init__(self, chunk_size: int = 4*1024*1024, max_concurrent_chunks: int = 5):
        """
        Initialize the fast downloader
        
        Args:
            chunk_size: Size of each chunk in bytes (default: 4MB)
            max_concurrent_chunks: Maximum number of concurrent chunk downloads (default: 5)
        """
        self.chunk_size = chunk_size  # 4MB chunks by default
        self.max_concurrent_chunks = max_concurrent_chunks
        self.download_dir = "downloads"
        os.makedirs(self.download_dir, exist_ok=True)
    
    async def download_video(self, 
                           message: Message, 
                           progress_callback=None, 
                           progress_args=None) -> str:
        """
        Download a video using concurrent chunked downloads
        
        Args:
            message: Pyrogram Message object containing the video
            progress_callback: Optional callback function for progress updates
            progress_args: Optional arguments for progress callback
            
        Returns:
            Path to the downloaded file
        """
        if not message.video and not (message.document and message.document.mime_type and 'video' in message.document.mime_type):
            # Fall back to regular download for non-video files
            return await message.download(progress=progress_callback, progress_args=progress_args)
        
        # Get file info
        file_id = message.video.file_id if message.video else message.document.file_id
        file_size = message.video.file_size if message.video else message.document.file_size
        file_name = f"{int(time())}.mp4"  # Generate unique filename
        file_path = os.path.join(self.download_dir, file_name)
        
        # Calculate number of chunks
        total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size
        
        # Pre-allocate file space
        async with aiofiles.open(file_path, 'wb') as f:
            await f.seek(file_size - 1)
            await f.write(b'\0')
        
        # Set up semaphore for concurrent downloads
        semaphore = asyncio.Semaphore(self.max_concurrent_chunks)
        
        # Track progress
        downloaded_bytes = 0
        last_progress_time = time()
        
        # Create a lock for updating progress
        progress_lock = asyncio.Lock()
        
        async def download_chunk(chunk_index: int):
            """Download a specific chunk of the file"""
            nonlocal downloaded_bytes, last_progress_time
            
            async with semaphore:
                offset = chunk_index * self.chunk_size
                limit = min(self.chunk_size, file_size - offset)
                
                # Skip if beyond file size
                if offset >= file_size:
                    return
                
                try:
                    # Download chunk
                    chunk = await message._client.download_media(
                        message=message,
                        file_name=None,
                        in_memory=True,
                        block=False,
                        offset=offset,
                        limit=limit
                    )
                    
                    # Write chunk to file at correct position
                    async with aiofiles.open(file_path, "r+b") as f:
                        await f.seek(offset)
                        await f.write(chunk)
                    
                    # Update progress
                    async with progress_lock:
                        downloaded_bytes += len(chunk)
                        
                        # Update progress (not too frequently)
                        current_time = time()
                        if progress_callback and (current_time - last_progress_time > 1.0):
                            last_progress_time = current_time
                            
                            # Calculate progress
                            progress_percentage = (downloaded_bytes / file_size) * 100
                            speed = downloaded_bytes / (current_time - progress_args[2]) if progress_args else 0
                            
                            # Call progress callback
                            await progress_callback(
                                downloaded_bytes, 
                                file_size,
                                *progress_args,
                                speed=get_readable_file_size(speed) if speed > 0 else "N/A"
                            )
                            
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
        if progress_callback and progress_args:
            await progress_callback(file_size, file_size, *progress_args)
        
        return file_path
    
    async def download_multiple_videos(self, 
                                     messages: list, 
                                     progress_callback=None, 
                                     progress_args=None) -> list:
        """
        Download multiple videos concurrently
        
        Args:
            messages: List of Pyrogram Message objects containing videos
            progress_callback: Optional callback function for progress updates
            progress_args: Optional arguments for progress callback
            
        Returns:
            List of paths to downloaded files
        """
        # Filter video messages
        video_messages = [
            msg for msg in messages 
            if msg.video or (msg.document and msg.document.mime_type and 'video' in msg.document.mime_type)
        ]
        
        # Set up semaphore for concurrent downloads (limit to 3 concurrent videos)
        video_semaphore = asyncio.Semaphore(3)
        
        async def download_video_with_semaphore(msg):
            async with video_semaphore:
                return await self.download_video(msg, progress_callback, progress_args)
        
        # Create download tasks
        tasks = [asyncio.create_task(download_video_with_semaphore(msg)) for msg in video_messages]
        
        # Wait for all downloads to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        file_paths = [path for path in results if isinstance(path, str)]
        
        return file_paths
