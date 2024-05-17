import os
import json
import logging
import glob
import io
import os
import tempfile

import youtube_dl
import yt_dlp


logger = logging.getLogger(__name__)


class QuietLogger:
    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


class DownloadError(Exception):
    pass


class VideoDownloader:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.opts = {
            "format": "bestaudio/best",
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "m4a",
                    "preferredquality": "192",
                },
            ],
            "rate_limit": "1M",
            "quiet": True,
            "noprogress": True,
            "logger": QuietLogger(),
        }
    
    def get_output_dir(self, video_id):
        return os.path.join(self.output_dir, video_id[:2])
    
    def get_output_file_template(self, video_id):
        return os.path.join(self.get_output_dir(video_id), f"{video_id}.%(ext)s")
    
    def get_output_file(self, video_id):
        return os.path.join(self.get_output_dir(video_id), f"{video_id}.m4a")

    def get_video_url(self, video_id):
        return f"https://www.youtube.com/watch?v={video_id}"

    def download(self, video_id, overwrite=False):
        output_dir = self.get_output_dir(video_id)
        os.makedirs(output_dir, exist_ok=True)
        
        if os.path.exists(self.get_output_file(video_id)) and not overwrite:
            logger.debug(f"Skipping {video_id} -- output directory already exists")
            return None

        opts = self.opts.copy()
        opts["outtmpl"] = self.get_output_file_template(video_id)

        with yt_dlp.YoutubeDL(opts) as ydl:
            video_url = self.get_video_url(video_id)

            try:
                download_metadata = ydl.extract_info(video_url, download=True)
            except yt_dlp.utils.DownloadError as e:
                logger.debug(f"Error occurred when downloading {video_id}: {e}")
                raise DownloadError
        
        return self.get_output_file(video_id)
