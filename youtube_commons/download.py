import os
import json
import logging
import glob

import youtube_dl


logger = logging.getLogger(__name__)


class DownloadError(Exception):
    pass


class VideoDownloader:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.opts = {
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "m4a",
                "preferredquality": "192",
            }],
            "rate_limit": "1M",
        }
    
    def get_output_dir(self, video_id):
        return os.path.join(self.output_dir, video_id[:2])
    
    def get_output_file_template(self, video_id):
        return os.path.join(self.get_output_dir(video_id), f"{video_id}.%(ext)s")
    
    def output_file_exists(self, video_id):
        output_file = os.path.join(self.get_output_dir(video_id), f"{video_id}.m4a")
        return os.path.exists(output_file)

    def get_video_url(self, video_id):
        return f"https://www.youtube.com/watch?v={video_id}"

    def download(self, video_id, overwrite=False):
        output_dir = self.get_output_dir(video_id)
        
        if self.output_file_exists(video_id) and not overwrite:
            logger.debug(f"Skipping {video_id} -- output directory already exists")
            return None

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        opts = self.opts.copy()
        opts["outtmpl"] = self.get_output_file_template(video_id)
         
        with youtube_dl.YoutubeDL(opts) as ydl:
            video_url = self.get_video_url(video_id)

            try:
                download_metadata = ydl.extract_info(video_url, download=True)
            except youtube_dl.utils.DownloadError as e:
                logger.debug(f"Error occurred when downloading {video_id}: {e}")
                raise DownloadError
                
            return download_metadata
