import os
import json
import logging
from glob import glob
import io
import os
import tempfile

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
            "rate_limit": "1M",
            "quiet": True,
            "noprogress": True,
            "logger": QuietLogger(),
        }
    
    def get_output_dir(self, video_id):
        return os.path.join(self.output_dir, video_id[:2])
    
    def get_output_file_tmpl(self, video_id):
        return os.path.join(self.get_output_dir(video_id), f"{video_id}.%(ext)s")
    
    def get_output_file_glob(self, video_id):
        return os.path.join(self.get_output_dir(video_id), f"{video_id}.*")

    def get_video_url(self, video_id):
        return f"https://www.youtube.com/watch?v={video_id}"

    def download(self, video_id, overwrite=False):
        output_dir = self.get_output_dir(video_id)
        os.makedirs(output_dir, exist_ok=True)
        
        if len(glob(self.get_output_file_glob(video_id))) > 0 and not overwrite:
            logger.debug(f"Skipping {video_id} -- output directory already exists")
            return None

        opts = self.opts.copy()
        opts["outtmpl"] = self.get_output_file_tmpl(video_id)

        with yt_dlp.YoutubeDL(opts) as ydl:
            video_url = self.get_video_url(video_id)

            try:
                download_metadata = ydl.extract_info(video_url, download=True)
            except yt_dlp.utils.DownloadError as e:
                logger.debug(f"Error occurred when downloading {video_id}: {e}")
                raise DownloadError
        
        download_path = ydl.prepare_filename(download_metadata)
        nbytes = os.path.getsize(download_path)
        return {"path": download_path, "nbytes": nbytes}
