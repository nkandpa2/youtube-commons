from functools import wraps
from urllib.error import HTTPError
import logging

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(name)s: %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def batched(n):
    def decorator(func):
        @wraps(func)
        def wrapper(self, lst, *args, **kwargs):
            ret = []
            for i in range(0, len(lst), n):
                ret.extend(func(self, lst[i:i+n], *args, **kwargs))
            return ret
        return wrapper
    return decorator


def cycle_api_keys(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except HttpError as e:
            if e.status_code == 403:
                if len(self.api_keys) == 0:
                    raise Exception("No more API keys")
                logger.info("Cycling API keys")
                self.init_caller()
                return func(self, *args, **kwargs)
            else:
                raise

    return wrapper
        

class YouTubeAPICaller:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.init_caller()

    def init_caller(self):
        self.caller = build("youtube", "v3", developerKey=self.api_keys.pop())
    
    @cycle_api_keys
    @batched(50)
    def get_channel_metadata(self, channel_ids):
        if len(channel_ids) == 0:
            return []

        request = self.caller.channels().list(part="snippet", id=channel_ids)
        metadata = request.execute()
        for item in metadata["items"]:
            yield item
    
    @cycle_api_keys
    @batched(50)
    def get_video_metadata(self, video_ids):
        if len(video_ids) == 0:
            return []

        request = self.caller.videos().list(part="snippet,contentDetails,status", id=video_ids)
        metadata = request.execute()
        for item in metadata["items"]:
            yield item

    @cycle_api_keys
    def get_cc_videos_from_channel(self, channel_id, skip_video_ids=set()):
        request = self.caller.channels().list(part="contentDetails", id=channel_id)
        channel_metadata = request.execute()

        uploads_playlist_id = channel_metadata["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        next_page_token = None
        while True:
            request = uploads_playlist_metadata = self.caller.playlistItems().list(part="contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
            )
            response = request.execute()

            video_ids = [item["contentDetails"]["videoId"] for item in response["items"] if item["contentDetails"]["videoId"] not in skip_video_ids]
            for video_metadata in self.get_video_metadata(video_ids):
                if video_metadata["status"]["license"] == "creativeCommon":
                    yield video_metadata
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
