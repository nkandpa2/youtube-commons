from functools import wraps
from urllib.error import HTTPError
import logging

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


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
        while len(self.api_keys) > 0:
            try:
                return func(self, *args, **kwargs)
            except HttpError as e:
                if e.status_code == 403:
                    self.init_caller()
                    logger.info(f"Cycled API keys. {len(self.api_keys) + 1} keys remaining.")
                else:
                    raise e

        raise Exception("No more API keys")
    return wrapper
        

class YouTubeAPICaller:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.init_caller()

    def init_caller(self):
        self.caller = build("youtube", "v3", developerKey=self.api_keys.pop())
    
    @cycle_api_keys
    def channels_api_call(self, part, channel_ids):
        request = self.caller.channels().list(part=part, id=channel_ids)
        metadata = request.execute()
        return metadata
    
    @cycle_api_keys
    def videos_api_call(self, part, video_ids):
        request = self.caller.videos().list(part=part, id=video_ids)
        metadata = request.execute()
        return metadata
    
    @cycle_api_keys
    def playlist_api_call(self, part, playlist_id, next_page_token):
        request = self.caller.playlistItems().list(part=part,
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
            )
        response = request.execute()
        return response


    @batched(50)
    def get_channel_metadata(self, channel_ids):
        if len(channel_ids) == 0:
            return

        metadata = self.channels_api_call("snippet", channel_ids)
        for item in metadata["items"]:
            yield item
    
    @batched(50)
    def get_video_metadata(self, video_ids):
        if len(video_ids) == 0:
            return 
        
        metadata = self.videos_api_call("snippet,contentDetails,status", video_ids)
        for item in metadata["items"]:
            yield item

    def get_cc_videos_from_channel(self, channel_id, skip_video_ids=set()):
        channel_metadata = self.channels_api_call("contentDetails", channel_id)
        uploads_playlist_id = channel_metadata["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        next_page_token = None
        while True:
            response = self.playlist_api_call("contentDetails", uploads_playlist_id, next_page_token)
            video_ids = [item["contentDetails"]["videoId"] for item in response["items"] if item["contentDetails"]["videoId"] not in skip_video_ids]
            for video_metadata in self.get_video_metadata(video_ids):
                if video_metadata["status"]["license"] == "creativeCommon":
                    yield video_metadata
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
