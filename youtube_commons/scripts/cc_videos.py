import argparse
import os
import logging
import sqlite3

import isodate
from tqdm.auto import tqdm

from yt_commons.youtube_api import YouTubeAPICaller
from yt_commons.database import VideoDatabase


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(name)s: %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Tools for cataloging Creative Commons YouTube videos")
    subparser = parser.add_subparsers(required=True)

    add_channels_parser = subparser.add_parser("add-channels", description="Add the passed channel IDs to the database")
    add_channels_parser.add_argument("--ids", required=True, nargs="+", help="Channel IDs to add")
    add_channels_parser.add_argument("--api-keys", required=True, nargs="+", help="YouTube Developer API keys")
    add_channels_parser.add_argument("--db-path", required=True, help="Path to output database")
    add_channels_parser.set_defaults(func=add_channels)

    add_videos_parser = subparser.add_parser("add-videos", description="Add videos from unfinished channels to the database")
    add_videos_parser.add_argument("--api-keys", required=True, nargs="+", help="YouTube Developer API keys")
    add_videos_parser.add_argument("--db-path", required=True, help="Path to output database")
    add_videos_parser.set_defaults(func=add_videos)
    
    print_stats_parser = subparser.add_parser("print-stats", description="Print statistics about cataloged videos and channels")
    print_stats_parser.add_argument("--db-path")
    print_stats_parser.set_defaults(func=print_stats)

    return parser.parse_args()


def add_channels(args):
    db = VideoDatabase(args.db_path)
    api_caller = YouTubeAPICaller(args.api_keys)
    
    logging.info(f"Retrieving metadata for {len(args.ids)} channels")
    metadata = api_caller.get_channel_metadata(args.ids)

    logging.info("Inserting channels into database")
    for channel_metadata in tqdm(metadata):
        channel_id = channel_metadata["id"]
        channel_name = channel_metadata["snippet"]["title"]
        db.add_channel(channel_id, channel_name)
        

def add_videos(args):
    db = VideoDatabase(args.db_path)
    api_caller = YouTubeAPICaller(args.api_keys)
    
    for channel_id in db.get_uncompleted_channels():
        existing_video_ids = set(db.get_channel_video_ids(channel_id))
        logging.info(f"Cataloging videos form channel {channel_id} ({len(existing_video_ids)} already found)")
        for video_metadata in tqdm(api_caller.get_cc_videos_from_channel(channel_id, skip_video_ids=existing_video_ids)):
            db.add_video(video_metadata["id"],
                    channel_id,
                    video_metadata["snippet"].get("title"),
                    video_metadata["snippet"].get("description"),
                    video_metadata["snippet"].get("tags"),
                    video_metadata["snippet"].get("publishedAt"),
                    isodate.parse_duration(video_metadata["contentDetails"].get("duration")).total_seconds()
            )

        db.mark_channel_completed(channel_id)
 

def print_stats(args):
    db = VideoDatabase(args.db_path)
    num_channels = len(db.get_all_channels())
    num_uncompleted_channels = len(db.get_uncompleted_channels())
    num_completed_channels = num_channels - num_uncompleted_channels
    videos = db.get_all_videos()
    num_videos = len(videos)
    total_seconds = sum([v[-1] for v in videos])

    print(f"Channels Completed: {num_completed_channels}/{num_channels}")
    print(f"Total Videos: {num_videos}")
    print(f"Total Time: {total_seconds / 60 / 60:.3f} hours")
    print(f"Average Channel Time: {total_seconds / 60 / 60 / num_completed_channels:.3f} hours")
    print(f"Average Video Time: {total_seconds / 60 / num_videos:.3f} minutes")


def main():
    args = parse_args()
    args.func(args)
