import argparse
import os
import logging
import sqlite3
import hashlib
import itertools
from collections import defaultdict

import isodate
from tqdm.auto import tqdm

from youtube_commons.youtube_api import YouTubeAPICaller
from youtube_commons.database import VideoDatabase
from youtube_commons.download import VideoDownloader, DownloadError
from youtube_commons.transcription import WhisperXTranscriber


logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Tools for cataloging Creative Commons YouTube videos")
    parser.add_argument("--log-file", default=None, help="Path to save logs to")

    subparser = parser.add_subparsers(required=True)
    
    add_channels_parser = subparser.add_parser("add-channels", description="Add the passed channel IDs to the database")
    add_channels_parser.add_argument("--ids", required=False, default=[], nargs="+", help="Channel IDs to add")
    add_channels_parser.add_argument("--ids-file", required=False, default=None, help="File containing channel IDs to add (if very many)")
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

    download_parser = subparser.add_parser("download-videos", description="Download the audio of videos cataloged in the database")
    download_parser.add_argument("--db-path", required=True, help="Path to video database")
    download_parser.add_argument("--output-dir", required=True, help="Path to output directory")
    download_parser.add_argument("--overwrite", default=False, type=bool, help="Overwrite existing videos in the output directory")
    download_parser.add_argument("--max-videos", default=-1, type=int, help="Maximum number of videos to download")
    download_parser.add_argument("--num-shards", default=100, type=int, help="Number of total shards")
    download_parser.add_argument("--shard", required=True, type=int, help="Current shard to process")
    download_parser.set_defaults(func=download_videos)

    transcribe_parser = subparser.add_parser("transcribe-videos", description="Transcribe videos to text")
    transcribe_parser.add_argument("--input-dir", required=True, help="Path to directory containing downloaded videos")
    transcribe_parser.add_argument("--output-dir", required=True, help="Path to output directory")
    transcribe_parser.add_argument("--hf-auth-token", required=True, help="HuggingFace Hub authentication token")
    transcribe_parser.add_argument("--overwrite", default=False, type=bool, help="Overwrite existing transcriptions in the output directory")
    transcribe_parser.add_argument("--transcription-model", default="large-v2", choices=["base", "small", "medium", "large", "large-v2"], help="Whisper model to use for transcription")
    transcribe_parser.add_argument("--batch-size", default=16, type=int, help="Inference batch size")
    transcribe_parser.set_defaults(func=transcribe_videos)

    return parser.parse_args()


def setup_logger(log_file):
    # Set root logger level
    logging.getLogger().setLevel(logging.DEBUG)
    
    # Setup module logger object to output to console and optionally file
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter("[%(asctime)s] %(name)s: %(levelname)s: %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


def video_id_to_shard(video_id, num_shards):
    h = hashlib.sha256(video_id.encode())
    h_bytes = h.digest()
    h_int = int.from_bytes(h_bytes, byteorder='big')
    return h_int % num_shards
    

def add_channels(args):
    db = VideoDatabase(args.db_path)
    api_caller = YouTubeAPICaller(args.api_keys)
    
    if args.ids_file is not None:
        with open(args.ids_file, "r") as f:
            args.ids.extend([l.strip() for l in f.readlines()])

    logger.info(f"Retrieving metadata for {len(args.ids)} channels")
    metadata = api_caller.get_channel_metadata(args.ids)

    logger.info("Inserting channels into database")
    for channel_metadata in tqdm(metadata):
        channel_id = channel_metadata["id"]
        channel_name = channel_metadata["snippet"]["title"]
        db.add_channel(channel_id, channel_name)
        

def add_videos(args):
    db = VideoDatabase(args.db_path)
    api_caller = YouTubeAPICaller(args.api_keys)
    
    logger.info("Loading videos in the database")
    existing_videos = defaultdict(set)
    all_videos = db.get_all_videos()
    for video in tqdm(all_videos):
        channel_id, video_id = video[1], video[0]
        existing_videos[channel_id].add(video_id)

    for channel_id in db.get_uncompleted_channels():
        existing_video_ids = existing_videos[channel_id]
        logger.info(f"Cataloging videos form channel {channel_id} ({len(existing_video_ids)} already found)")
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
    
    logger.info(f"Channels Completed: {num_completed_channels}/{num_channels}")
    logger.info(f"Total Videos: {num_videos}")
    logger.info(f"Total Time: {total_seconds / 60 / 60:.3f} hours")
    logger.info(f"Average Channel Time: {total_seconds / 60 / 60 / num_completed_channels:.3f} hours")
    logger.info(f"Average Video Time: {total_seconds / 60 / num_videos:.3f} minutes")


def download_videos(args):
    db = VideoDatabase(args.db_path)
    videos = db.get_all_videos()
    video_ids = [video[0] for video in videos]
    
    downloader = VideoDownloader(args.output_dir)

    pbar = tqdm(video_ids)
    postfix = {"Downloaded": 0, "Previously Downloaded": 0, "Skipped": 0, "Errors": 0}

    for video_id in pbar:
        if video_id_to_shard(video_id, args.num_shards) == args.shard:
            try:
                ret = downloader.download(video_id, overwrite=args.overwrite)
            except DownloadError:
                postfix["Errors"] += 1
                continue
            
            if ret is None:
                postfix["Previously Downloaded"] += 1
            else:
                postfix["Downloaded"] += 1
        else:
            postfix["Skipped"] += 1

        if postfix["Downloaded"] == args.max_videos:
            break

        pbar.set_postfix(postfix)


def transcribe_videos(args):
    transcriber = WhisperXTranscriber(args.transcription_model, args.hf_auth_token, batch_size=args.batch_size)
    
    directories = os.listdir(args.input_dir)
    files = [[os.path.join(args.input_dir, d, f) for f in os.listdir(os.path.join(args.input_dir, d)) if f.endswith(".m4a")] for d in directories]
    for audio_file in tqdm(list(itertools.chain(*files))):
        output_file = os.path.splitext(os.path.join(args.output_dir, os.path.relpath(audio_file, start=args.input_dir)))[0] + ".txt"

        if os.path.exists(output_file) and not args.overwrite:
            logging.debug(f"Skipping transcription of {audio_file} since {output_file} already exists")
            continue
        
        output_file_dir = os.path.dirname(output_file)
        if not os.path.exists(output_file_dir):
            os.makedirs(output_file_dir)

        text = transcriber.transcribe(audio_file)
        with open(output_file, "w") as f:
            f.write(text)


def main():
    args = parse_args()
    setup_logger(args.log_file)
    logger.debug(args)
    args.func(args)
