import argparse
import os
import logging
import hashlib
import itertools
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import random
from glob import glob

import isodate
from tqdm.auto import tqdm
from faster_whisper import WhisperModel

from youtube_commons import utils
from youtube_commons.youtube_api import YouTubeAPICaller
from youtube_commons.database import VideoDatabase
from youtube_commons.download import VideoDownloader


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
    transcribe_parser.add_argument("--overwrite", default=False, type=bool, help="Overwrite existing transcriptions in the output directory")
    transcribe_parser.add_argument("--model-size", default="small", choices=["base", "small", "medium", "large", "large-v2", "large-v3"], help="Whisper model to use for transcription")
    transcribe_parser.add_argument("--compute-type", default="int8", help="Type to use for inference")
    transcribe_parser.add_argument("--device", default="cpu", choices=["cpu", "cuda"], help="CPU or GPU inference")
    transcribe_parser.add_argument("--n-procs", default=10, type=int, help="Number of transcription processes to run in parallel")
    transcribe_parser.set_defaults(func=transcribe_videos)

    download_and_transcribe_parser = subparser.add_parser("download-and-transcribe", description="Download videos and transcribe videos to text")
    download_and_transcribe_parser.add_argument("--db-path", required=True, help="Path to video database")
    download_and_transcribe_parser.add_argument("--output-dir", required=True, help="Path to output directory")
    download_and_transcribe_parser.add_argument("--overwrite", default=False, action="store_true", help="Overwrite existing transcriptions in the output directory")
    download_and_transcribe_parser.add_argument("--keep-audio", default=False, action="store_true", help="Keep audio files (default behavior is to discard after transcription)")

    download_and_transcribe_parser.add_argument("--max-videos", default=-1, type=int, help="Maximum number of videos to download")
    download_and_transcribe_parser.add_argument("--num-shards", default=100, type=int, help="Number of total shards")
    download_and_transcribe_parser.add_argument("--shard", required=True, type=int, help="Current shard to process")

    download_and_transcribe_parser.add_argument("--model-size", default="small", choices=["base", "small", "medium", "large", "large-v2", "large-v3"], help="Whisper model to use for transcription")
    download_and_transcribe_parser.add_argument("--compute-type", default="int8", help="Type to use for inference")
    download_and_transcribe_parser.add_argument("--device", default="cpu", choices=["cpu", "cuda"], help="CPU or GPU inference")
    download_and_transcribe_parser.add_argument("--n-procs", default=10, type=int, help="Number of transcription processes to run in parallel")
    download_and_transcribe_parser.set_defaults(func=download_and_transcribe_videos)

    return parser.parse_args()


def setup_logger(log_file):
    # Set root and module logger level
    logging.getLogger().setLevel(logging.ERROR)
    logging.getLogger("youtube_commons").setLevel(logging.DEBUG)
    
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


def download(args, video_id, output_dir):
    downloader = VideoDownloader(output_dir)
    return downloader.download(video_id, overwrite=args.overwrite)


def download_videos(args):
    output_dir = os.path.join(args.output_dir, f"shard_{args.shard}_of_{args.num_shards}")
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Loading videos database from {args.db_path}")
    db = VideoDatabase(args.db_path)
    videos = db.get_all_videos()
    video_ids = [video[0] for video in videos]

    # Filter to only video_ids that fall in this shard
    logger.info("Filtering videos")
    video_ids = list(filter(lambda v: utils.video_id_to_shard(v, args.num_shards) == args.shard, video_ids))

    # Filter to only video_ids that have not already been transcribed
    if not args.overwrite:
        existing_video_ids = utils.get_existing_video_ids(output_dir, filter_exts=[".part"])
        video_ids = [v for v in video_ids if v not in existing_video_ids]
   
    # Shuffle so that progress bar estimates aren't biased by collection order
    logger.info("Shuffling videos")
    random.shuffle(video_ids)
    
    # Compute total duration of the videos for progress bar 
    logger.info("Computing total duration of video set")
    video_ids_set = set(video_ids)
    video_duration_dict = {video[0]: video[-1] for video in videos if video[0] in video_ids_set}
    total_duration = sum(video_duration_dict.values())
    logger.info(f"Downloading {total_duration/60/60:.3f} hours of video")
 
    postfix = {"Files Downloaded": 0, "Errors": 0, "Downloaded Size (GB)": 0}
    pbar = tqdm(video_ids, total=total_duration, unit="video seconds")
    for video_id in pbar:
        try:
            download_info = download(args, video_id, output_dir)
        except:
            postfix["Errors"] += 1
            pbar.set_postfix(postfix)
            continue
        
        postfix["Downloaded Size (GB)"] += download_info["nbytes"] / 1e9
        postfix["Files Downloaded"] += 1

        if postfix["Files Downloaded"] == args.max_videos:
            break
        
        pbar.update(video_duration_dict[video_id])
        pbar.set_postfix(postfix)


def transcribe(args, file):
    model = WhisperModel(args.model_size, device=args.device, compute_type=args.compute_type)
    segments, info = model.transcribe(file, vad_filter=True, beam_size=5)
    transcript = "".join([segment.text for segment in segments])
    return {"transcript": transcript, "nbytes": len(transcript.encode("utf-8", "ignore"))}


def transcribe_videos(args):
    os.makedirs(args.output_dir, exist_ok=True)
    
    video_ids = utils.get_existing_video_ids(args.input_dir, filter_exts=[".part"])
    logger.info(f"Found {len(video_ids)} audio files")

    # Filter to only files that have not already been transcribed (if --overwrite is not specified)
    if not args.overwrite:
        already_transcribed = utils.get_existing_video_ids(args.output_dir, keep_exts=[".txt"])
        video_ids = [v for v in video_ids if v not in already_transcribed]
    logger.info(f"Transcribing {len(video_ids)} files")
    
    files = [glob(utils.video_id_to_path(args.input_dir, v, ".*"))[0] for v in video_ids]

    postfix = {"Transcribed Size (MB)": 0}
    with ProcessPoolExecutor(max_workers=args.n_procs) as executor:
        future_to_video_id = {executor.submit(transcribe, args, file): video_id for file, video_id in zip(files, video_ids)}
        
        pbar = tqdm(as_completed(future_to_video_id))
        for future in pbar:
            video_id = future_to_video_id[future]
            transcribe_info = future.result()
            
            output_file = utils.video_id_to_path(args.output_dir, video_id, ".txt")
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, "w") as f:
                f.write(transcribe_info["transcript"])

            postfix["Transcribed Size (MB)"] += transcribe_info["nbytes"] / 1e6
            pbar.set_postfix(postfix)


def download_and_transcribe(args, video_id, output_dir):
    try:
        download_info = download(args, video_id, output_dir)
    except:
        return None
    if download_info is None:
        return None
    transcribe_info = transcribe(args, download_info["path"])
    return {"download": download_info, "transcribe": transcribe_info}


def download_and_transcribe_videos(args):
    output_dir = os.path.join(args.output_dir, f"shard_{args.shard}_of_{args.num_shards}")
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Loading videos database from {args.db_path}")
    db = VideoDatabase(args.db_path)
    videos = db.get_all_videos()
    video_ids = [video[0] for video in videos]
    
    # Filter to only video_ids that fall in this shard
    logger.info("Filtering videos")
    video_ids = list(filter(lambda v: utils.video_id_to_shard(v, args.num_shards) == args.shard, video_ids))

    # Filter to only video_ids that have not already been transcribed
    if not args.overwrite:
        existing_video_ids = utils.get_existing_video_ids(output_dir, keep_exts=[".txt"])
        video_ids = [v for v in video_ids if v not in existing_video_ids]
    
    # Shuffle so that progress bar estimates aren't biased by collection order
    logger.info("Shuffling videos")
    random.shuffle(video_ids)

    # Compute total duration of the videos for progress bar 
    logger.info("Computing total duration of video set")
    video_ids_set = set(video_ids)
    video_duration_dict = {video[0]: video[-1] for video in videos if video[0] in video_ids_set}
    total_duration = sum(video_duration_dict.values())
    logger.info(f"Downloading and transcribing {total_duration/60/60:.3f} hours of video")
    
    postfix = {"Files Processed": 0, "Errors": 0, "Transcribed Size (MB)": 0, "Downloaded Size (GB)": 0}
    with ProcessPoolExecutor(max_workers=args.n_procs) as executor:
        future_to_video_id = {executor.submit(download_and_transcribe, args, video_id, output_dir): video_id for video_id in video_ids}
        
        pbar = tqdm(as_completed(future_to_video_id), total=total_duration, unit=" video seconds")
        for future in pbar:
            video_id = future_to_video_id[future]
            info = future.result()
            
            if info is not None:
                output_file = utils.video_id_to_path(output_dir, video_id, ".txt")
                with open(output_file, "w") as f:
                    f.write(info["transcribe"]["transcript"])
                postfix["Files Processed"] += 1
                postfix["Transcribed Size (MB)"] += info["transcribe"]["nbytes"] / 1e6
                postfix["Downloaded Size (GB)"] += info["download"]["nbytes"] / 1e9
                pbar.update(video_duration_dict[video_id])
            else:
                postfix["Errors"] += 1
            
            if not args.keep_audio:
                if os.path.exists(info["download"]["path"]):
                    os.remove(info["download"]["path"])

            pbar.set_postfix(postfix)
           

def main():
    args = parse_args()
    setup_logger(args.log_file)
    logger.debug(args)
    args.func(args)
