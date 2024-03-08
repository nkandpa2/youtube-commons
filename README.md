# youtube-commons

A project cataloging and transcribing Creative Commons videos posted to YouTube

## How are we doing this?

A small fraction of videos on YouTube are published under a [Creative Commons Attribution (CC-BY) license](https://support.google.com/youtube/answer/2797468?hl=en). Although YouTube attempts to prevent this, not all of Creative Commons videos are original content so it is unclear as to whether the uploader has the right to upload that content under a permissive license. In other words, some of these Creative Commons YouTube videos are cases of [license laundering](https://en.wikipedia.org/wiki/Licence_laundering). 

To catalog only Creative Commons YouTube videos that have not been license-laundered, we take the following approach:

1. Curate a list of YouTube channels that have uploaded some content with a CC license
2. Manually filter out channels that do not upload original content
3. Collect metadata (video name, ID, etc.) for the CC videos from the remaining channels

## Codebase Usage

Once we've collected a list of channel IDs for YouTube channels with original CC content, we use the following steps to catalog the CC videos:

1. Acquire a [YouTube API key](https://developers.google.com/youtube/v3/getting-started) from your Google account
2. Clone the [youtube-commons](https://github.com/nkandpa2/youtube-commons) repository
3. Run `pip install -e .` from inside the repo to install the package's dependencies and scripts
4. Run `cc-videos add-channels --ids IDS [IDS ...] --api-keys API_KEYS [API_KEYS ...] --db-path DB_PATH`
> This command creates (or appends to) a SQLite database at `DB_PATH`. The command adds each of the passed channel IDs along with some channel metadata to the `channels` table.
5. Run `cc-videos add-videos --api-keys API_KEYS [API_KEYS ...] --db-path DB_PATH`
> This command goes through each of the uncompleted channels in the `channels` table and queries the YouTube API for all CC videos from those channels. The returned videos along with video metadata are added to the `videos` table. Once all videos from a channel have been collected, that channel is marked as completed in the `channels` table.
6. Run `cc-videos print-stats [--db-path DB_PATH]`
> This command prints some statistics about the videos cataloged so far. Some sample output is below:
```
Channels Completed: 1582/1582
Total Videos: 790485
Total Time: 299432.235 hours
Average Channel Time: 189.274 hours
Average Video Time: 22.728 minutes
```
7. Run `cc-videos download-videos --db-path DB_PATH --output-dir OUTPUT_DIR [--overwrite OVERWRITE] [--max-videos MAX_VIDEOS] [--num-shards NUM_SHARDS] --shard SHARD`
> This command downloads videos cataloged in the database. Since this step can be slow, this command can be run multiple times concurrently on different `SHARD`s. The result of this command is a collection of audio files downloaded into `OUTPUT_DIR`. The audio file for a particular video id `VIDEO_ID` can be found at `OUTPUT_DIR/VIDEO_ID[:2]/VIDEO_ID.m4a`.

