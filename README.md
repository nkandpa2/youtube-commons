# youtube-commons

A project cataloging and transcribing Creative Commons videos posted to YouTube

## How are we doing this?

A small fraction of videos on YouTube are published under a [Creative Commons Attribution (CC-BY) license](https://support.google.com/youtube/answer/2797468?hl=en). Although YouTube attempts to prevent this, not all of Creative Commons videos are original content so it is unclear as to whether the uploader has the right to upload that content under a permissive license. In other words, some of these Creative Commons YouTube videos are cases of [license laundering](https://en.wikipedia.org/wiki/Licence_laundering). 

In an attempt to catalog and transcribe Creative Commons YouTube videos that have not been license-laundered, we take the following approach:

1. Curate a list of YouTube channels that have uploaded some content with a CC license and contain videos with speech
2. Manually filter out channels that do not upload original content
3. Collect metadata (video name, ID, etc.) for the CC videos from the remaining channels
4. Download and transcribe the cataloged CC videos

## Codebase Usage

To get started, clone the [youtube-commons](https://github.com/nkandpa2/youtube-commons) repository run `pip install -e .` from inside the repo to install the package's dependencies and scripts.

### Cataloging

To catalog videos, first collect a list of channel IDs for YouTube channels with original CC content. This can be done by searching YouTube with search terms for the type of video you'd like to catalog (in this case, we focus on speech-based videos by using search terms like "lecture", "symposium", "vlog", etc.) and filtering the search results to only show Creative Commons videos. You can then scroll through these results to find channels that have uploaded CC content.

Once we've collected a list of channel IDs for YouTube channels with original CC content, we use the following steps to catalog the CC videos:

1. Acquire a [YouTube API key](https://developers.google.com/youtube/v3/getting-started) from your Google account
2. Run `cc-videos add-channels --ids IDS [IDS ...] --api-keys API_KEYS [API_KEYS ...] --db-path DB_PATH`
> This command creates (or appends to) a SQLite database at `DB_PATH`. The command adds each of the passed channel IDs along with some channel metadata to the `channels` table. At the time a channel is added, it is given an "incomplete" flag indicating that videos from this channel have not been cataloged.
5. Run `cc-videos add-videos --api-keys API_KEYS [API_KEYS ...] --db-path DB_PATH`
> This command goes through each of the incomplete channels in the `channels` table and queries the YouTube API for all CC videos from those channels. The returned videos along with video metadata are added to the `videos` table. Once all videos from a channel have been collected, that channel is marked as completed in the `channels` table.
6. Run `cc-videos print-stats [--db-path DB_PATH]`
> This command prints some statistics about the videos cataloged so far. Some sample output is below:
```
Channels Completed: 1582/1582
Total Videos: 790485
Total Time: 299432.235 hours
Average Channel Time: 189.274 hours
Average Video Time: 22.728 minutes
```

### Downloading

To download videos cataloged in a video database, run `cc-videos download-videos --db-path DB_PATH --output-dir OUTPUT_DIR [--overwrite OVERWRITE] [--max-videos MAX_VIDEOS] [--num-shards NUM_SHARDS] --shard SHARD`. This command downloads videos cataloged in the database. Since this step can be slow, this command can be run multiple times concurrently on different `SHARD`s ranging from 0 to `NUM_SHARDS`. The result of this command is a collection of audio files downloaded into `OUTPUT_DIR`. The audio file for a particular video id `VIDEO_ID` can be found at `OUTPUT_DIR/VIDEO_ID[:2]/VIDEO_ID.m4a`.

### Transcribing

To transcribe downloaded videos, run `cc-videos transcribe-videos --input-dir INPUT_DIR --output-dir OUTPUT_DIR [--overwrite OVERWRITE] [--model-size {base,small,medium,large,large-v2,large-v3}] [--compute-type COMPUTE_TYPE] [--device {cpu,cuda}] [--n-procs N_PROCS]`. This command uses [faster-whisper](https://github.com/SYSTRAN/faster-whisper) to transcribe the audio files under `INPUT_DIR` and stores these transcripts as text files in `OUTPUT_DIR` using the same directory structure and file naming convention. Multiple files can be processed in parallel by specifying `N_PROCS`. Note, that each whisper process uses 4 CPU cores according to the default settings in faster-whisper, so it's a good idea to set this parameter to the (number of CPU cores available)/4.

### Downloading and Transcribing in One Shot

Audio files tend to be much larger than text transcripts. For this reason, it may be better to do the downloading and transcription of each video together so that the audio file can be deleted immediately after transcription. To do this run `cc-videos download-and-transcribe --db-path DB_PATH --output-dir OUTPUT_DIR [--overwrite OVERWRITE] [--max-videos MAX_VIDEOS] [--num-shards NUM_SHARDS] --shard SHARD [--model-size {base,small,medium,large,large-v2,large-v3}] [--compute-type COMPUTE_TYPE] [--device {cpu,cuda}] [--n-procs N_PROCS]`. This command loads the video IDs in the video database, and downloads, transcribes, and deletes the audio for each video ID. `N_PROCS` videos are handled in parallel and like the `cc-videos download-videos` command, this can also be run multiple times on different `SHARD`s.
