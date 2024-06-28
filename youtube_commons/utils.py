import hashlib
import os


def video_id_to_shard(video_id, num_shards):
    h = hashlib.sha256(video_id.encode())
    h_bytes = h.digest()
    h_int = int.from_bytes(h_bytes, byteorder='big')
    return h_int % num_shards


def video_id_to_path(root_dir, video_id, extension):
    return os.path.join(root_dir, video_id[:2], f"{video_id}{extension}")


def get_existing_video_ids(directory, keep_exts=None, filter_exts=None):
    """
    Find video ids for existing files of the form `{directory}/{video_id[:2]}/{video_id}.{extension}`
    where `extension` is in `keep_exts` and not in `filter_exts`.
    Default behavior for `keep_exts` is keep all extensions.
    Default behavior for `filter_exts` is filter out no extensions.
    """
    existing_video_ids = set()

    # Only keep subdirectories that are two characters long
    subdirs = [s for s in os.listdir(directory) if len(s) == 2]
    for subdir in subdirs:
        # Only keep files whose name prefix matches the subdir name
        files = [f for f in os.listdir(os.path.join(directory, subdir)) if f[:2] == subdir]
        if len(files) == 0:
            continue
        # Filter out partial downloads with the .part extension
        video_ids, extensions = list(zip(*[os.path.splitext(f) for f in files]))
        keep_exts = set(extensions) if keep_exts is None else keep_exts
        filter_exts = set() if filter_exts is None else filter_exts
        video_ids = [v for v, e in zip(video_ids, extensions) if e in keep_exts and e not in filter_exts]
        existing_video_ids.update(set(video_ids))

    return existing_video_ids
