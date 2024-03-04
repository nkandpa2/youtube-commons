import sqlite3
import json
import logging


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(name)s: %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class VideoDatabase:
    def __init__(self, path):
        self.con = sqlite3.connect(path)
        self.cur = self.con.cursor()
        self.init_db_tables()
    
    def execute(self, *args, commit=False, **kwargs):
        ret = self.cur.execute(*args, **kwargs).fetchall()
        if commit:
            self.con.commit()
        return ret

    def init_db_tables(self):
        self.execute("""CREATE TABLE IF NOT EXISTS videos(video_id TEXT PRIMARY KEY, 
                channel_id TEXT, 
                title TEXT, 
                description TEXT, 
                tags TEXT, 
                published_time TEXT, 
                cataloged_time TEXT DEFAULT (datetime('now')),
                duration INT)""", commit=True
        )
        self.execute("""CREATE TABLE IF NOT EXISTS channels(channel_id TEXT PRIMARY KEY, 
                name TEXT, 
                completed INT DEFAULT 0)""", commit=True
        )
    
    def add_video(self, video_id, channel_id, title, description, tags, published_time, duration):
        try:
            self.execute("INSERT INTO videos(video_id, channel_id, title, description, tags, published_time, duration) VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (video_id, channel_id, title, description, json.dumps(tags), published_time, duration),
                    commit=True
            )
            return True
        except sqlite3.IntegrityError:
            logger.error(f"Failed to insert video {video_id} (already exists)")
            return False
    
    def get_video(self, video_id):
        video_result = self.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
        if len(video_result) == 0:
            return None
        else:
            video_id, channel_id, title, description, tags, published_time, cataloged_time, duration = video_result[0]
            return video_id, channel_id, title, description, json.dumps(tags), published_time, cataloged_time, duration
    
    def get_channel_video_ids(self, channel_id):
        video_result = self.execute("SELECT video_id FROM videos WHERE channel_id = ?", (channel_id,))
        return [video_id for (video_id,) in video_result]

    def get_all_videos(self):
        return self.execute("SELECT * FROM videos")

    def add_channel(self, channel_id, channel_name):
        try:
            self.execute("INSERT INTO channels(channel_id, name) VALUES(?, ?)", 
                    (channel_id, channel_name), 
                    commit=True)
            return True
        except sqlite3.IntegrityError:
            logger.error(f"Failed to insert channel {channel_id} (already exists)")
            return False
    
    def get_channel(self, channel_id):
        channel_result = self.execute("SELECT * FROM channels WHERE channel_id = ?", (channel_id,))
        if len(channel_result) == 0:
            return None
        else:
            return channel_result[0]
    
    def get_all_channels(self):
        return [channel_id for (channel_id,) in self.execute("SELECT channel_id FROM channels")]

    def get_uncompleted_channels(self):
        return [channel_id for (channel_id,) in self.execute("SELECT channel_id FROM channels WHERE completed=0")]

    def mark_channel_completed(self, channel_id):
        self.execute("UPDATE channels SET completed = 1 WHERE channel_id = ?", 
                (channel_id,),
                commit=True)
