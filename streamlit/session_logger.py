import os
from datetime import datetime
import json


class SessionLogger:
    """
    Helper class for logging user events in the Streamlit session.
    Logs are stored as structured JSON for easy parsing.
    """

    def __init__(self, logs_dir: str = "data/logs"):

        os.makedirs(logs_dir, exist_ok=True)
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file_path = os.path.join(logs_dir, f'session_{self.session_id}.jsonl')

        # Initialize the log file
        with open(self.log_file_path, 'w', encoding='utf-8') as f:
            json.dump({"event": "session_start", "timestamp": self.session_id}, f)

    def append_entry(self, entry:dict):
        """
        Append a log entry to the session log file.
        Each entry is a dictionary that will be serialized to JSON.
        """
        with open(self.log_file_path, "r+", encoding='utf-8') as f:
            logs = json.load(f)
            logs_list = logs if isinstance(logs, list) else [logs]
            logs_list.append(entry)
            f.seek(0)
            json.dump(logs_list, f, indent=2)

    def log(self, key: str, value):
        """
        Log a key-value pair with a timestamp.
        """
        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "key": key,
            "value": value
        }
        self.append_entry(entry)