import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.pipeline_scripts.bronze_store_view import extract_and_store_bronze
from src.pipeline_scripts.silver_store_view import transform_bronze_to_silver_with_metadata
from src.pipeline_scripts.gold_store_view import aggregate_gold

RAW_FOLDER = os.getenv("RAW_STORE_DIR", "ed_raw/raw_store")
BRONZE_FOLDER = os.getenv("BRONZE_DIR", "ed_raw/bronze_store")
SILVER_FOLDER = os.getenv("SILVER_DIR", "ed_raw/silver_store")
GOLD_FOLDER = os.getenv("GOLD_DIR", "ed_raw/gold_store")

class RawFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".csv"):
            print(f"New raw file detected: {event.src_path}")
            bronze_file = extract_and_store_bronze(event.src_path, BRONZE_FOLDER)
            silver_file = transform_bronze_to_silver_with_metadata(bronze_file, SILVER_FOLDER)
            gold_file = aggregate_gold(silver_file, GOLD_FOLDER)
            print(f"✅ Finished processing: {event.src_path}")

def start_watchdog():
    observer = Observer()
    handler = RawFileHandler()
    observer.schedule(handler, path=RAW_FOLDER, recursive=False)
    observer.start()
    print(f"🕵️ Watchdog started. Monitoring folder: {RAW_FOLDER}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("🛑 Watchdog stopped by user.")
    observer.join()
