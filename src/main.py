from src.triggers.watchdog_monitor_view import start_watchdog

if __name__ == "__main__":
    print("****************** Starting Data Pipeline ******************")
    start_watchdog()  # continuously monitors raw_store
    print("****************** Pipeline Finished ******************")