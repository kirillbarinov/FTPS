import time
from datetime import datetime, timedelta, UTC
import app

TARGET_HOUR = 3   # 03:00 UTC
TARGET_MIN = 0

def seconds_until_next_run():
    now = datetime.now(UTC)
    nxt = now.replace(hour=TARGET_HOUR, minute=TARGET_MIN,
                      second=0, microsecond=0)
    if nxt <= now:
        nxt += timedelta(days=1)
    return (nxt - now).total_seconds()

if __name__ == "__main__":
    # Первый запуск сразу
    try:
        print("[SCHED] First immediate run")
        app.run()
    except Exception as e:
        print(f"[SCHED] First run error: {e}")

    # Далее — строго по расписанию
    while True:
        sleep_sec = int(seconds_until_next_run())
        print(f"[SCHED] Sleeping {sleep_sec} sec until next run at {TARGET_HOUR:02d}:{TARGET_MIN:02d} UTC")
        time.sleep(sleep_sec)
        try:
            app.run()
        except Exception as e:
            print(f"[SCHED] Scheduled run error: {e}")
            time.sleep(60)
