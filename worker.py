# worker.py — простой планировщик без сторонних либ
import time
from datetime import datetime, timedelta
import app  # наш app.py с функцией run()

TARGET_HOUR = 3
TARGET_MIN = 0

def seconds_until_next_run():
    now = datetime.utcnow()
    nxt = now.replace(hour=TARGET_HOUR, minute=TARGET_MIN, second=0, microsecond=0)
    if nxt <= now:
        nxt += timedelta(days=1)
    return (nxt - now).total_seconds()

if __name__ == "__main__":
    # 1) Первый прогон сразу (чтобы проверить)
    try:
        print("[SCHED] First immediate run")
        app.run()
    except Exception as e:
        print(f"[SCHED] First run error: {e}")

    # 2) Далее — строго раз в сутки в 03:00 UTC
    while True:
        sleep_sec = int(seconds_until_next_run())
        print(f"[SCHED] Sleeping {sleep_sec} seconds until next run at 03:00 UTC")
        time.sleep(sleep_sec)
        try:
            app.run()
        except Exception as e:
            print(f"[SCHED] Scheduled run error: {e}")
            # маленькая задержка, чтобы не зациклиться на ошибке
            time.sleep(60)
