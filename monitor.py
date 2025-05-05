import time
import json
import os
import sys

def monitor(limit):
    limit = int(limit)
    seen = set()
    log = []

    while len(seen) < limit:
        if not os.path.exists("transferred.json"):
            time.sleep(0.5)
            continue

        with open("transferred.json") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                time.sleep(0.5)
                continue

        for entry in data:
            if entry["chunk_id"] not in seen:
                seen.add(entry["chunk_id"])
                log.append(entry)

                print(f"[MONITOR] Chunk {entry['chunk_id']} - {entry['status'].upper()}")

                with open("log.json", "w") as logf:
                    json.dump(log, logf, indent=2)

        time.sleep(0.5)

if __name__ == "__main__":
    print("[MONITOR] Monitoring started.")
    arg = sys.argv[1] if len(sys.argv) > 1 else "10"
    monitor(arg)
