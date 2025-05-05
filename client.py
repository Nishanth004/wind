import json
import time
import random
import sys

def load_chunks():
    with open("input.json") as f:
        return json.load(f)

def client(limit):
    chunks = load_chunks()
    limit = min(int(limit), len(chunks))
    with open("transferred.json", "w") as f:
        json.dump([], f)

    for i in range(limit):
        print(f"[CLIENT] Opening window for chunk {i+1}...")
        time.sleep(1)

        if i % 7 == 0 and i != 0:
            print(f"[CLIENT] Simulating ILLEGAL transfer for chunk {i+1}")
            status = "violation"
        else:
            print(f"[CLIENT] Closing window for chunk {i+1}...")
            status = "legal"

        with open("transferred.json", "r+") as f:
            data = json.load(f)
            data.append({
                "chunk_id": i+1,
                "timestamp": time.time(),
                "status": status
            })
            f.seek(0)
            json.dump(data, f)
            f.truncate()

        time.sleep(0.5)

if __name__ == "__main__":
    limit = sys.argv[1] if len(sys.argv) > 1 else "10"
    client(limit)
