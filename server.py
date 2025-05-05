import json
import time
import os

TRANSFER_FILE = "transferred.json"

def read_latest_transfer():
    if not os.path.exists(TRANSFER_FILE):
        return []

    with open(TRANSFER_FILE, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []

def simulate_server_response():
    seen = set()
    print("[SERVER] SCADA server listening for chunk updates...")

    while True:
        data = read_latest_transfer()

        for entry in data:
            chunk_id = entry["chunk_id"]
            if chunk_id not in seen:
                seen.add(chunk_id)
                print(f"[SERVER] Received Chunk {chunk_id}: Status - {entry['status'].upper()}")

        time.sleep(1)

if __name__ == "__main__":
    simulate_server_response()
