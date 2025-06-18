import time
import json
import os

LOG_FILE = "./logs/simulation.log" # Path on the host machine
POLL_INTERVAL = 2 # Seconds
SEEK_FILE = ".monitor_seek_pos" # File to store last read position

def read_new_logs(seek_position):
    """Reads new lines from the log file since the last seek position."""
    new_logs = []
    try:
        with open(LOG_FILE, 'r') as f:
            f.seek(seek_position)
            for line in f:
                try:
                    log_entry = json.loads(line.strip())
                    new_logs.append(log_entry)
                except json.JSONDecodeError:
                    print(f"[MONITOR_HOST] Warning: Skipping malformed JSON line: {line.strip()}")
            new_seek_position = f.tell()
            return new_logs, new_seek_position
    except FileNotFoundError:
        # Log file might not exist yet
        return [], 0
    except Exception as e:
        print(f"[MONITOR_HOST] Error reading log file: {e}")
        return [], seek_position # Return old position on error

def main():
    print("[MONITOR_HOST] Starting...")
    print(f"[MONITOR_HOST] Watching log file: {os.path.abspath(LOG_FILE)}")

    # Load last seek position
    current_seek_position = 0
    if os.path.exists(SEEK_FILE):
        try:
            with open(SEEK_FILE, 'r') as f:
                current_seek_position = int(f.read())
        except ValueError:
            print("[MONITOR_HOST] Warning: Invalid seek position file. Starting from beginning.")
            current_seek_position = 0

    try:
        while True:
            new_entries, current_seek_position = read_new_logs(current_seek_position)

            if new_entries:
                print("-" * 20)
                for entry in new_entries:
                    ts = datetime.datetime.fromtimestamp(entry.get("timestamp", 0)).strftime('%H:%M:%S')
                    zone = entry.get("zone", "N/A")
                    event = entry.get("event", "N/A")
                    msg = f"{ts} [{zone}] Event: {event}"

                    # Add specific details for important events
                    if event == "AttemptSend":
                        dest = entry.get("destination", "?")
                        chunk = entry.get("chunk_id", "?")
                        allowed = entry.get("time_allowed", "?")
                        msg += f" -> {dest} (Chunk {chunk}, Allowed: {allowed})"
                    elif event == "SendSuccess":
                        dest = entry.get("destination", "?")
                        chunk = entry.get("chunk_id", "?")
                        rtt = entry.get("round_trip_latency_ms", -1)
                        msg += f" -> {dest} (Chunk {chunk}, RTT: {rtt:.1f}ms)"
                    elif event == "Blocked_TimeWindow":
                        dest = entry.get("destination", "?")
                        chunk = entry.get("chunk_id", "?")
                        window = entry.get("allowed_window", "?")
                        msg += f" -> {dest} (Chunk {chunk}, Window: {window})"
                    elif event.startswith("SendFail"):
                        dest = entry.get("destination", "?")
                        chunk = entry.get("chunk_id", "?")
                        error = entry.get("error", "")
                        msg += f" -> {dest} (Chunk {chunk}) {error}"
                    elif event == "ReceivedData":
                        source_ip = entry.get("source_ip", "?")
                        payload = entry.get("payload", {})
                        source_zone = payload.get("source_zone", "?")
                        chunk = payload.get("chunk_id", "?")
                        msg += f" from {source_ip} ({source_zone}, Chunk {chunk})"

                    print(msg)

                # Save the new seek position
                with open(SEEK_FILE, 'w') as f:
                    f.write(str(current_seek_position))

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n[MONITOR_HOST] Stopping.")
        # Clean up seek file if desired, or leave it for next run
        # if os.path.exists(SEEK_FILE):
        #     os.remove(SEEK_FILE)

if __name__ == "__main__":
    # Need datetime for the monitor output formatting
    import datetime
    main()