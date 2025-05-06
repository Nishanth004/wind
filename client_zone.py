import socket
import json
import time
import os
import datetime
import random

LOG_FILE = "/app/logs/simulation.log"
SCHEDULE_FILE = "schedule.json"
DATA_FILES_DIR_IN_CONTAINER = "/app/data_to_send" # e.g., wind CSVs

BUFFER_SIZE = 1024
CONNECTION_TIMEOUT = 3
REGULAR_SEND_INTERVAL = 5 # Seconds between normal send attempts
ROGUE_ATTEMPT_PROBABILITY = 0.2 # 20% chance each cycle to also make a rogue attempt
TOTAL_DURATION = 240 # Run client for longer to see more events

def log_event(log_data):
    log_data["timestamp_iso"] = datetime.datetime.now().isoformat()
    try:
        with open(LOG_FILE, "a") as f:
            f.write(json.dumps(log_data) + "\n")
    except Exception as e:
        print(f"[ERROR][{os.getenv('ZONE_NAME', 'unknown')}] Logging failed: {e}")

def get_available_data_files():
    if not os.path.exists(DATA_FILES_DIR_IN_CONTAINER):
        return []
    try:
        return [f for f in os.listdir(DATA_FILES_DIR_IN_CONTAINER) if os.path.isfile(os.path.join(DATA_FILES_DIR_IN_CONTAINER, f))]
    except Exception:
        return []

def attempt_send(zone_name, target_zone, target_host, target_port, rule, chunk_id, payload_ref, payload_size, content_type, is_rogue_attempt=False):
    """Handles a single send attempt, legitimate or rogue."""
    now = datetime.datetime.now()
    current_second = now.second
    
    # Time window check applies to BOTH legitimate and rogue attempts
    # A rogue attempt is only "successful" if it happens to fall within an existing open window
    is_time_allowed_by_schedule = False
    if rule: # rule might be None if target is not defined for this client
        start_sec = rule.get("start_sec", 0)
        end_sec = rule.get("end_sec", 60) # Default to always open if rule incomplete (should not happen with good schedule)
        is_time_allowed_by_schedule = start_sec <= current_second < end_sec
    else: # No rule for this client to send, so any attempt is effectively unscheduled
        is_time_allowed_by_schedule = False


    event_prefix = "Rogue" if is_rogue_attempt else ""
    log_event_type_attempt = f"{event_prefix}AttemptSend"

    print(f"[{zone_name}] {log_event_type_attempt} chunk {chunk_id} (Ref: {payload_ref}). Time: {now.strftime('%H:%M:%S')} ({current_second}s). AllowedBySched: {is_time_allowed_by_schedule}")

    log_data_base = {
        "timestamp": time.time(),
        "zone": zone_name,
        "destination": target_zone,
        "destination_host": target_host,
        "destination_port": target_port,
        "chunk_id": chunk_id,
        "current_second": current_second,
        "allowed_window_config": f"{rule.get('start_sec', 'N/A')}-{rule.get('end_sec', 'N/A')-1}" if rule else "N/A",
        "time_allowed_by_schedule": is_time_allowed_by_schedule,
        "payload_reference": payload_ref,
        "payload_size_bytes": payload_size,
        "payload_content_type": content_type,
        "is_rogue_attempt": is_rogue_attempt
    }
    log_event({**log_data_base, "event": log_event_type_attempt})

    if is_time_allowed_by_schedule: # Path is open according to schedule
        try:
            start_conn_time = time.time()
            with socket.create_connection((target_host, target_port), timeout=CONNECTION_TIMEOUT) as sock:
                end_conn_time = time.time()
                conn_latency_ms = (end_conn_time - start_conn_time) * 1000

                message_payload = {
                    "source_zone": zone_name, "chunk_id": chunk_id, "send_timestamp": time.time(),
                    "data_reference": payload_ref, "data_size_bytes": payload_size,
                    "content_type": content_type, "is_rogue": is_rogue_attempt
                }
                message_bytes = json.dumps(message_payload).encode('utf-8')
                sock.sendall(message_bytes)
                ack = sock.recv(BUFFER_SIZE)
                end_ack_time = time.time()
                rtt_ms = (end_ack_time - start_conn_time) * 1000

                if ack == b'ACK':
                    log_event_type_success = f"{event_prefix}SendSuccess"
                    print(f"[{zone_name}] {log_event_type_success} chunk {chunk_id} to {target_zone}. RTT: {rtt_ms:.2f} ms")
                    log_event({**log_data_base, "event": log_event_type_success, "conn_latency_ms": conn_latency_ms, "round_trip_latency_ms": rtt_ms})
                else:
                    log_event({**log_data_base, "event": f"{event_prefix}SendFail_BadAck"})
        except socket.timeout:
            log_event({**log_data_base, "event": f"{event_prefix}SendFail_Timeout"})
        except socket.error as e:
            log_event({**log_data_base, "event": f"{event_prefix}SendFail_SocketError", "error": str(e)})
        except Exception as e:
            log_event({**log_data_base, "event": f"{event_prefix}SendFail_Unknown", "error": str(e)})
    else: # Path is closed by schedule
        log_event_type_blocked = f"{event_prefix}Blocked_TimeWindow"
        print(f"[{zone_name}] {log_event_type_blocked} chunk {chunk_id} (Ref: {payload_ref})")
        log_event({**log_data_base, "event": log_event_type_blocked})


def run_client(zone_name, target_zone, target_host, target_port, rule, available_data_files):
    legit_chunk_id_counter = 1
    rogue_chunk_id_counter = 10000 # Start rogue IDs high to distinguish
    start_sim_time = time.time()

    print(f"[{zone_name}] Starting client. Target: {target_zone} ({target_host}:{target_port}). Duration: {TOTAL_DURATION}s. RogueProb: {ROGUE_ATTEMPT_PROBABILITY}")

    can_send_legit_data = True
    if zone_name == "data_ingestion_zone" and not available_data_files:
        print(f"[{zone_name}] No data files for legit sends. Only rogue attempts possible if configured.")
        can_send_legit_data = False


    while time.time() - start_sim_time < TOTAL_DURATION:
        # --- Legitimate Attempt ---
        if can_send_legit_data or zone_name != "data_ingestion_zone": # Allow non-ingestion zones to send placeholder legit data
            payload_ref = "N/A"
            payload_size = 0
            content_type = "generic_summary"

            if zone_name == "data_ingestion_zone" and available_data_files:
                payload_ref = random.choice(available_data_files)
                content_type = "raw_forecast_metadata"
                try: payload_size = os.path.getsize(os.path.join(DATA_FILES_DIR_IN_CONTAINER, payload_ref))
                except: pass
            elif zone_name == "forecast_processing_zone":
                payload_ref = f"proc_summary_{legit_chunk_id_counter}"
                content_type = "processed_forecast_summary"
                payload_size = random.randint(100, 500)
            elif zone_name == "operational_control_zone":
                payload_ref = f"op_update_{legit_chunk_id_counter}"
                content_type = "operational_hmi_data"
                payload_size = random.randint(50, 200)

            if payload_ref != "N/A": # Ensure there's something to send
                 attempt_send(zone_name, target_zone, target_host, target_port, rule,
                             legit_chunk_id_counter, payload_ref, payload_size, content_type,
                             is_rogue_attempt=False)
                 legit_chunk_id_counter += 1

        # --- Rogue Attempt (Probabilistic) ---
        if random.random() < ROGUE_ATTEMPT_PROBABILITY:
            rogue_payload_ref = f"rogue_data_{rogue_chunk_id_counter}"
            rogue_payload_size = random.randint(10, 1000) # Rogue payload can be anything
            rogue_content_type = "rogue_exfiltration_attempt"
            attempt_send(zone_name, target_zone, target_host, target_port, rule,
                         rogue_chunk_id_counter, rogue_payload_ref, rogue_payload_size, rogue_content_type,
                         is_rogue_attempt=True)
            rogue_chunk_id_counter += 1

        time.sleep(REGULAR_SEND_INTERVAL)

    log_event({"timestamp": time.time(), "zone": zone_name, "event": "ClientFinished"})


if __name__ == "__main__":
    zone_name = os.getenv('ZONE_NAME')
    if not zone_name: exit(1)

    try:
        with open(SCHEDULE_FILE, 'r') as f: schedule_data = json.load(f)
    except Exception: exit(1)

    available_files = []
    if zone_name == "data_ingestion_zone":
        available_files = get_available_data_files()
        print(f"[{zone_name}] Found {len(available_files)} data files in {DATA_FILES_DIR_IN_CONTAINER}.")

    client_rule, target_zone_name, target_host_name, target_port_num = None, None, None, None
    my_role_info = schedule_data.get("zones", {}).get(zone_name, {})

    if my_role_info.get("role") in ["client", "server_client"]:
        target_zone_name = my_role_info.get("target")
        if target_zone_name:
            target_host_name = target_zone_name
            for r in schedule_data.get("rules", []):
                if r.get("source") == zone_name and r.get("destination") == target_zone_name:
                    client_rule = r
                    target_port_num = r.get("port")
                    break
    
    if client_rule and target_port_num:
        run_client(zone_name, target_zone_name, target_host_name, target_port_num, client_rule, available_files)
    else:
        print(f"[{zone_name}] No client rule or target. Idling.")
        log_event({"timestamp": time.time(), "zone": zone_name, "event": "ClientNotStarted_NoConfig"})
        while True: time.sleep(60)