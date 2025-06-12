# This code is intended to be part of zone_agent.py

import socket
import json
import time
import os
import datetime
import random
import queue # For internal data queue if this client is part of a server_client agent
from zone_agent import log_lock # Assuming log_lock is defined in zone_agent.py
from zone_agent import log_event # Assuming log_event is defined in zone_agent.py

# --- Constants (ensure these are defined or passed appropriately in zone_agent.py) ---
# LOG_FILE = "/app/logs/simulation.log" (defined in zone_agent.py)
# SCHEDULE_FILE = "schedule.json" (defined in zone_agent.py)
DATA_FILES_DIR_IN_CONTAINER = "/app/data_to_send" # For data_ingestion_zone specifically

BUFFER_SIZE = 1024
CONNECTION_TIMEOUT = 3
CLIENT_REGULAR_SEND_INTERVAL = 5
CLIENT_ROGUE_ATTEMPT_PROBABILITY = 0.3 # Higher for more visible rogue events
CLIENT_TOTAL_DURATION = 300

# log_event function (defined in zone_agent.py and uses log_lock)
# def log_event(log_data): ...

# --- Client Component Functions ---

def get_initial_data_files_for_ingestion(zone_name_for_client):
    if zone_name_for_client == "data_ingestion_zone":
        if not os.path.exists(DATA_FILES_DIR_IN_CONTAINER):
            print(f"[{zone_name_for_client}/C] Initial data directory not found: {DATA_FILES_DIR_IN_CONTAINER}", flush=True)
            return []
        try:
            files = [f for f in os.listdir(DATA_FILES_DIR_IN_CONTAINER) if os.path.isfile(os.path.join(DATA_FILES_DIR_IN_CONTAINER, f))]
            print(f"[{zone_name_for_client}/C] Found {len(files)} initial data files.", flush=True)
            return files
        except Exception as e:
            print(f"[{zone_name_for_client}/C] Error listing initial data files: {e}", flush=True)
            return []
    return []


def client_attempt_send(zone_name_for_client, target_zone, target_host, target_port, rule_for_path,
                        message_id, payload_ref, payload_size, content_type, is_rogue_attempt):
    # Passed log_event implicitly as it's global in this script
    now = datetime.datetime.now()
    current_second = now.second
    is_path_actually_open_by_schedule = False
    log_event_base_info = {
        "zone": zone_name_for_client, "event_context": "client", "destination": target_zone,
        "message_id": message_id, "payload_reference": payload_ref,
        "is_rogue_attempt": is_rogue_attempt, "current_second": current_second,
        "scheduled_window_for_path": f"{rule_for_path.get('start_sec','N/A')}-{rule_for_path.get('end_sec','N/A')-1}" if rule_for_path else "N/A"
    }

    if rule_for_path:
        start_sec, end_sec = rule_for_path.get("start_sec",0), rule_for_path.get("end_sec",60)
        is_path_actually_open_by_schedule = start_sec <= current_second < end_sec
    
    log_event_base_info["path_open_by_schedule"] = is_path_actually_open_by_schedule
    event_prefix = "Rogue" if is_rogue_attempt else ""
    log_event_type_attempt = f"{event_prefix}AttemptSend"

    print(f"[{zone_name_for_client}/C] {log_event_type_attempt} ID {message_id} (Ref: {payload_ref[:30]}...). PathOpenSched: {is_path_actually_open_by_schedule}", flush=True)
    log_event({**log_event_base_info, "timestamp": time.time(), "event": log_event_type_attempt,
               "payload_size_bytes": payload_size, "payload_content_type": content_type})

    if is_path_actually_open_by_schedule:
        try:
            start_conn_time = time.time()
            with socket.create_connection((target_host, target_port), timeout=CONNECTION_TIMEOUT) as sock:
                conn_latency_ms = (time.time() - start_conn_time) * 1000
                network_payload = {
                    "source_zone": zone_name_for_client, "message_id": message_id,
                    "send_timestamp_utc": datetime.datetime.utcnow().isoformat(),
                    "data_reference": payload_ref, "data_size_bytes": payload_size,
                    "content_type": content_type, "is_rogue": is_rogue_attempt
                }
                sock.sendall(json.dumps(network_payload).encode('utf-8'))
                ack = sock.recv(BUFFER_SIZE)
                round_trip_latency_ms = (time.time() - start_conn_time) * 1000
                if ack == b'ACK':
                    event_type_outcome = f"{event_prefix}SendSuccess"
                    log_event({**log_event_base_info, "timestamp": time.time(), "event": event_type_outcome,
                               "conn_latency_ms": conn_latency_ms, "round_trip_latency_ms": round_trip_latency_ms})
                else:
                    log_event({**log_event_base_info, "timestamp": time.time(), "event": f"{event_prefix}SendFail_BadAck"})
        except socket.timeout:
            log_event({**log_event_base_info, "timestamp": time.time(), "event": f"{event_prefix}SendFail_Timeout"})
        except socket.error as e:
            log_event({**log_event_base_info, "timestamp": time.time(), "event": f"{event_prefix}SendFail_SocketError", "error": str(e)})
        except Exception as e:
            log_event({**log_event_base_info, "timestamp": time.time(), "event": f"{event_prefix}SendFail_Unknown", "error": str(e)})
    else: # Path is "closed" by the schedule policy
        event_type_outcome = f"{event_prefix}Blocked_TimeWindow"
        log_event({**log_event_base_info, "timestamp": time.time(), "event": event_type_outcome})


def run_client_logic(zone_name_as_client, target_zone_name, target_host_address, target_port_number,
                     outbound_rule_for_client,
                     initial_data_files_list,
                     input_data_queue_from_server_part): # Removed log_event_func, use global
    legitimate_message_id_counter = 1
    rogue_message_id_counter = 10000
    simulation_start_time = time.time()
    log_event_base_info_client = {"zone": zone_name_as_client, "event_context": "client", "destination": target_zone_name}

    print(f"[{zone_name_as_client}/C] Starting Client Logic. Target: {target_zone_name}. Interval: {CLIENT_REGULAR_SEND_INTERVAL}s. RogueProb: {CLIENT_ROGUE_ATTEMPT_PROBABILITY}", flush=True)
    is_initial_ingestion_client = zone_name_as_client == "data_ingestion_zone"

    while time.time() - simulation_start_time < CLIENT_TOTAL_DURATION:
        current_time = datetime.datetime.now()
        current_second_of_minute = current_time.second
        
        payload_for_legitimate_send = None # Dict: {"ref": ..., "size": ..., "type": ...}

        if is_initial_ingestion_client:
            if initial_data_files_list:
                selected_file = random.choice(initial_data_files_list)
                file_size = 0; 
                try: file_size = os.path.getsize(os.path.join(DATA_FILES_DIR_IN_CONTAINER, selected_file)); 
                except: pass
                payload_for_legitimate_send = {"ref": selected_file, "size": file_size, "type": "raw_forecast_metadata"}
        elif input_data_queue_from_server_part: # Chained client
            try:
                data_from_server_part = input_data_queue_from_server_part.get_nowait()
                original_ref = data_from_server_part.get("original_data_reference", "unknown_upstream")
                payload_for_legitimate_send = {
                    "ref": f"processed_by_{zone_name_as_client}_from_{original_ref[:20]}",
                    "size": random.randint(50, 500),
                    "type": f"{zone_name_as_client}_output"
                }
                print(f"[{zone_name_as_client}/C] Got data from internal queue: {payload_for_legitimate_send['ref'][:30]}...", flush=True)
            except queue.Empty:
                print(f"[{zone_name_as_client}/C] Input queue empty for legit send.", flush=True) # Can be noisy
                pass
        
        is_my_outbound_window_open_now = False
        if outbound_rule_for_client:
            start_sec = outbound_rule_for_client.get("start_sec", 0); end_sec = outbound_rule_for_client.get("end_sec", 60)
            is_my_outbound_window_open_now = start_sec <= current_second_of_minute < end_sec

        if payload_for_legitimate_send:
            if is_my_outbound_window_open_now:
                client_attempt_send(zone_name_as_client, target_zone_name, target_host_address, target_port_number,
                                    outbound_rule_for_client, legitimate_message_id_counter,
                                    payload_for_legitimate_send["ref"], payload_for_legitimate_send["size"],
                                    payload_for_legitimate_send["type"], is_rogue_attempt=False)
                legitimate_message_id_counter += 1
            else:
                log_event({
                    **log_event_base_info_client, "timestamp": time.time(), "event": "LegitSend_Held_ClientAwareWindowClosed",
                    "message_id": legitimate_message_id_counter, "payload_reference": payload_for_legitimate_send["ref"],
                    "current_second": current_second_of_minute, "is_rogue_attempt": False,
                    "scheduled_window_for_path": f"{outbound_rule_for_client.get('start_sec','N/A')}-{outbound_rule_for_client.get('end_sec','N/A')-1}" if outbound_rule_for_client else "N/A"
                })
                if input_data_queue_from_server_part and 'data_from_server_part' in locals() and data_from_server_part:
                    try: input_data_queue_from_server_part.put_nowait(data_from_server_part) # Put it back
                    except queue.Full:
                         log_event({**log_event_base_info_client, "timestamp": time.time(), "event": "LegitSend_ReQueueFail_Full", "message_id": legitimate_message_id_counter})
        
        if random.random() < CLIENT_ROGUE_ATTEMPT_PROBABILITY:
            client_attempt_send(zone_name_as_client, target_zone_name, target_host_address, target_port_number,
                                outbound_rule_for_client, rogue_message_id_counter,
                                f"rogue_payload_{rogue_message_id_counter}", random.randint(10, 1000),
                                "rogue_generated_data", is_rogue_attempt=True)
            rogue_message_id_counter += 1
        
        time.sleep(CLIENT_REGULAR_SEND_INTERVAL)
    
    log_event({**log_event_base_info_client, "timestamp": time.time(), "event": "ClientPartFinished"})
    print(f"[{zone_name_as_client}/C] Client part finished its run duration.", flush=True)

# The rest of zone_agent.py (main block, server logic) would integrate these functions.
# For example, in the __main__ of zone_agent.py:
#
# if role in ["client", "server_client"]:
#    # ... (determine target_zone_name, target_host_address, etc. and outbound_rule_for_client) ...
#    initial_files = get_initial_data_files_for_ingestion(zone_name)
#    client_thread = threading.Thread(
#        target=run_client_logic,
#        args=(zone_name, target_zone_name, target_host_address, target_port_number,
#              outbound_rule_for_client, initial_files, internal_data_queue_for_client_part, # Pass the queue here
#              log_event), # Pass the actual log_event function
#        daemon=True
#    )
#    threads.append(client_thread)
#    client_thread.start()