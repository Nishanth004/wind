import socket
import json
import time
import os
import datetime
import random
import threading
import queue

# --- Constants ---
LOG_FILE = "/app/logs/simulation.log"
SCHEDULE_FILE = "schedule.json"
DATA_FILES_DIR_IN_CONTAINER = "/app/data_to_send" # For data_ingestion_zone

BUFFER_SIZE = 1024
CONNECTION_TIMEOUT = 3
CLIENT_REGULAR_SEND_INTERVAL = 4 # Seconds between legit send checks
CLIENT_ROGUE_ATTEMPT_PROBABILITY = 0.3 # Increased for more rogue events
CLIENT_TOTAL_DURATION = 480 # Run client parts longer

# --- Global Lock for Logging ---
log_lock = threading.Lock()

def log_event(log_data): # This is the shared log_event function
    log_data["timestamp_iso"] = datetime.datetime.now(datetime.timezone.utc).isoformat() # Use UTC for logs
    with log_lock:
        try:
            with open(LOG_FILE, "a") as f:
                f.write(json.dumps(log_data) + "\n")
        except Exception as e:
            print(f"[CRITICAL_LOG_ERROR][{log_data.get('zone','UNKNOWN')}] Logging failed: {e}", flush=True)

# --- Server Component ---
def handle_client_connection(conn, addr, current_zone_name,
                             internal_output_queue_for_client_part,
                             log_event_func):
    peer_ip, peer_port = addr
    log_event_base_info_server = {
        "zone": current_zone_name, "event_context": "server",
        "peer_ip": peer_ip, "peer_port": peer_port
    }
    print(f"[{current_zone_name}/S] Connection established from {peer_ip}:{peer_port}", flush=True)
    log_event_func({**log_event_base_info_server, "timestamp": time.time(), "event": "ConnectionReceived"})
    try:
        with conn:
            data_bytes = conn.recv(BUFFER_SIZE)
            if not data_bytes:
                log_event_func({**log_event_base_info_server, "timestamp": time.time(), "event": "ConnectionEmpty"})
                return
            message_str = data_bytes.decode('utf-8')
            received_payload = json.loads(message_str)
            is_rogue_payload_received = received_payload.get("is_rogue", False)
            received_data_ref = received_payload.get("data_reference", "N/A")
            print(f"[{current_zone_name}/S] Received (Rogue: {is_rogue_payload_received}): Ref='{received_data_ref[:30]}...' from {received_payload.get('source_zone', 'UnknownSource')}", flush=True)
            log_event_func({
                **log_event_base_info_server, "timestamp": time.time(), "event": "ReceivedData",
                "payload": received_payload, "is_rogue_payload": is_rogue_payload_received
            })
            if internal_output_queue_for_client_part:
                data_to_queue_for_client = {
                    "original_source_zone": received_payload.get("source_zone"),
                    "original_message_id": received_payload.get("message_id"),
                    "original_data_reference": received_data_ref,
                    "original_content_type": received_payload.get("content_type"),
                    "is_rogue_when_received": is_rogue_payload_received,
                    "server_processing_timestamp_utc": datetime.datetime.utcnow().isoformat(),
                    "processed_by_zone": current_zone_name
                }
                try:
                    internal_output_queue_for_client_part.put_nowait(data_to_queue_for_client)
                    log_event_func({
                        **log_event_base_info_server, "timestamp": time.time(), "event": "DataQueuedForOwnClientPart",
                        "queued_data_summary": {"original_ref": received_data_ref}
                    })
                except queue.Full:
                    log_event_func({
                        **log_event_base_info_server, "timestamp": time.time(), "event": "DataQueueFull_Dropped",
                        "original_ref": received_data_ref
                    })
            conn.sendall(b'ACK')
    except json.JSONDecodeError:
        log_event_func({**log_event_base_info_server, "timestamp": time.time(), "event": "ReceiveFail_BadJson"})
    except socket.error as e:
        log_event_func({**log_event_base_info_server, "timestamp": time.time(), "event": "ReceiveFail_SocketError", "error": str(e)})
    except Exception as e:
        log_event_func({**log_event_base_info_server, "timestamp": time.time(), "event": "ReceiveFail_UnknownError", "error": str(e)})
    finally:
        pass # Connection closed by 'with conn:'


def run_server_logic(current_zone_name, listen_host, listen_port,
                     internal_output_queue_for_client_part,
                     log_event_func):
    log_event_base_info_server_main = {"zone": current_zone_name, "event_context": "server_main"}
    print(f"[{current_zone_name}/S] Attempting to start server on {listen_host}:{listen_port}", flush=True)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server_socket.bind((listen_host, listen_port))
            server_socket.listen()
            print(f"[{current_zone_name}/S] Server Listening on {listen_host}:{listen_port}", flush=True)
            log_event_func({**log_event_base_info_server_main, "timestamp": time.time(), "event": "ServerPartStarted", "host": listen_host, "port": listen_port})
            while True:
                try:
                    client_conn, client_addr = server_socket.accept()
                    conn_handler_thread = threading.Thread(
                        target=handle_client_connection,
                        args=(client_conn, client_addr, current_zone_name, internal_output_queue_for_client_part, log_event_func),
                        daemon=True)
                    conn_handler_thread.start()
                except socket.error as e:
                    log_event_func({**log_event_base_info_server_main, "timestamp": time.time(), "event": "ServerAcceptError", "error": str(e)})
                    time.sleep(0.1)
                except Exception as e:
                    log_event_func({**log_event_base_info_server_main, "timestamp": time.time(), "event": "ServerAcceptLoop_CriticalError", "error": str(e)})
                    time.sleep(1)
        except OSError as e:
            log_event_func({**log_event_base_info_server_main, "timestamp": time.time(), "event": "ServerPartFatal_BindError", "error": str(e)})
        except Exception as e:
            log_event_func({**log_event_base_info_server_main, "timestamp": time.time(), "event": "ServerPartFatal_SetupError", "error": str(e)})
    log_event_func({**log_event_base_info_server_main, "timestamp": time.time(), "event": "ServerPartExited"})

# --- Client Component ---
def get_initial_data_files(zone_name_for_client): # Only for data_ingestion_zone
    if zone_name_for_client == "data_ingestion_zone":
        if not os.path.exists(DATA_FILES_DIR_IN_CONTAINER): return []
        try:
            return [f for f in os.listdir(DATA_FILES_DIR_IN_CONTAINER) if os.path.isfile(os.path.join(DATA_FILES_DIR_IN_CONTAINER, f))]
        except: return []
    return []

def client_attempt_send(zone_name_for_client, target_zone, target_host, target_port, rule,
                        chunk_id, payload_ref, payload_size, content_type, is_rogue_attempt=False):
    now = datetime.datetime.now()
    current_second = now.second
    is_time_allowed_by_schedule = False # This is the crucial check by the "firewall"
    log_event_base = {"zone": zone_name_for_client, "event_context": "client", "destination": target_zone}

    if rule:
        start_sec, end_sec = rule.get("start_sec",0), rule.get("end_sec",60)
        is_time_allowed_by_schedule = start_sec <= current_second < end_sec
    
    event_prefix = "Rogue" if is_rogue_attempt else ""
    log_event_type_attempt = f"{event_prefix}AttemptSend"

    print(f"[{zone_name_for_client}/C] {log_event_type_attempt} ID {chunk_id} (Ref: {payload_ref[:30]}...). WinOpen: {is_time_allowed_by_schedule}", flush=True)

    log_data_for_attempt = {
        **log_event_base, "timestamp": time.time(), "event": log_event_type_attempt,
        "chunk_id": chunk_id, "current_second": current_second,
        "allowed_window_config": f"{rule.get('start_sec', 'N')}-{rule.get('end_sec', 'N')-1}" if rule else "N/A",
        "time_allowed_by_schedule": is_time_allowed_by_schedule,
        "payload_reference": payload_ref, "payload_size_bytes": payload_size,
        "payload_content_type": content_type, "is_rogue_attempt": is_rogue_attempt
    }
    log_event(log_data_for_attempt)

    if is_time_allowed_by_schedule: # "Firewall" allows connection attempt
        try:
            start_conn_time = time.time()
            with socket.create_connection((target_host, target_port), timeout=CONNECTION_TIMEOUT) as sock:
                conn_latency_ms = (time.time() - start_conn_time) * 1000
                message_payload = {
                    "source_zone": zone_name_for_client, "chunk_id": chunk_id, "send_timestamp": time.time(),
                    "data_reference": payload_ref, "data_size_bytes": payload_size,
                    "content_type": content_type, "is_rogue": is_rogue_attempt
                }
                sock.sendall(json.dumps(message_payload).encode('utf-8'))
                ack = sock.recv(BUFFER_SIZE)
                rtt_ms = (time.time() - start_conn_time) * 1000
                if ack == b'ACK':
                    log_event({**log_data_for_attempt, "event": f"{event_prefix}SendSuccess", "conn_latency_ms": conn_latency_ms, "round_trip_latency_ms": rtt_ms})
                    print(f"[{zone_name_for_client}/C] {event_prefix}SendSuccess ID {chunk_id}. RTT: {rtt_ms:.1f}ms", flush=True)
                else: log_event({**log_data_for_attempt, "event": f"{event_prefix}SendFail_BadAck"})
        except socket.timeout: log_event({**log_data_for_attempt, "event": f"{event_prefix}SendFail_Timeout"})
        except socket.error as e: log_event({**log_data_for_attempt, "event": f"{event_prefix}SendFail_SocketError", "error": str(e)})
        except Exception as e: log_event({**log_data_for_attempt, "event": f"{event_prefix}SendFail_Unknown", "error": str(e)})
    else: # "Firewall" blocks connection attempt
        log_event({**log_data_for_attempt, "event": f"{event_prefix}Blocked_TimeWindow"})
        print(f"[{zone_name_for_client}/C] {event_prefix}Blocked_TimeWindow ID {chunk_id}", flush=True)


def run_client_logic(zone_name_for_client, target_zone, target_host, target_port, rule, initial_data_files, input_data_q):
    legit_chunk_id = 1
    rogue_chunk_id = 10000 # Differentiate IDs
    start_sim_time = time.time()
    log_event_base = {"zone": zone_name_for_client, "event_context": "client", "destination": target_zone}

    print(f"[{zone_name_for_client}/C] Starting client. Target: {target_zone}. Duration: {CLIENT_TOTAL_DURATION}s. RogueProb: {CLIENT_ROGUE_ATTEMPT_PROBABILITY}", flush=True)

    is_ingestion_node = zone_name_for_client == "data_ingestion_zone"

    while time.time() - start_sim_time < CLIENT_TOTAL_DURATION:
        now = datetime.datetime.now()
        current_second = now.second
        
        # --- Legitimate Send Logic ---
        is_legit_window_open = False
        if rule:
            start_sec, end_sec = rule.get("start_sec",0), rule.get("end_sec",60)
            is_legit_window_open = start_sec <= current_second < end_sec

        payload_to_send_legit = None # This will hold the data object for legit send

        if is_ingestion_node:
            if initial_data_files: # Ingestion node creates data from files
                payload_to_send_legit = {
                    "ref": random.choice(initial_data_files),
                    "size": 0, # Placeholder, could get actual size
                    "type": "raw_forecast_metadata"
                }
                try: payload_to_send_legit["size"] = os.path.getsize(os.path.join(DATA_FILES_DIR_IN_CONTAINER, payload_to_send_legit["ref"]))
                except: pass
        elif input_data_q and not input_data_q.empty(): # Chained node gets data from its server part's queue
            try:
                processed_data_from_server = input_data_q.get_nowait()
                # Create a new payload based on "processed" data
                payload_to_send_legit = {
                    "ref": f"processed_{processed_data_from_server.get('original_data_reference','unknown')}_by_{zone_name_for_client}",
                    "size": random.randint(50,500), # Simulated size of processed data
                    "type": f"{zone_name_for_client}_processed_output"
                }
            except queue.Empty:
                pass # No data from server part yet

        if is_legit_window_open and payload_to_send_legit:
            print(f"[{zone_name_for_client}/C] Legit window OPEN & data ready. Attempting legit send ID {legit_chunk_id}.", flush=True)
            client_attempt_send(zone_name_for_client, target_zone, target_host, target_port, rule,
                                legit_chunk_id, payload_to_send_legit["ref"], payload_to_send_legit["size"], payload_to_send_legit["type"],
                                is_rogue_attempt=False)
            legit_chunk_id += 1
        elif payload_to_send_legit: # Data was ready, but window closed for legit send
            print(f"[{zone_name_for_client}/C] Legit window CLOSED. Holding legit send ID {legit_chunk_id} (Ref: {payload_to_send_legit['ref'][:30]}...).", flush=True)
            log_event({
                **log_event_base, "timestamp": time.time(), "event": "LegitSend_Held_WindowClosed",
                "chunk_id": legit_chunk_id, "payload_reference": payload_to_send_legit["ref"], "current_second": current_second,
                "allowed_window_config": f"{rule.get('start_sec', 'N')}-{rule.get('end_sec', 'N')-1}" if rule else "N/A", "is_rogue_attempt": False
            })

        # --- Rogue Send Logic ---
        if random.random() < CLIENT_ROGUE_ATTEMPT_PROBABILITY:
            print(f"[{zone_name_for_client}/C] Initiating ROGUE send ID {rogue_chunk_id}.", flush=True)
            client_attempt_send(zone_name_for_client, target_zone, target_host, target_port, rule,
                                rogue_chunk_id, f"rogue_payload_{rogue_chunk_id}", random.randint(10, 1000), "rogue_data_type",
                                is_rogue_attempt=True)
            rogue_chunk_id += 1
        
        time.sleep(CLIENT_REGULAR_SEND_INTERVAL)
    
    log_event({**log_event_base, "timestamp": time.time(), "event": "ClientPartFinished"})
    print(f"[{zone_name_for_client}/C] Client part finished.", flush=True)


# --- Main Agent Logic ---
if __name__ == "__main__":
    if os.path.exists("logs\simulation.log"):
        os.remove("logs\simulation.log")
    zone_name = os.getenv('ZONE_NAME')
    if not zone_name:
        print("FATAL: ZONE_NAME environment variable not set.", flush=True)
        exit(1)

    print(f"[{zone_name}] Agent starting...", flush=True)
    log_event({"timestamp": time.time(), "zone": zone_name, "event": "AgentStarting"})

    try:
        with open(SCHEDULE_FILE, 'r') as f:
            schedule_config = json.load(f)
    except Exception as e:
        print(f"[{zone_name}] FATAL: Could not load schedule file '{SCHEDULE_FILE}': {e}", flush=True)
        log_event({"timestamp": time.time(), "zone": zone_name, "event": "AgentFatal_NoSchedule", "error": str(e)})
        exit(1)

    my_zone_config = schedule_config.get("zones", {}).get(zone_name)
    if not my_zone_config:
        print(f"[{zone_name}] FATAL: No configuration found for this zone in schedule.", flush=True)
        log_event({"timestamp": time.time(), "zone": zone_name, "event": "AgentFatal_NoZoneConfig"})
        exit(1)

    role = my_zone_config.get("role")
    threads = []
    # This queue is for data passing from the server part to the client part *within the same agent*
    internal_data_queue_for_client_part = queue.Queue() if role == "server_client" else None

    # --- Start Server Part (if applicable) ---
    if role in ["server", "server_client"]:
        listen_source_zone = my_zone_config.get("listen_source")
        server_listen_port = None
        for r in schedule_config.get("rules", []):
            if r.get("source") == listen_source_zone and r.get("destination") == zone_name:
                server_listen_port = r.get("port")
                break
        if server_listen_port:
            server_thread = threading.Thread(
                target=run_server_logic,
                args=(zone_name, '0.0.0.0', server_listen_port, internal_data_queue_for_client_part,log_event),
                daemon=True,
                name=f"Thread-Server-{zone_name}" 
            )
            threads.append(server_thread)
            server_thread.start()
        else:
            msg = f"[{zone_name}] No valid incoming rule/port found for server role. Server part not starting."
            print(msg, flush=True)
            log_event({"timestamp": time.time(), "zone": zone_name, "event": "ServerPartNotStarted_NoRule", "role": role})


    # --- Start Client Part (if applicable) ---
    if role in ["client", "server_client"]:
        target_destination_zone = my_zone_config.get("target")
        client_target_host = None
        client_target_port = None
        client_rule_for_sending = None
        if target_destination_zone:
            client_target_host = target_destination_zone # Docker DNS
            for r in schedule_config.get("rules", []):
                if r.get("source") == zone_name and r.get("destination") == target_destination_zone:
                    client_rule_for_sending = r
                    client_target_port = r.get("port")
                    break
        
        if client_target_port and client_rule_for_sending:
            initial_files = get_initial_data_files(zone_name) # Only ingestion node uses this
            client_thread = threading.Thread(
                target=run_client_logic,
                args=(zone_name, target_destination_zone, client_target_host, client_target_port,
                      client_rule_for_sending, initial_files, internal_data_queue_for_client_part),
                daemon=True
            )
            threads.append(client_thread)
            # Stagger client starts slightly to avoid all hitting network at once (optional)
            time.sleep(random.uniform(0.1, 1.0))
            client_thread.start()
        else:
            msg = f"[{zone_name}] No valid outgoing rule/target/port found for client role. Client part not starting."
            print(msg, flush=True)
            log_event({"timestamp": time.time(), "zone": zone_name, "event": "ClientPartNotStarted_NoRule", "role": role})

    if not threads:
        print(f"[{zone_name}] No server or client parts started based on role '{role}'. Idling.", flush=True)
        log_event({"timestamp": time.time(), "zone": zone_name, "event": "AgentIdle_NoParts", "role": role})
        # Keep container alive for logs or if it's a pure endpoint, otherwise it might exit
        while True: time.sleep(3600)


    # Keep main thread alive while daemon threads run
    try:
        for t in threads:
            t.join() # This will only effectively join if threads are not daemons or if they exit
        # If all threads are daemons, main thread will exit if it has nothing else to do.
        # So, we add a loop to keep it alive, primarily for the server threads.
        if any(t.name.startswith("Thread-Server") or "run_server_logic" in str(t._target) for t in threads if t.is_alive()):
             print(f"[{zone_name}] Main agent thread entering keep-alive loop for server parts.", flush=True)
             while True:
                 time.sleep(60)
                 # Check if any critical threads are still alive (optional)
                 if not any(t.is_alive() for t in threads):
                     print(f"[{zone_name}] All worker threads have exited. Main agent thread exiting.", flush=True)
                     break
        else:
             print(f"[{zone_name}] No long-running server parts; client parts may have finished. Main agent thread may exit.", flush=True)


    except KeyboardInterrupt:
        print(f"[{zone_name}] Agent interrupted. Shutting down.", flush=True)
        log_event({"timestamp": time.time(), "zone": zone_name, "event": "AgentShutdown_Interrupt"})
    finally:
        print(f"[{zone_name}] Agent cleanup and exit.", flush=True)
        log_event({"timestamp": time.time(), "zone": zone_name, "event": "AgentExited"})