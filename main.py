import threading
import time
import os
import subprocess

def wait_for_file(filename, timeout=10):
    for _ in range(timeout * 10):
        if os.path.exists(filename):
            return True
        time.sleep(0.1)
    return False

def run_script(script_name, args=None):
    cmd = ["python", script_name]
    if args:
        cmd += args
    return subprocess.Popen(cmd)

if __name__ == "__main__":
    print("[MAIN] Starting all processes...")

    limit = input("Enter number of chunks to transfer: ")

    # Start the client process first so it creates transferred.json
    client = run_script("client.py", [limit])

    # Wait until transferred.json exists
    if not wait_for_file("transferred.json", timeout=5):
        print("[MAIN] Error: transferred.json was not created in time.")
        client.kill()
        exit(1)

    # Start monitor and visualizer
    monitor = run_script("monitor.py", [limit])
    visualizer = run_script("visualizer.py")

    # Wait for client to complete
    client.wait()
    print("[MAIN] Chunk transfer complete.")

    # Ask user whether to continue
    proceed = input("[MAIN] Do you want to continue? (y/n): ").strip().lower()
    if proceed != 'y':
        print("[MAIN] Terminating all processes...")
        monitor.terminate()
        visualizer.terminate()
        print("[MAIN] Shutdown complete.")
