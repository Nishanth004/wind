import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import matplotlib.patches as patches
import matplotlib.dates as mdates # For proper date formatting on timeline
import os
import time
import datetime
import pandas as pd # For easier data handling and time conversions
from collections import defaultdict
import numpy as np

LOG_FILE = "./logs/simulation.log"
SCHEDULE_FILE = "schedule.json"
UPDATE_INTERVAL = 3000 # Milliseconds (3 seconds for updates)
# To select a specific path to visualize, or set to None to iterate/prompt
# PATH_TO_VISUALIZE_EXPLICITLY = "data_ingestion_zone -> forecast_processing_zone"
PATH_TO_VISUALIZE_EXPLICITLY = "data_ingestion_zone -> forecast_processing_zone"

log_data_global = []
last_log_mtime_global = 0
schedule_rules_global = {}

def load_schedule():
    global schedule_rules_global
    try:
        with open(SCHEDULE_FILE, 'r') as f: schedule_config = json.load(f)
        parsed_rules = {}
        for rule in schedule_config.get("rules", []):
            key = (rule.get("source"), rule.get("destination"))
            if rule.get("start_sec") is not None and rule.get("end_sec") is not None: # Ensure rule is complete
                parsed_rules[key] = {"start_sec": int(rule["start_sec"]), "end_sec": int(rule["end_sec"]), "port": rule.get("port")}
        schedule_rules_global = parsed_rules
        print(f"[VISUALIZER_HOST] Loaded {len(schedule_rules_global)} valid schedule rules.")
    except Exception as e:
        print(f"[VISUALIZER_HOST] Error loading schedule: {e}")

def read_all_logs():
    global log_data_global, last_log_mtime_global
    try:
        if not os.path.exists(LOG_FILE):
            # print("[VISUALIZER_HOST] Log file not found yet.")
            return False # Indicate no update
        current_mtime = os.path.getmtime(LOG_FILE)
        if current_mtime > last_log_mtime_global or not log_data_global:
            temp_data = []
            with open(LOG_FILE, 'r') as f:
                for line_num, line in enumerate(f):
                    try:
                        entry = json.loads(line.strip())
                        # Convert timestamp to datetime object early
                        if 'timestamp' in entry:
                            entry['datetime'] = pd.to_datetime(entry['timestamp'], unit='s')
                        temp_data.append(entry)
                    except json.JSONDecodeError:
                        print(f"[VISUALIZER_HOST] Warning: Skipping malformed JSON line {line_num+1}: {line.strip()}")
            log_data_global = temp_data
            last_log_mtime_global = current_mtime
            print(f"[VISUALIZER_HOST] Loaded {len(log_data_global)} log entries.")
            return True # Indicate update
    except FileNotFoundError: pass
    except Exception as e: print(f"[VISUALIZER_HOST] Error reading log file: {e}")
    return False


def aggregate_data_by_path(current_log_data):
    stats_by_path = defaultdict(lambda: {
        "legit_attempt_count": 0, "legit_success_count": 0, "legit_held_window_closed_count":0,
        "legit_blocked_by_firewall_count": 0, # Legit attempt that hit a closed window at firewall
        "rogue_attempt_count": 0, "rogue_success_breach_count": 0,
        "rogue_blocked_by_firewall_win_count": 0, # Rogue attempt blocked by firewall
        "legit_fails_comm_count":0, "rogue_fails_comm_count":0, # timeouts, socket errors after allowed by firewall
        "legit_success_latencies": [], "rogue_success_latencies": [],
        "timeline_events": [] # (datetime, event_short_type, is_rogue, success_state)
    })

    if not current_log_data: return stats_by_path

    for entry in current_log_data:
        event = entry.get("event", "")
        dt_obj = entry.get('datetime')
        source_zone = entry.get("zone")
        dest_zone = entry.get("destination")
        is_rogue = entry.get("is_rogue_attempt", False)

        if not source_zone or not dest_zone or not dt_obj: continue

        path_key = f"{source_zone} -> {dest_zone}"
        s = stats_by_path[path_key] # s for path_stats

        # Timeline event structure: (datetime, y_category, marker_style, color, label_for_legend)
        # Success states: 0=Fail/Blocked, 1=Success, 2=Held/Attempt
        
        if event == "AttemptSend" or event == "RogueAttemptSend":
            if is_rogue: s["rogue_attempt_count"] += 1
            else: s["legit_attempt_count"] += 1
            s["timeline_events"].append({'dt': dt_obj, 'type': 'Attempt', 'rogue': is_rogue, 'success_state': 2, 'latency': None})
        
        elif event == "SendSuccess" or event == "RogueSendSuccess":
            latency = entry.get("round_trip_latency_ms")
            if is_rogue:
                s["rogue_success_breach_count"] += 1
                if latency is not None: s["rogue_success_latencies"].append(latency)
            else:
                s["legit_success_count"] += 1
                if latency is not None: s["legit_success_latencies"].append(latency)
            s["timeline_events"].append({'dt': dt_obj, 'type': 'Success', 'rogue': is_rogue, 'success_state': 1, 'latency': latency})

        elif event == "Blocked_TimeWindow" or event == "RogueBlocked_TimeWindow":
            if is_rogue: s["rogue_blocked_by_firewall_win_count"] += 1
            else: s["legit_blocked_by_firewall_count"] += 1 # Legit attempt made when path was closed
            s["timeline_events"].append({'dt': dt_obj, 'type': 'BlockedFW', 'rogue': is_rogue, 'success_state': 0, 'latency': None})
        
        elif event == "LegitSend_Held_ClientAwareWindowClosed":
            s["legit_held_window_closed_count"] +=1
            s["timeline_events"].append({'dt': dt_obj, 'type': 'HeldClient', 'rogue': False, 'success_state': 2, 'latency': None})

        elif "SendFail" in event: # Catches Timeout, SocketError, BadAck, Unknown
            if is_rogue: s["rogue_fails_comm_count"] += 1
            else: s["legit_fails_comm_count"] += 1
            s["timeline_events"].append({'dt': dt_obj, 'type': 'FailComm', 'rogue': is_rogue, 'success_state': 0, 'latency': None})


    for path_key in stats_by_path:
        stats_by_path[path_key]["timeline_events"].sort(key=lambda x: x['dt'])
    return stats_by_path


def animate(i, fig, selected_path_key_ref): # Pass selected_path_key as a mutable reference (list)
    if read_all_logs() or not fig.axes: # Force redraw if logs updated or first run
        pass # Continue to redraw
    elif i == 0 and not fig.axes: # First call, no axes yet
        pass
    # else: # No log update and not first call, potentially skip redraw
    #     return [artist for ax in fig.axes for artist in ax.get_children()]


    stats_by_path_agg = aggregate_data_by_path(log_data_global)
    fig.clf() # Clear previous drawing

    if not stats_by_path_agg:
        ax_empty = fig.add_subplot(1,1,1)
        ax_empty.text(0.5, 0.5, 'Waiting for log data or no communication paths found...', ha='center', va='center')
        return [ax_empty]

    # Determine which path to plot
    current_path_to_plot = selected_path_key_ref[0]
    if current_path_to_plot is None or current_path_to_plot not in stats_by_path_agg:
        current_path_to_plot = next(iter(stats_by_path_agg)) # Default to first available path

    stats = stats_by_path_agg[current_path_to_plot]
    source_name_viz, dest_name_viz = current_path_to_plot.split(" -> ")
    
    # --- Overall Title ---
    fig.suptitle(f"Time-Based SCADA Segmentation: {current_path_to_plot}", fontsize=16, y=0.99)
    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1.5]) # Let constrained_layout handle spacing

    # --- 1. Detailed Pie Chart ---
    ax1 = fig.add_subplot(gs[0, 0])
    pie_labels = ['Rogue Blocked (Win)', 'Rogue Success (Breach)', 'Rogue Comm.Fail']
    pie_sizes = [
    stats["rogue_blocked_by_firewall_win_count"],
    stats["rogue_success_breach_count"],
    stats["rogue_fails_comm_count"]
        ]
    pie_colors = ['darkred', 'orange', 'dimgrey']
    pie_explode = [0.05 if s > 0 else 0 for s in pie_sizes] # Explode successful slices

    # Filter out zero-value slices for cleaner pie chart
    valid_indices = [idx for idx, size in enumerate(pie_sizes) if size > 0]
    labels_f = [pie_labels[i] for i in valid_indices]
    sizes_f = [pie_sizes[i] for i in valid_indices]
    colors_f = [pie_colors[i] for i in valid_indices]
    explode_f = [pie_explode[i] for i in valid_indices]

    if sizes_f:
        ax1.pie(sizes_f, labels=labels_f, colors=colors_f, autopct=lambda p: '{:.1f}% ({:.0f})'.format(p, p * sum(sizes_f) / 100),
                startangle=90, explode=explode_f if any(e > 0 for e in explode_f) else None,
                wedgeprops={'edgecolor': 'white', 'linewidth': 0.5}, pctdistance=0.80, textprops={'fontsize': 8})
    else:
        ax1.text(0.5, 0.5, 'No data for pie chart', ha='center', va='center')
    total_legit = stats["legit_attempt_count"] + stats["legit_held_window_closed_count"] # attempts + holds
    total_rogue = stats["rogue_attempt_count"]
    ax1.set_title(f"Rogue Attempt Outcomes for {current_path_to_plot}\n(Total Rogue Attempts: {stats['rogue_attempt_count']})", fontsize=10)    
    ax1.axis('equal')


    # --- 2. Latency Distribution ---
    ax2 = fig.add_subplot(gs[0, 1])
    combined_latencies = []
    latency_labels = []
    latency_colors = []
    if stats["legit_success_latencies"]:
        combined_latencies.append(stats["legit_success_latencies"])
        latency_labels.append(f'Legit Success (Avg: {np.mean(stats["legit_success_latencies"]):.1f}ms)')
        latency_colors.append('skyblue')
    if stats["rogue_success_latencies"]:
        combined_latencies.append(stats["rogue_success_latencies"])
        latency_labels.append(f'Rogue Success (Avg: {np.mean(stats["rogue_success_latencies"]):.1f}ms)')
        latency_colors.append('lightcoral')

    if combined_latencies:
        ax2.hist(combined_latencies, bins=15, label=latency_labels, color=latency_colors, edgecolor='black', stacked=False) # Can set stacked=True
        ax2.legend(fontsize='small')
        # Add overall average if meaningful
        all_lats = [l for sublist in combined_latencies for l in sublist]
        if all_lats:
             ax2.axvline(np.mean(all_lats), color='red', linestyle='dashed', linewidth=1, label=f'Overall Avg: {np.mean(all_lats):.1f}ms')
    else:
        ax2.text(0.5, 0.5, 'No successful sends with latency data', ha='center', va='center')
    ax2.set_title('Latency Distribution for Successful Sends', fontsize=10)
    ax2.set_xlabel('Round Trip Time (ms)', fontsize=9)
    ax2.set_ylabel('Frequency', fontsize=9)
    ax2.xaxis.label.set_visible(True)
    ax2.tick_params(axis='x', labelbottom=True) # Force tick labels



    # --- 3. Event Timeline with Scheduled Windows ---
    ax3 = fig.add_subplot(gs[1, :]) # Span bottom row
    path_schedule_info = schedule_rules_global.get((source_name_viz, dest_name_viz))

    if stats["timeline_events"]:
        timeline_df = pd.DataFrame(stats["timeline_events"])
        min_time_dt = timeline_df['dt'].min()
        max_time_dt = timeline_df['dt'].max()

        # Plot scheduled windows as background
        if path_schedule_info and pd.notna(min_time_dt) and pd.notna(max_time_dt):
            plot_start_time = min_time_dt.floor('min') #- pd.Timedelta(seconds=10) # Add padding
            plot_end_time = max_time_dt.ceil('min') #+ pd.Timedelta(seconds=10)   # Add padding
            ax3.set_xlim(plot_start_time, plot_end_time)

            current_minute_iter = plot_start_time
            while current_minute_iter <= plot_end_time:
                win_start_dt = current_minute_iter + pd.Timedelta(seconds=path_schedule_info['start_sec'])
                win_end_dt = current_minute_iter + pd.Timedelta(seconds=path_schedule_info['end_sec'])
                ax3.add_patch(patches.Rectangle((win_start_dt, -0.5), win_end_dt - win_start_dt,
                                                5, # Height of rect to cover y-categories
                                                facecolor='gainsboro', alpha=0.4, edgecolor='darkgrey', linewidth=0.5, zorder=0, label='_nolegend_'))
                current_minute_iter += pd.Timedelta(minutes=1)
        
        # Y-categories for timeline events
        y_categories = {
            'Attempt': 0, 'HeldClient': 0.8, 'BlockedFW': 1.6, 'FailComm': 2.4, 'Success': 3.2
        } # Base Y for legit, rogue will be offset
        y_rogue_offset = 0.25 # Offset rogue events slightly on Y for visibility

        event_styles = { # (marker, size, alpha, zorder, color_map_key)
            ('Attempt', False): ('o', 15, 0.7, 5, 'attempt_legit'),
            ('Attempt', True):  ('x', 25, 0.7, 6, 'attempt_rogue'),
            ('HeldClient', False): ('>', 20, 0.8, 7, 'held_legit'), # Client held it
            ('BlockedFW', False): ('o', 20, 1, 8, 'blocked_fw_legit'), # FW blocked legit
            ('BlockedFW', True):  ('x', 35, 1, 9, 'blocked_fw_rogue_WIN'), # FW blocked rogue - SECURITY WIN!
            ('FailComm', False): ('s', 15, 0.8, 3, 'fail_comm_legit'),
            ('FailComm', True):  ('P', 25, 0.8, 4, 'fail_comm_rogue'),
            ('Success', False): ('o', 30, 1, 10, 'success_legit'),
            ('Success', True):  ('X', 40, 1, 11, 'success_rogue_BREACH'), # Rogue got through - BREACH!
        }
        color_map = {
            'attempt_legit': 'blue', 'attempt_rogue': 'lightblue',
            'held_legit': 'skyblue',
            'blocked_fw_legit': 'lightcoral', 'blocked_fw_rogue_WIN': 'darkred',
            'fail_comm_legit': 'grey', 'fail_comm_rogue': 'dimgrey',
            'success_legit': 'green', 'success_rogue_BREACH': 'orange',
        }

        # Plot events
        for _, event_row in timeline_df.iterrows():
            y_base = y_categories.get(event_row['type'], -1)
            style_key = (event_row['type'], event_row['rogue'])
            marker, size, alpha, z, color_key = event_styles.get(style_key, ('D',10,0.5,1,'default_black'))
            color = color_map.get(color_key,'black')
            
            y_val = y_base + y_rogue_offset if event_row['rogue'] else y_base
            ax3.scatter(event_row['dt'], y_val, marker=marker, s=size, alpha=alpha, color=color, zorder=z, edgecolors='black', linewidths=0.3)

        # Setup Y-axis labels from categories
        y_ticks = []
        y_tick_labels = []
        for cat, y_base_val in y_categories.items():
            y_ticks.extend([y_base_val, y_base_val + y_rogue_offset])
            y_tick_labels.extend([f"L: {cat}", f"R: {cat}"])
        # Sort them by y_value for correct display
        sorted_y_display = sorted(zip(y_ticks, y_tick_labels), key=lambda x: x[0])
        ax3.set_yticks([item[0] for item in sorted_y_display])
        ax3.set_yticklabels([item[1] for item in sorted_y_display], fontsize=7)
        ax3.set_ylim(-0.6, max(y_categories.values()) + y_rogue_offset + 0.6)

        # Format X-axis for time
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        fig.autofmt_xdate(rotation=30, ha='right')
        ax3.set_xlabel('Time (Simulation Clock)', fontsize=9)
        ax3.set_ylabel('Event Type (L: Legit, R: Rogue)', fontsize=9)
        ax3.tick_params(axis='x', which='major', labelsize=8)
        ax3.grid(True, axis='y', linestyle=':', linewidth=0.5)
        ax3.set_title('Detailed Event Timeline (Shaded: Scheduled Open Windows for Path)', fontsize=10)

        # Custom Legend for Timeline
        legend_handles = []
        # Base legend for scheduled window
        legend_handles.append(patches.Patch(facecolor='gainsboro', alpha=0.4, edgecolor='darkgrey', label='Scheduled Open Window'))
        # Event specific legends
        unique_styles_for_legend = {} # To avoid duplicate legend entries
        for (etype, is_r), (m, sz, al, zo, ck) in event_styles.items():
            label = f"{'Rogue ' if is_r else 'Legit '}{etype.replace('FW','Blocked').replace('Comm','Fail')}"
            if label not in unique_styles_for_legend:
                legend_handles.append(plt.Line2D([0], [0], marker=m, color=color_map[ck], linestyle='None', markersize=np.sqrt(sz/2), label=label))
                unique_styles_for_legend[label] = True
        ax3.legend(handles=legend_handles, loc='upper center', bbox_to_anchor=(0.5, -0.25), ncol=3, fontsize='x-small', frameon=False)

    else:
        ax3.text(0.5, 0.5, 'No timeline events for this path', ha='center', va='center')

    plt.tight_layout(rect=[0, 0.08, 1, 0.95]) # Adjust layout for suptitle and timeline legend
    fig.subplots_adjust(hspace=.6)
    # Return all artists for blitting if used (not fully implemented here for simplicity of full redraw)
    return [child for ax_child in fig.axes for child in ax_child.get_children()]


if __name__ == "__main__":
    load_schedule()
    selected_path_key_ref = [PATH_TO_VISUALIZE_EXPLICITLY] # Use a list to make it mutable for potential future UI interaction

    fig = plt.figure(figsize=(16, 11), constrained_layout=True)
    #plt.subplots_adjust(hspace=7) # Padding between subplots


    # To allow cycling through paths if PATH_TO_VISUALIZE_EXPLICITLY is None
    # This is conceptual for now, would need a button or key press event in matplotlib
    # For now, it will default to the first path if None is specified.
    
    ani = FuncAnimation(fig, animate, fargs=(fig, selected_path_key_ref,), interval=UPDATE_INTERVAL, cache_frame_data=False, repeat=False)
    plt.show()
    print("[VISUALIZER_HOST] Plot window closed.")