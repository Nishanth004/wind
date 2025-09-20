import json
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.dates as mdates
import os
import time
import datetime
import pandas as pd
from collections import defaultdict
import numpy as np
import argparse

# --- Configuration ---
LOG_FILE = "./logs/simulation.log"
SCHEDULE_FILE = "schedule.json"
PATH_TO_VISUALIZE_EXPLICITLY = "operational_control_zone -> scada_presentation_zone" # Default path to plot

PLOT_OUTPUT_DIR = "visualization_output" # New directory for output plots

log_data_global = []
schedule_rules_global = {}

def ensure_dir(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Created directory: {directory_path}")

def load_schedule():
    global schedule_rules_global
    try:
        with open(SCHEDULE_FILE, 'r') as f: schedule_config = json.load(f)
        parsed_rules = {}
        for rule in schedule_config.get("rules", []):
            key = (rule.get("source"), rule.get("destination"))
            if rule.get("start_sec") is not None and rule.get("end_sec") is not None:
                parsed_rules[key] = {"start_sec": int(rule["start_sec"]), "end_sec": int(rule["end_sec"]), "port": rule.get("port")}
        schedule_rules_global = parsed_rules
        print(f"[VISUALIZER_HOST] Loaded {len(schedule_rules_global)} valid schedule rules.")
    except Exception as e:
        print(f"[VISUALIZER_HOST] Error loading schedule: {e}")

def read_all_logs():
    global log_data_global
    try:
        if not os.path.exists(LOG_FILE):
            print("[VISUALIZER_HOST] Log file not found yet. Cannot generate plots.")
            return False
        
        temp_data = []
        with open(LOG_FILE, 'r') as f:
            for line_num, line in enumerate(f):
                try:
                    entry = json.loads(line.strip())
                    if 'timestamp' in entry:
                        entry['datetime'] = pd.to_datetime(entry['timestamp'], unit='s')
                    temp_data.append(entry)
                except json.JSONDecodeError:
                    print(f"[VISUALIZER_HOST] Warning: Skipping malformed JSON line {line_num+1}: {line.strip()}")
        log_data_global = temp_data
        print(f"[VISUALIZER_HOST] Loaded {len(log_data_global)} log entries.")
        return True
    except FileNotFoundError:
        print(f"[VISUALIZER_HOST] Log file '{LOG_FILE}' not found.")
    except Exception as e:
        print(f"[VISUALIZER_HOST] Error reading log file: {e}")
    return False

def aggregate_data_by_path(current_log_data):
    stats_by_path = defaultdict(lambda: {
        "legit_attempt_count": 0, "legit_success_count": 0, "legit_held_window_closed_count":0,
        "legit_blocked_by_firewall_count": 0,
        "rogue_attempt_count": 0, "rogue_success_breach_count": 0,
        "rogue_blocked_by_firewall_win_count": 0,
        "legit_fails_comm_count":0, "rogue_fails_comm_count":0,
        "legit_success_latencies": [], "rogue_success_latencies": [],
        "timeline_events": []
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
        s = stats_by_path[path_key]

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
            else: s["legit_blocked_by_firewall_count"] += 1
            s["timeline_events"].append({'dt': dt_obj, 'type': 'BlockedFW', 'rogue': is_rogue, 'success_state': 0, 'latency': None})
        
        elif event == "LegitSend_Held_ClientAwareWindowClosed":
            s["legit_held_window_closed_count"] +=1
            s["timeline_events"].append({'dt': dt_obj, 'type': 'HeldClient', 'rogue': False, 'success_state': 2, 'latency': None})

        elif "SendFail" in event:
            if is_rogue: s["rogue_fails_comm_count"] += 1
            else: s["legit_fails_comm_count"] += 1
            s["timeline_events"].append({'dt': dt_obj, 'type': 'FailComm', 'rogue': is_rogue, 'success_state': 0, 'latency': None})

    for path_key in stats_by_path:
        stats_by_path[path_key]["timeline_events"].sort(key=lambda x: x['dt'])
    return stats_by_path

def generate_plots_and_save(selected_path_key, output_dir, start_time_arg=None, end_time_arg=None):
    ensure_dir(output_dir)
    load_schedule()
    if not read_all_logs():
        print("Exiting: No log data available for plotting.")
        return

    stats_by_path_agg = aggregate_data_by_path(log_data_global)

    if not stats_by_path_agg:
        print("No aggregated data to plot.")
        return

    if selected_path_key is None or selected_path_key not in stats_by_path_agg:
        print(f"Warning: Specified path '{selected_path_key}' not found or not provided. Defaulting to first available path.")
        selected_path_key = next(iter(stats_by_path_agg))

    stats = stats_by_path_agg[selected_path_key]
    source_name_viz, dest_name_viz = selected_path_key.split(" -> ")
    
    # --- Plot 1: Detailed Pie Chart ---
    fig1, ax1 = plt.subplots(figsize=(14, 12)) # Pie chart figure size
    pie_labels = ['Rogue Blocked (Win)', 'Rogue Success (Breach)', 'Rogue Comm.Fail']
    pie_sizes = [
        stats["rogue_blocked_by_firewall_win_count"],
        stats["rogue_success_breach_count"],
        stats["rogue_fails_comm_count"]
    ]
    pie_colors = ['darkred', 'gold', 'dimgrey']
    pie_explode = [0.05 if s > 0 else 0 for s in pie_sizes]

    valid_indices = [idx for idx, size in enumerate(pie_sizes) if size > 0]
    labels_f = [pie_labels[i] for i in valid_indices]
    sizes_f = [pie_sizes[i] for i in valid_indices]
    colors_f = [pie_colors[i] for i in valid_indices]
    explode_f = [pie_explode[i] for i in valid_indices]

    if sizes_f:
        ax1.pie(sizes_f, labels=labels_f, colors=colors_f, autopct=lambda p: '{:.1f}% ({:.0f})'.format(p, p * sum(sizes_f) / 100),
                startangle=90, explode=explode_f if any(e > 0 for e in explode_f) else None,
                wedgeprops={'edgecolor': 'white', 'linewidth': 0.5}, pctdistance=0.75, textprops={'fontsize': 18}) # Pie chart percentage/label font size
    else:
        ax1.text(0.5, 0.5, 'No data for pie chart', ha='center', va='center', fontsize=20) # "No data" message font size
    ax1.set_title(f"Rogue Attempt Outcomes for {selected_path_key}\n(Total Rogue Attempts: {stats['rogue_attempt_count']})", fontsize=22) # Pie chart title font size
    ax1.axis('equal')
    plt.tight_layout()
    fig1.savefig(os.path.join(output_dir, f"pie_chart_{selected_path_key.replace(' -> ', '_')}.png"), dpi=300)
    plt.close(fig1)
    print(f"Saved pie chart for {selected_path_key}")

    # --- Plot 2: Latency Distribution ---
    fig2, ax2 = plt.subplots(figsize=(14, 12)) # Latency plot figure size
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
        ax2.hist(combined_latencies, bins=20, label=latency_labels, color=latency_colors, edgecolor='black', stacked=False, alpha=0.8)
        ax2.legend(fontsize=18) # Latency plot legend font size
        all_lats = [l for sublist in combined_latencies for l in sublist]
        if all_lats:
             ax2.axvline(np.mean(all_lats), color='red', linestyle='dashed', linewidth=1.5, label=f'Overall Avg: {np.mean(all_lats):.1f}ms')
    else:
        ax2.text(0.5, 0.5, 'No successful sends with latency data', ha='center', va='center', fontsize=20) # "No data" message font size
    ax2.set_title(f'Latency Distribution for Successful Sends ({selected_path_key})', fontsize=22) # Latency plot title font size
    ax2.set_xlabel('Round Trip Time (ms)', fontsize=20) # X-axis label font size
    ax2.set_ylabel('Frequency', fontsize=20) # Y-axis label font size
    ax2.tick_params(axis='x', labelbottom=True, labelsize=18) # X-axis tick labels font size
    ax2.tick_params(axis='y', labelsize=18) # Y-axis tick labels font size
    ax2.grid(True, linestyle=':', alpha=0.7)
    plt.tight_layout()
    fig2.savefig(os.path.join(output_dir, f"latency_dist_{selected_path_key.replace(' -> ', '_')}.png"), dpi=300)
    plt.close(fig2)
    print(f"Saved latency distribution for {selected_path_key}")

    # --- Plot 3: Event Timeline with Scheduled Windows ---
    fig3, ax3 = plt.subplots(figsize=(24, 16)) # Timeline plot figure size
    path_schedule_info = schedule_rules_global.get((source_name_viz, dest_name_viz))

    if stats["timeline_events"]:
        timeline_df = pd.DataFrame(stats["timeline_events"])
        
        # Determine time window for the plot
        if start_time_arg and end_time_arg:
            plot_start_time = pd.to_datetime(start_time_arg)
            plot_end_time = pd.to_datetime(end_time_arg)
        else:
            min_time_dt = timeline_df['dt'].min()
            max_time_dt = timeline_df['dt'].max()
            plot_start_time = min_time_dt.floor('min') - pd.Timedelta(seconds=5) if pd.notna(min_time_dt) else None
            plot_end_time = max_time_dt.ceil('min') + pd.Timedelta(seconds=5) if pd.notna(max_time_dt) else None

        if plot_start_time and plot_end_time:
            ax3.set_xlim(plot_start_time, plot_end_time)

            # Plot scheduled windows as background
            if path_schedule_info:
                current_minute_iter = plot_start_time.replace(second=0, microsecond=0)
                if current_minute_iter > plot_start_time:
                     current_minute_iter = current_minute_iter - pd.Timedelta(minutes=1)

                while current_minute_iter <= plot_end_time + pd.Timedelta(minutes=1):
                    win_start_dt = current_minute_iter + pd.Timedelta(seconds=path_schedule_info['start_sec'])
                    win_end_dt = current_minute_iter + pd.Timedelta(seconds=path_schedule_info['end_sec'])
                    
                    rect_start = max(win_start_dt, plot_start_time)
                    rect_end = min(win_end_dt, plot_end_time)
                    
                    if rect_start < rect_end:
                        ax3.add_patch(patches.Rectangle((rect_start, -0.8), rect_end - rect_start,
                                                        5, # Height of rect to cover y-categories
                                                        facecolor='gainsboro', alpha=0.4, edgecolor='darkgrey', linewidth=0.5, zorder=0, label='_nolegend_'))
                    current_minute_iter += pd.Timedelta(minutes=1)
        
        # ACTIVE Y-CATEGORIES FOR THE TIMELINE (COMPRESSED)
        y_categories = {
            'Attempt': 0,
            'BlockedFW': 0.5, # Reduced spacing
            'Success': 1.0    # Reduced spacing
        }
        y_rogue_offset = 0.2 # Reduced offset for vertical separation between L and R

        # (marker, size, alpha, zorder, color_map_key, scatter_line_width, scatter_edge_color, legend_line_width)
        # ACTIVE EVENT STYLES (markersizes increased for clarity)
        event_styles = { 
            ('Attempt', False): ('o', 55, 0.7, 5, 'attempt_legit', 0.8, 'black', 0.8),
            ('Attempt', True):  ('x', 65, 0.7, 6, 'attempt_rogue', 2.5, None, 2.5),
            ('BlockedFW', False): ('o', 60, 1, 8, 'blocked_fw_legit', 0.8, 'black', 0.8),
            ('BlockedFW', True):  ('x', 75, 1, 9, 'blocked_fw_rogue_WIN', 3.0, None, 3.0),
            ('Success', False): ('o', 70, 1, 10, 'success_legit', 0.8, 'black', 0.8),
            ('Success', True):  ('X', 80, 1, 11, 'success_rogue_BREACH', 3.0, None, 3.0),
        }
        # ACTIVE COLOR MAP
        color_map = {
            'attempt_legit': 'blue',
            'attempt_rogue': 'darkorange',
            'blocked_fw_legit': 'lightcoral', 'blocked_fw_rogue_WIN': 'darkred',
            'success_legit': 'green', 'success_rogue_BREACH': 'gold',
        }

        # Plot events
        for _, event_row in timeline_df.iterrows():
            if plot_start_time and plot_end_time and not (plot_start_time <= event_row['dt'] <= plot_end_time):
                continue

            if event_row['type'] not in y_categories:
                continue

            y_base = y_categories.get(event_row['type'], -1)
            style_key = (event_row['type'], event_row['rogue'])
            
            if style_key not in event_styles:
                continue

            marker, size, alpha, z, color_key, lw_scatter, ec_scatter, _ = event_styles[style_key]
            color = color_map.get(color_key,'black')
            
            y_val = y_base + y_rogue_offset if event_row['rogue'] else y_base
            ax3.scatter(event_row['dt'], y_val, marker=marker, s=size, alpha=alpha, color=color, zorder=z,
                        edgecolors=ec_scatter if ec_scatter else color,
                        linewidths=lw_scatter)

        y_ticks = []
        y_tick_labels = []
        for cat, y_base_val in y_categories.items(): # Loop only through the active categories
            y_ticks.extend([y_base_val, y_base_val + y_rogue_offset])
            y_tick_labels.extend([f"L: {cat.replace('BlockedFW', 'Blocked by FW')}",
                                  f"R: {cat.replace('BlockedFW', 'Blocked by FW')}"])
        sorted_y_display = sorted(zip(y_ticks, y_tick_labels), key=lambda x: x[0])
        ax3.set_yticks([item[0] for item in sorted_y_display])
        ax3.set_yticklabels([item[1] for item in sorted_y_display], fontsize=22) # Y-axis tick labels font size (increased)
        
        # Tightly set Y-axis limits based on actual plotted range
        min_y_val = min(y_categories.values())
        max_y_val = max(y_categories.values()) + y_rogue_offset
        ax3.set_ylim(min_y_val - 0.2, max_y_val + 0.2) # Adjusted limits for minimal padding

        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        fig3.autofmt_xdate(rotation=30, ha='right')
        ax3.set_xlabel('Time (Simulation Clock)', fontsize=24) # X-axis label font size
        ax3.set_ylabel('Event Type (L: Legit, R: Rogue)', fontsize=24) # Y-axis label font size
        ax3.tick_params(axis='x', which='major', labelsize=20) # X-axis tick labels font size
        ax3.tick_params(axis='y', which='major', labelsize=20) # Y-axis tick labels font size
        ax3.grid(True, axis='y', linestyle=':', linewidth=0.5)
        ax3.set_title(f'Detailed Event Timeline for {selected_path_key} ', fontsize=28) # Timeline plot title font size

        legend_handles = []
        legend_handles.append(patches.Patch(facecolor='gainsboro', alpha=0.4, edgecolor='darkgrey', label='Scheduled Open Window'))
        
        unique_styles_for_legend = {}
        for (etype, is_r), (m, sz, al, zo, ck, lw_s, ec_s, lw_l) in event_styles.items(): # Loop over *active* event styles
            label_text = ""
            if etype == 'Attempt':
                label_text = f"{'Rogue' if is_r else 'Legit'} Attempt"
            elif etype == 'BlockedFW':
                label_text = f"{'Rogue' if is_r else 'Legit'} Blocked by FW"
                if is_r: label_text += " (Win)"
            elif etype == 'Success':
                label_text = f"{'Rogue' if is_r else 'Legit'} Success"
                if is_r: label_text += " (Breach)"
            
            if label_text not in unique_styles_for_legend:
                legend_markeredgecolor = 'black'
                if m in ['x', 'X']:
                    legend_markeredgecolor = color_map[ck]
                
                legend_handles.append(plt.Line2D([0], [0], marker=m, color=color_map[ck], linestyle='None',
                                                 markersize=np.sqrt(sz/2)*1.8, # Legend marker size
                                                 label=label_text,
                                                 markeredgewidth=lw_l,
                                                 markeredgecolor=legend_markeredgecolor))
                unique_styles_for_legend[label_text] = True
        
        ax3.legend(handles=legend_handles, loc='upper center', bbox_to_anchor=(0.5, -0.25), ncol=3, fontsize=26, frameon=False, markerscale=1.5) # Timeline plot legend font size
    else:
        ax3.text(0.5, 0.5, 'No timeline events for this path', ha='center', va='center', fontsize=20) # "No data" message font size

    # Adjusted rect for more padding, especially bottom for huge legend
    plt.tight_layout(rect=[0.04, 0.1, 0.98, 0.95]) # [left, bottom, right, top]
    fig3.savefig(os.path.join(output_dir, f"timeline_{selected_path_key.replace(' -> ', '_')}.png"), dpi=300)
    plt.close(fig3)
    print(f"Saved timeline plot for {selected_path_key}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and save SCADA segmentation visualization plots.")
    parser.add_argument("--path", type=str, default=PATH_TO_VISUALIZE_EXPLICITLY,
                        help=f"The communication path to visualize (e.g., 'zone_A -> zone_B'). Default: '{PATH_TO_VISUALIZE_EXPLICITLY}'")
    parser.add_argument("--output_dir", type=str, default=PLOT_OUTPUT_DIR,
                        help=f"Directory to save the generated plots. Default: '{PLOT_OUTPUT_DIR}'")
    parser.add_argument("--start_time", type=str,
                        help="Start time for the timeline plot (e.g., '2025-05-07 18:25:00').")
    parser.add_argument("--end_time", type=str,
                        help="End time for the timeline plot (e.g., '2025-05-07 18:30:00').")
    
    args = parser.parse_args()

    generate_plots_and_save(args.path, args.output_dir, args.start_time, args.end_time)
    print("All plots generated and saved.")