import cfgrib
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import os
import argparse
import pandas as pd
import numpy as np # For sqrt

# --- Configuration ---
DEFAULT_GRIB_FILE_DIR = "downloaded_grib_files"
PLOT_OUTPUT_DIR = "grib_plots_wind" # New folder for wind-specific plots
CSV_OUTPUT_DIR = "grib_csv_data_wind"  # New folder for wind-specific CSVs

# Define GRIB shortNames for parameters relevant to wind energy
# You might need to check the .idx files or experiment to find the exact shortNames
# used in your specific AIFS/IFS data. Common ones:
WIND_PARAM_SHORTNAMES = [
    '10u', '10v',       # 10m wind components
    '100u', '100v',     # 100m wind components (often closer to hub height)
    '2t',               # 2m temperature (for air density context)
    'msl',              # Mean sea level pressure (for air density context)
    'sp',               # Surface pressure (alternative for air density)
    # Add others if you find them, e.g., direct wind speed '10si', 'si10', 'ws', or gust parameters.
    # For AIFS, the parameter names might be slightly different, check idx files.
    # e.g. looking at an index file for aifs-single/0p25/oper:
    # "param": "10u" -> "10 metre U wind component"
    # "param": "10v" -> "10 metre V wind component"
    # "param": "100u" -> "100 metre U wind component"
    # "param": "100v" -> "100 metre V wind component"
    # "param": "2t" -> "2 metre temperature"
    # "param": "msl" -> "Mean sea level pressure"
]
# --- End Configuration ---

def ensure_dir(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Created directory: {directory_path}")

def list_grib_files(directory):
    if not os.path.exists(directory):
        print(f"Directory not found: {directory}")
        return []
    files = [f for f in os.listdir(directory) if f.endswith(".grib2")]
    if not files:
        print(f"No .grib2 files found in {directory}")
    return files

def calculate_wind_speed_and_direction(ds, u_comp_name='10u', v_comp_name='10v', height_prefix="10m"):
    """Calculates wind speed and direction if u and v components are present."""
    calculated_vars = {}
    if u_comp_name in ds.data_vars and v_comp_name in ds.data_vars:
        u_comp = ds[u_comp_name]
        v_comp = ds[v_comp_name]

        # Wind Speed
        wind_speed = np.sqrt(u_comp**2 + v_comp**2)
        wind_speed.name = f'{height_prefix}_wind_speed'
        wind_speed.attrs['long_name'] = f'{height_prefix} Wind Speed'
        wind_speed.attrs['units'] = u_comp.attrs.get('units', 'm s**-1') # Assume u/v have same units
        calculated_vars[wind_speed.name] = wind_speed
        print(f"  Calculated: {wind_speed.name}")

        # Wind Direction (meteorological convention: from where the wind blows, 0 deg = North, 90 deg = East)
        # atan2 returns radians in [-pi, pi]. Convert to degrees [0, 360]
        # wind_dir_rad = np.arctan2(u_comp, v_comp) # math.atan2(u,v) for direction TO
        # wind_dir_deg_to = np.degrees(wind_dir_rad)
        # wind_dir_deg_from = (270 - wind_dir_deg_to) % 360 # Or (180 - wind_dir_deg_to + 90) % 360
        # More standard met formula from U/V: WD = 270 - atan2(V,U) * 180/pi; if WD<0 then WD = WD + 360
        wind_dir_deg = (270 - np.degrees(np.arctan2(v_comp, u_comp))) % 360
        wind_dir_deg.name = f'{height_prefix}_wind_direction'
        wind_dir_deg.attrs['long_name'] = f'{height_prefix} Wind Direction (from, 0=N)'
        wind_dir_deg.attrs['units'] = 'degrees'
        calculated_vars[wind_dir_deg.name] = wind_dir_deg
        print(f"  Calculated: {wind_dir_deg.name}")
    return calculated_vars


def process_variable_data(data_array, var_name, base_filename_prefix, is_calculated=False):
    """Processes a single DataArray: attempts to plot and save CSV."""
    processed_output = False
    if 'latitude' not in data_array.coords or 'longitude' not in data_array.coords:
        print(f"  Skipping {var_name}: Missing latitude/longitude coordinates.")
        return processed_output

    # --- Plotting (if 2D) ---
    if len(data_array.shape) == 2:
        print(f"  Attempting to plot {var_name}...")
        try:
            plt.figure(figsize=(12, 8))
            ax = plt.axes(projection=ccrs.PlateCarree())
            
            # Choose colormap based on variable
            cmap = 'viridis'
            if 'wind_speed' in var_name.lower():
                cmap = 'BuPu' # Or 'viridis', 'plasma'
            elif 'wind_direction' in var_name.lower():
                cmap = 'hsv' # Good for cyclic data like direction
            elif '2t' in var_name.lower() or 'temperature' in data_array.attrs.get('long_name', '').lower():
                cmap = 'coolwarm'
            elif 'msl' in var_name.lower() or 'pressure' in data_array.attrs.get('long_name', '').lower():
                cmap = 'Blues'

            data_array.plot(ax=ax, x='longitude', y='latitude', transform=ccrs.PlateCarree(),
                            cmap=cmap, cbar_kwargs={'label': f"{data_array.attrs.get('long_name', var_name)} ({data_array.attrs.get('units', '')})"})
            ax.coastlines()
            ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
            
            title_step = data_array.attrs.get('step', 'N/A')
            title_valid_time = data_array.attrs.get('valid_time', 'N/A')
            # Ensure step and valid_time are strings for the title
            if hasattr(title_step, 'astype') and pd.notnull(title_step):
                try: title_step = str(title_step.astype('timedelta64[h]'))
                except: title_step = str(title_step)
            if hasattr(title_valid_time, 'astype') and pd.notnull(title_valid_time):
                try: title_valid_time = str(title_valid_time.astype('datetime64[s]'))
                except: title_valid_time = str(title_valid_time)

            plt.title(f"{data_array.attrs.get('long_name', var_name)} from {base_filename_prefix}\nStep: {title_step}, Valid Time: {title_valid_time}")

            plot_filename_base = f"{base_filename_prefix}_{var_name}.png"
            plot_filepath = os.path.join(PLOT_OUTPUT_DIR, plot_filename_base)
            ensure_dir(PLOT_OUTPUT_DIR)
            plt.savefig(plot_filepath)
            print(f"  Saved plot to {plot_filepath}")
            plt.close()
            processed_output = True
        except ImportError:
            print("  Skipping map plot: cartopy is not installed or not working.")
        except Exception as e:
            print(f"  Error plotting {var_name}: {e}")
            # import traceback; traceback.print_exc() # for more detailed error
    else:
        print(f"  Skipping plot for {var_name}: Not a 2D variable (shape: {data_array.shape}).")

    # --- CSV Export (if 2D) ---
    if len(data_array.shape) == 2:
        print(f"  Attempting to export {var_name} to CSV...")
        try:
            if data_array.latitude.ndim == 1 and data_array.longitude.ndim == 1:
                df = data_array.to_pandas()
            else:
                print(f"    {var_name} has 2D coordinates, stacking for CSV export.")
                df = data_array.stack(point=('latitude', 'longitude')).to_pandas().reset_index()
                df.rename(columns={0: var_name}, inplace=True) # Rename the data column

            csv_filename_base = f"{base_filename_prefix}_{var_name}.csv"
            csv_filepath = os.path.join(CSV_OUTPUT_DIR, csv_filename_base)
            ensure_dir(CSV_OUTPUT_DIR)
            df.to_csv(csv_filepath, index=True, float_format='%.3f') # Control float precision
            print(f"  Saved CSV to {csv_filepath}")
            processed_output = True
        except Exception as e:
            print(f"  Error exporting {var_name} to CSV: {e}")
            # import traceback; traceback.print_exc()
    else:
        print(f"  Skipping CSV export for {var_name}: Not a 2D variable (shape: {data_array.shape}).")
    
    return processed_output


def inspect_grib_file_data(filepath):
    print(f"\n--- Inspecting GRIB File for Wind Parameters: {filepath} ---")
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}")
        return

    base_filename_prefix = os.path.splitext(os.path.basename(filepath))[0]

    try:
        # Filter for specific wind-related parameters when opening
        # Use 'shortName' as it's common in cfgrib attributes
        datasets = cfgrib.open_datasets(
            filepath,
            backend_kwargs={'filter_by_keys': {'shortName': WIND_PARAM_SHORTNAMES}}
        )

        if not datasets:
            print(f"No datasets found for specified wind parameters ({WIND_PARAM_SHORTNAMES}) in the file.")
            # Optionally, try opening without filter to see all params
            # print("Trying to open without filter to see all available parameters...")
            # all_datasets = cfgrib.open_datasets(filepath)
            # if all_datasets:
            #     for i, ds_all in enumerate(all_datasets):
            #         print(f"\n  Available params in unfiltered Dataset {i+1}: {list(ds_all.data_vars.keys())}")
            # else:
            #     print("  Still no datasets found even without filter.")
            return

        print(f"Found {len(datasets)} dataset(s) containing one or more specified wind parameters.")

        for i, ds in enumerate(datasets):
            print(f"\n--------------- Wind-Related Dataset {i+1} ---------------")
            print("Dataset Summary (Xarray):")
            print(ds)

            # Calculate wind speed and direction for 10m and 100m if components are present
            calculated_ds_vars = {}
            calculated_ds_vars.update(calculate_wind_speed_and_direction(ds, '10u', '10v', '10m'))
            calculated_ds_vars.update(calculate_wind_speed_and_direction(ds, '100u', '100v', '100m'))

            processed_any_var_in_ds = False
            # Process original (filtered) variables
            for var_name in ds.data_vars:
                print(f"\n  Processing GRIB Parameter: {var_name}")
                print(f"    Long Name: {ds[var_name].attrs.get('long_name', 'N/A')}")
                print(f"    Units: {ds[var_name].attrs.get('units', 'N/A')}")
                if process_variable_data(ds[var_name], var_name, f"{base_filename_prefix}_ds{i+1}"):
                    processed_any_var_in_ds = True
            
            # Process calculated variables (wind speed, direction)
            for calc_var_name, calc_data_array in calculated_ds_vars.items():
                print(f"\n  Processing Calculated Parameter: {calc_var_name}")
                if process_variable_data(calc_data_array, calc_var_name, f"{base_filename_prefix}_ds{i+1}", is_calculated=True):
                    processed_any_var_in_ds = True
            
            if not processed_any_var_in_ds:
                print("    No 2D variables processed for plotting/CSV in this dataset.")
            print("--------------------------------------")

    except Exception as e:
        print(f"An error occurred while processing {filepath}: {e}")
        import traceback
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(description="Inspect GRIB2 files for wind-related parameters, plot data, and save to CSV.")
    # ... (parser arguments remain the same as your last version) ...
    parser.add_argument("grib_file", nargs='?', default=None,
                        help="Path to a specific GRIB2 file to inspect. If not provided, lists files in default directory.")
    parser.add_argument("--dir", default=DEFAULT_GRIB_FILE_DIR,
                        help=f"Directory containing GRIB files (default: {DEFAULT_GRIB_FILE_DIR})")
    parser.add_argument("--list", action="store_true",
                        help="List available GRIB files in the directory and exit.")
    parser.add_argument("--all", action="store_true",
                        help="Inspect all GRIB files found in the directory.")

    args = parser.parse_args()

    ensure_dir(PLOT_OUTPUT_DIR)
    ensure_dir(CSV_OUTPUT_DIR)

    if args.list:
        print(f"Available GRIB files in '{args.dir}':")
        files = list_grib_files(args.dir)
        for f in files: print(f"  - {f}")
        return

    if args.grib_file:
        inspect_grib_file_data(args.grib_file)
    elif args.all:
        files = list_grib_files(args.dir)
        if files:
            for f_name in files:
                inspect_grib_file_data(os.path.join(args.dir, f_name))
        else:
            print(f"No GRIB files found in '{args.dir}' to inspect.")
    else:
        # ... (interactive choice logic remains the same) ...
        files = list_grib_files(args.dir)
        if files:
            print(f"Available GRIB files in '{args.dir}':")
            for i, f_name in enumerate(files): print(f"  {i+1}. {f_name}")
            try:
                choice = int(input("Enter the number of the file to inspect (or 0 to exit): "))
                if 0 < choice <= len(files):
                    inspect_grib_file_data(os.path.join(args.dir, files[choice-1]))
            except ValueError: print("Invalid input.")
            except IndexError: print("Invalid choice.")
        else:
            print(f"No GRIB files found. Use the download script first or specify a file path.")
            parser.print_help()

if __name__ == "__main__":
    # import matplotlib
    # matplotlib.use('Agg') # Uncomment if running in a non-GUI environment and only saving files
    main()