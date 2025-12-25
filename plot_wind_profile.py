# -*- coding: utf-8 -*-
# File: plot_wind_profile.py
# Version: 5.2 (Full Restoration for Dashboard Integration)
#

import matplotlib
import sys

# --- 1. BACKEND INITIALIZATION ---
try:
    # Force TkAgg to ensure the plot window pops up over the dashboard
    matplotlib.use('TkAgg')
except Exception as e:
    print(f"Warning: TkAgg backend initialization failed: {e}")

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging

# =============================================================================
# 2. CONFIGURATION & GLOBAL PARAMETERS
# =============================================================================
# Database connection string synced with shengic/sirirat credentials
DB_CONN_STR = "mysql+pymysql://shengic:sirirat@127.0.0.1:3306/doopler"

# Visual settings for the wind profile display
FIG_SIZE = (12, 6)
DPI = 150
MAX_HEIGHT_GATES = 60

# Wind Barb Styling
BARB_INTERVAL = 8        # Density of barbs on the X-axis (Time)
BARB_GATE_INTERVAL = 2   # Density of barbs on the Y-axis (Height)
BARB_LINEWIDTH = 1.2
BARB_LENGTH = 6.5
VMAX_SPEED = 25.0        # Max speed for color mapping scale

# =============================================================================
# 3. DATA ACQUISITION
# =============================================================================

def get_wind_data(start_date=None, end_date=None):
    """Retrieves processed VAD fit data from the MySQL database with optional date filtering"""
    try:
        engine = create_engine(DB_CONN_STR)
        
        # Base SQL query joins headers and gates
        sql_query = """
            SELECT 
                h.start_time,
                f.range_gate_index,
                COALESCE(h.range_gate_length_m, 30.0) as gate_len,
                f.u_ms,
                f.v_ms,
                f.speed_ms
            FROM vad_gate_fit f
            JOIN wind_profile_header h ON f.header_id = h.header_id
            WHERE f.status = 'ok'
              AND f.speed_ms IS NOT NULL
              AND f.speed_ms < 100
        """
        
        # Dynamically append date range filters if provided by the dashboard
        if start_date and end_date:
            sql_query += f" AND h.start_time BETWEEN '{start_date}' AND '{end_date}'"
        
        sql_query += " ORDER BY h.start_time, f.range_gate_index"
        
        logging.info("Executing database query for wind profile...")
        with engine.connect() as connection:
            dataframe = pd.read_sql(sql_query, connection)
            
        return dataframe
    except Exception as error:
        print(f"CRITICAL: Database read failure in plotter: {error}")
        return pd.DataFrame()
# =============================================================================
# 4. VISUALIZATION LOGIC
# =============================================================================

def generate_wind_chart(df):
    """Processes the dataframe and renders the Matplotlib figure"""
    if df.empty:
        print("Error: No valid wind data available for the requested period.")
        return

    # --- Data Preprocessing ---
    # Calculate Height Above Ground Level (AGL)
    df['height_m'] = (df['range_gate_index'] + 0.5) * df['gate_len']
    df = df[df['range_gate_index'] <= MAX_HEIGHT_GATES]

    # Reshape data into grids for mesh plotting
    pivot_speed = df.pivot_table(index='height_m', columns='start_time', values='speed_ms')
    pivot_u = df.pivot_table(index='height_m', columns='start_time', values='u_ms')
    pivot_v = df.pivot_table(index='height_m', columns='start_time', values='v_ms')
    
    times = pivot_speed.columns
    heights = pivot_speed.index
    # Create coordinate matrices
    X, Y = np.meshgrid(mdates.date2num(times), heights)

    # --- Plotting Setup ---
    fig, ax = plt.subplots(figsize=FIG_SIZE, dpi=DPI)
    color_map = plt.get_cmap('jet')
    color_norm = mcolors.Normalize(vmin=0, vmax=VMAX_SPEED)

    # --- Subsampling for Barb Clarity ---
    # Reduces visual clutter in the time-height series
    skip_t, skip_h = BARB_INTERVAL, BARB_GATE_INTERVAL
    
    u_vals = pivot_u.values[::skip_h, ::skip_t]
    v_vals = pivot_v.values[::skip_h, ::skip_t]
    s_vals = pivot_speed.values[::skip_h, ::skip_t]
    
    # Filter out NaN values to prevent plotting errors
    valid_mask = ~np.isnan(u_vals) & ~np.isnan(v_vals) & ~np.isnan(s_vals)
    
    X_plot = X[::skip_h, ::skip_t][valid_mask]
    Y_plot = Y[::skip_h, ::skip_t][valid_mask]
    u_plot = u_vals[valid_mask]
    v_plot = v_vals[valid_mask]
    s_plot = s_vals[valid_mask]

    # --- Rendering ---
    barb_colors = color_map(color_norm(s_plot))
    
    # Draw the wind barbs
    ax.barbs(X_plot, Y_plot, u_plot, v_plot, 
             color=barb_colors,
             length=BARB_LENGTH,
             linewidth=BARB_LINEWIDTH,
             pivot='middle',
             sizes=dict(emptybarb=0.0))

    # Add legend/colorbar
    scalar_mappable = cm.ScalarMappable(cmap=color_map, norm=color_norm)
    scalar_mappable.set_array([])
    plt.colorbar(scalar_mappable, ax=ax, label='Horizontal Wind Speed (m/s)')

    # --- Final Formatting ---
    ax.set_title(f'Doppler Lidar Wind Profile (VAD Retrieval)\nTime Range: {times[0]} to {times[-1]}')
    ax.set_ylabel('Height AGL (m)')
    ax.set_xlabel('Time (UTC)')
    ax.set_ylim(0, heights.max() + 50)
    
    ax.grid(True, which='major', linestyle='--', alpha=0.3)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    fig.autofmt_xdate()
    
    plt.tight_layout()
    
    # Save a physical file and then display the window
    plt.savefig("latest_wind_plot.png")
    print("Plot saved as 'latest_wind_plot.png'. Opening window...")
    plt.show(block=True)

# =============================================================================
# 5. DASHBOARD ENTRY POINT
# =============================================================================

def main(start_date=None, end_date=None):
    """
    This is the function called by the Dashboard.
    It does NOT need a new tk.Tk() because the Dashboard already has one.
    """
    try:
        # 1. Fetch data based on the dates passed from the Dashboard
        wind_df = get_wind_data(start_date=start_date, end_date=end_date) 
        
        if not wind_df.empty:
            # 2. Generate and show the Matplotlib window
            generate_wind_chart(wind_df)
        else:
            print(f"No data retrieved for range: {start_date} to {end_date}.")
            
    except Exception as main_err:
        print(f"An unexpected error occurred during plotting: {main_err}")

# Standalone execution logic
if __name__ == "__main__":
    # If you run this file directly, it calls main() without filters
    main()