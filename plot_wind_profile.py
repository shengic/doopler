# -*- coding: utf-8 -*-
# File: plot_wind_profile.py
# Purpose: Wind Profile Visualization Logic (Embedded in Dashboard & Standalone)
# Version: 6.2 (Dashboard Region 3 Integration + Agg/TkAgg Backend Management)

import matplotlib
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cm as cm
import matplotlib.colors as mcolors

# Use Agg backend by default for embedding logic.
# This prevents Matplotlib from trying to open a window automatically when called by the dashboard.
# Dashboard v3.5.4+ uses 'force_load_module' which relies on this non-interactive state.
matplotlib.use('Agg')

# =============================================================================
# 1. CONFIGURATION (Synchronized with doopler.sql schema)
# =============================================================================
DB_CONNECTION_STR = "mysql+pymysql://shengic:sirirat@127.0.0.1:3306/doopler"

# Visual Settings
FIG_SIZE = (12, 6)
DPI = 100 
MAX_HEIGHT_GATES = 60  # Only plot up to gate index 60 (approx 1.8km AGL)

# Visual Tweak Zone (Subsampling to prevent visual clutter)
BARB_INTERVAL = 8        # Density on X-axis (Time: plot 1 barb every 8 steps)
BARB_GATE_INTERVAL = 2   # Density on Y-axis (Height: plot 1 barb every 2 gates)
BARB_LINEWIDTH = 1.2
BARB_LENGTH = 6.5
VMAX_SPEED = 25.0        # Max speed for color mapping scale (m/s)

# =============================================================================
# 2. DATA ACQUISITION
# =============================================================================

def get_wind_data(start_date=None, end_date=None):
    """Retrieves processed VAD fit data from MySQL for the specified range."""
    try:
        engine = create_engine(DB_CONNECTION_STR)
        
        # Base query joining retrieval results with file headers
        sql = """
            SELECT 
                h.start_time, f.range_gate_index,
                COALESCE(h.range_gate_length_m, 30.0) as gate_len,
                f.u_ms, f.v_ms, f.speed_ms
            FROM vad_gate_fit f
            JOIN wind_profile_header h ON f.header_id = h.header_id
            WHERE f.status = 'ok' 
              AND f.speed_ms IS NOT NULL 
              AND f.speed_ms < 100
        """
        
        # Dashboard-driven date filtering
        if start_date and end_date:
            sql += f" AND h.start_time BETWEEN '{start_date}' AND '{end_date}'"
        
        sql += " ORDER BY h.start_time, f.range_gate_index"
        
        with engine.connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception as e:
        print(f"CRITICAL: DB Read Error in Plotter: {e}")
        return pd.DataFrame()

# =============================================================================
# 3. VISUALIZATION ENGINE (Dashboard Embedding Entry Point)
# =============================================================================

def create_wind_figure(df, start_date_str=None, end_date_str=None):
    """
    Constructs a Matplotlib Figure object specifically for dashboard embedding.
    Returns the Figure object to the caller (dooplerDashboard.py).
    """
    if df.empty:
        return None

    # --- A. Data Preprocessing ---
    # Calculate Height Above Ground Level (AGL) for the Y-axis
    df['height_m'] = (df['range_gate_index'] + 0.5) * df['gate_len']
    
    # Filter vertical range
    df = df[df['range_gate_index'] <= MAX_HEIGHT_GATES]

    # Reshape data into grids for mesh plotting
    pivot_speed = df.pivot_table(index='height_m', columns='start_time', values='speed_ms')
    pivot_u = df.pivot_table(index='height_m', columns='start_time', values='u_ms')
    pivot_v = df.pivot_table(index='height_m', columns='start_time', values='v_ms')
    
    times = pivot_speed.columns
    heights = pivot_speed.index
    
    # Create coordinate matrices (converting times to numeric for matplotlib)
    X, Y = np.meshgrid(mdates.date2num(times), heights)

    # --- B. Figure Construction ---
    fig, ax = plt.subplots(figsize=FIG_SIZE, dpi=DPI)
    cmap = plt.get_cmap('jet')
    norm = mcolors.Normalize(vmin=0, vmax=VMAX_SPEED)

    # --- C. Subsampling ---
    # Downsample the grid to ensure wind barbs do not overlap
    s_t, s_h = BARB_INTERVAL, BARB_GATE_INTERVAL
    u_vals = pivot_u.values[::s_h, ::s_t]
    v_vals = pivot_v.values[::s_h, ::s_t]
    s_vals = pivot_speed.values[::s_h, ::s_t]
    
    # Identify valid data points (exclude NaNs)
    valid_mask = ~np.isnan(u_vals) & ~np.isnan(v_vals) & ~np.isnan(s_vals)
    
    # --- D. Rendering ---
    # Draw Bold Colored Barbs
    ax.barbs(X[::s_h, ::s_t][valid_mask], Y[::s_h, ::s_t][valid_mask], 
             u_vals[valid_mask], v_vals[valid_mask], 
             color=cmap(norm(s_vals[valid_mask])),
             length=BARB_LENGTH, 
             linewidth=BARB_LINEWIDTH, 
             pivot='middle',
             sizes=dict(emptybarb=0.0))

    # --- E. Formatting & Colorbar ---
    sm = cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    plt.colorbar(sm, ax=ax, label='Horizontal Wind Speed (m/s)')

    # Labels and Titles
    title_date = times[0].strftime("%Y-%m-%d")
    ax.set_title(f'Lidar VAD Wind Profile: {title_date}\n{times[0].strftime("%H:%M")} to {times[-1].strftime("%H:%M")} (UTC)')
    ax.set_ylabel('Height AGL (m)')
    ax.set_xlabel('Time (UTC)')
    
    # Visual Guides
    ax.grid(True, which='major', color='gray', linestyle='--', alpha=0.3)
    
    # Axis Handling
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    fig.autofmt_xdate()
    
    plt.tight_layout()
    return fig

# =============================================================================
# 4. STANDALONE EXECUTION (Standalone Mode)
# =============================================================================

def main(start_date=None, end_date=None):
    """
    Entry point for standalone execution. 
    Switches backend to TkAgg to allow an interactive popup window.
    """
    try:
        # Re-enable interactive backend for local testing
        plt.switch_backend('TkAgg')
    except Exception as e:
        print(f"Warning: Could not switch to interactive backend: {e}")

    # Fetch all data if no dates provided
    df = get_wind_data(start_date, end_date)
    
    if not df.empty:
        print(f"Generating plot for {len(df)} data points...")
        fig = create_wind_figure(df)
        if fig:
            plt.show()
    else:
        print("No processed wind data found in the database.")

if __name__ == "__main__":
    main()