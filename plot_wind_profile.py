# -*- coding: utf-8 -*-
# File: plot_wind_profile.py
# Purpose: Wind Profile Visualization Logic
# Version: 6.0 (Dashboard Embedding Optimized)

import matplotlib
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cm as cm
import matplotlib.colors as mcolors

# Use Agg backend for clean figure generation without window popups
# This is mandatory for embedding into the Tkinter dashboard
matplotlib.use('Agg')

# =============================================================================
# CONFIGURATION
# =============================================================================
DB_CONN_STR = "mysql+pymysql://shengic:sirirat@127.0.0.1:3306/doopler"

# Plotting Settings
FIG_SIZE = (12, 6)
DPI = 100 
MAX_HEIGHT_GATES = 60
BARB_INTERVAL = 8
BARB_GATE_INTERVAL = 2
VMAX_SPEED = 25.0 

def get_wind_data(start_date=None, end_date=None):
    """Retrieves processed VAD fit data from MySQL."""
    try:
        engine = create_engine(DB_CONN_STR)
        sql_query = """
            SELECT h.start_time, f.range_gate_index, 
                   COALESCE(h.range_gate_length_m, 30.0) as gate_len,
                   f.u_ms, f.v_ms, f.speed_ms
            FROM vad_gate_fit f
            JOIN wind_profile_header h ON f.header_id = h.header_id
            WHERE f.status = 'ok' 
              AND f.speed_ms IS NOT NULL
              AND f.speed_ms < 100
        """
        if start_date and end_date:
            sql_query += f" AND h.start_time BETWEEN '{start_date}' AND '{end_date}'"
        
        sql_query += " ORDER BY h.start_time, f.range_gate_index"
        
        with engine.connect() as connection:
            return pd.read_sql(sql_query, connection)
    except Exception as error:
        print(f"DB Read Error in Plotter: {error}")
        return pd.DataFrame()

def create_wind_figure(df, start_date_str=None, end_date_str=None):
    """
    Constructs a Matplotlib Figure object specifically for dashboard embedding.
    Does NOT call plt.show(). This is the attribute the Dashboard is looking for.
    """
    if df.empty:
        return None

    # 1. Preprocessing
    df['height_m'] = (df['range_gate_index'] + 0.5) * df['gate_len']
    df = df[df['range_gate_index'] <= MAX_HEIGHT_GATES]

    # 2. Creating grids
    pivot_speed = df.pivot_table(index='height_m', columns='start_time', values='speed_ms')
    pivot_u = df.pivot_table(index='height_m', columns='start_time', values='u_ms')
    pivot_v = df.pivot_table(index='height_m', columns='start_time', values='v_ms')
    
    times = pivot_speed.columns
    heights = pivot_speed.index
    X, Y = np.meshgrid(mdates.date2num(times), heights)

    # 3. Figure Construction
    fig, ax = plt.subplots(figsize=FIG_SIZE, dpi=DPI)
    color_map = plt.get_cmap('jet')
    color_norm = mcolors.Normalize(vmin=0, vmax=VMAX_SPEED)

    # 4. Subsampling for visual clarity
    s_t, s_h = BARB_INTERVAL, BARB_GATE_INTERVAL
    u_vals = pivot_u.values[::s_h, ::s_t]
    v_vals = pivot_v.values[::s_h, ::s_t]
    s_vals = pivot_speed.values[::s_h, ::s_t]
    
    # Filter out empty/NaN cells
    valid_mask = ~np.isnan(u_vals) & ~np.isnan(v_vals) & ~np.isnan(s_vals)
    
    X_plot = X[::s_h, ::s_t][valid_mask]
    Y_plot = Y[::s_h, ::s_t][valid_mask]
    u_plot = u_vals[valid_mask]
    v_plot = v_vals[valid_mask]
    s_plot = s_vals[valid_mask]

    # 5. Drawing Wind Barbs
    barb_colors = color_map(color_norm(s_plot))
    ax.barbs(X_plot, Y_plot, u_plot, v_plot, 
             color=barb_colors,
             length=6, 
             pivot='middle')

    # 6. Formatting
    sm = cm.ScalarMappable(norm=color_norm, cmap=color_map)
    sm.set_array([])
    plt.colorbar(sm, ax=ax, label='Horizontal Wind Speed (m/s)')
    
    ax.set_title(f'Lidar Wind Profile: {times[0]} to {times[-1]} (UTC)')
    ax.set_ylabel('Height AGL (m)')
    ax.set_xlabel('Time (UTC)')
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    fig.autofmt_xdate()
    plt.tight_layout()
    
    return fig

def main(start_date=None, end_date=None):
    """
    Fallback entry point for standalone execution. 
    Switches backend to TkAgg to pop up an interactive window.
    """
    plt.switch_backend('TkAgg') 
    df = get_wind_data(start_date, end_date)
    if not df.empty:
        fig = create_wind_figure(df, start_date, end_date)
        plt.show()
    else:
        print(f"No wind data found for period: {start_date} to {end_date}")

if __name__ == "__main__":
    main()