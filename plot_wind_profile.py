# -*- coding: utf-8 -*-
# File: plot_wind_profile.py
# Purpose: Wind Profile Visualization Logic (Dual Mode: Horizontal & Total)
# Version: 6.5.0 (Georgia Typography + Dual Speed Support + Main Section)

import matplotlib
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cm as cm
import matplotlib.colors as mcolors

# Default to Agg for dashboard integration; main() will override this for local testing.
matplotlib.use('Agg')

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
DB_CONNECTION_STR = "mysql+pymysql://shengic:sirirat@127.0.0.1:3306/doopler"

# Visual Settings
FIG_SIZE = (12, 6)
DPI = 100 
MAX_HEIGHT_GATES = 60 
VMAX_SPEED = 25.0

# Subsampling for visual clarity
BARB_INTERVAL = 8        
BARB_GATE_INTERVAL = 2   
BARB_LINEWIDTH = 1.2
BARB_LENGTH = 6.5

# Typography Configuration
FONT_FAMILY = 'Georgia'

# =============================================================================
# 2. DATA ACQUISITION
# =============================================================================

def get_wind_data(start_date=None, end_date=None):
    """取得 VAD 計算結果，包含水平 (speed_ms) 與三維總風速 (speed_total_ms)。"""
    try:
        engine = create_engine(DB_CONNECTION_STR)
        sql = """
            SELECT 
                h.start_time, f.range_gate_index,
                COALESCE(h.range_gate_length_m, 30.0) as gate_len,
                f.u_ms, f.v_ms, f.speed_ms, f.speed_total_ms
            FROM vad_gate_fit f
            JOIN wind_profile_header h ON f.header_id = h.header_id
            WHERE f.status = 'ok' 
              AND f.speed_ms IS NOT NULL 
              AND f.speed_ms < 100
        """
        if start_date and end_date:
            sql += f" AND h.start_time BETWEEN '{start_date}' AND '{end_date}'"
        
        sql += " ORDER BY h.start_time, f.range_gate_index"
        
        with engine.connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception as e:
        print(f"CRITICAL: DB Read Error: {e}")
        return pd.DataFrame()

# =============================================================================
# 3. VISUALIZATION ENGINE
# =============================================================================

def create_wind_figure(df, start_date_str=None, end_date_str=None, plot_type='horizontal'):
    """
    建立風場垂直剖面圖。
    plot_type: 'horizontal' (使用 speed_ms) 或 'total' (使用 speed_total_ms)
    """
    if df.empty:
        return None

    # 設定全域字體為 Georgia
    plt.rcParams['font.family'] = FONT_FAMILY

    # 計算高度 AGL
    df = df.copy()
    df['height_m'] = (df['range_gate_index'] + 0.5) * df['gate_len']
    df = df[df['range_gate_index'] <= MAX_HEIGHT_GATES]

    # 切換顯示欄位與標籤
    speed_col = 'speed_ms' if plot_type == 'horizontal' else 'speed_total_ms'
    label_text = 'Horizontal Wind Speed' if plot_type == 'horizontal' else 'Total Wind Speed'

    # 安全機制：若資料庫尚未新增總風速欄位則自動回退
    if speed_col not in df.columns:
        speed_col = 'speed_ms'
        label_text = 'Horizontal Wind Speed (Fallback)'

    # 建立數據網格
    pivot_speed = df.pivot_table(index='height_m', columns='start_time', values=speed_col)
    pivot_u = df.pivot_table(index='height_m', columns='start_time', values='u_ms')
    pivot_v = df.pivot_table(index='height_m', columns='start_time', values='v_ms')
    
    if pivot_speed.empty: return None

    times = pivot_speed.columns
    heights = pivot_speed.index
    X, Y = np.meshgrid(mdates.date2num(times), heights)

    fig, ax = plt.subplots(figsize=FIG_SIZE, dpi=DPI)
    cmap = plt.get_cmap('jet')
    norm = mcolors.Normalize(vmin=0, vmax=VMAX_SPEED)

    # 繪製風標 (Barbs)
    s_t, s_h = BARB_INTERVAL, BARB_GATE_INTERVAL
    u_vals = pivot_u.values[::s_h, ::s_t]
    v_vals = pivot_v.values[::s_h, ::s_t]
    s_vals = pivot_speed.values[::s_h, ::s_t]
    
    valid_mask = ~np.isnan(u_vals) & ~np.isnan(v_vals) & ~np.isnan(s_vals)
    
    ax.barbs(X[::s_h, ::s_t][valid_mask], Y[::s_h, ::s_t][valid_mask], 
             u_vals[valid_mask], v_vals[valid_mask], 
             color=cmap(norm(s_vals[valid_mask])),
             length=BARB_LENGTH, linewidth=BARB_LINEWIDTH, pivot='middle')

    # 設定 Colorbar
    sm = cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax)
    cbar.set_label(f'{label_text} (m/s)', family=FONT_FAMILY)

    # 標題與座標軸
    title_date = times[0].strftime("%Y-%m-%d")
    ax.set_title(f'Doppler Lidar Wind Profile: {title_date}\n({label_text})', fontname=FONT_FAMILY, fontsize=12)
    ax.set_ylabel('Height AGL (m)', fontname=FONT_FAMILY)
    ax.set_xlabel('Time (UTC)', fontname=FONT_FAMILY)
    
    ax.grid(True, which='major', color='gray', linestyle='--', alpha=0.3)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    fig.autofmt_xdate()
    
    plt.tight_layout()
    return fig

# =============================================================================
# 4. MAIN EXECUTION BLOCK (Standalone Mode)
# =============================================================================

def main():
    """
    本地測試入口。
    自動檢測資料庫最新觀測日期，並產生水平與三維風速對比圖。
    """
    print(f"Initializing Standalone Plotter (Font: {FONT_FAMILY})...")
    
    try:
        plt.switch_backend('TkAgg')
    except Exception as e:
        print(f"Warning: Could not switch to interactive backend: {e}")

    # 1. 取得資料
    full_df = get_wind_data()
    if full_df.empty:
        print("No processed wind data found in database.")
        return

    # 2. 抓取最新日期
    full_df['start_time'] = pd.to_datetime(full_df['start_time'])
    latest_date = full_df['start_time'].dt.date.max()
    print(f"Targeting Latest Observation: {latest_date}")
    
    date_df = full_df[full_df['start_time'].dt.date == latest_date]

    # 3. 循序產生兩張圖表
    for p_type in ['horizontal', 'total']:
        print(f"Rendering {p_type} plot...")
        fig = create_wind_figure(date_df, plot_type=p_type)
        if fig:
            plt.show()

if __name__ == "__main__":
    main()