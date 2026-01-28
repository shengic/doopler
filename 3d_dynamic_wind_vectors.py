# -*- coding: utf-8 -*-
import pymysql
import pandas as pd
import plotly.graph_objects as go
import numpy as np
import os
import sys
import traceback
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ================= 版本資訊 =================
__version__ = "5.2"
# 專業更新 (Professional Update)：
# 1. 彩色向量渲染 (Chromatic Vector Rendering)：將線條顏色與風速大小 (Magnitude) 綁定。
#    使用 Numpy 陣列擴展技術 (Array Broadcasting/Repeat)，將單一風速值映射至構成箭頭的 15 個幾何頂點上。
# 2. 視覺一致性 (Visual Consistency)：向量顏色現在直接對應右側的 Color Ribbon，強風顯示暖色，弱風顯示冷色。
# 3. 幾何引擎維持 (Geometry Engine)：保持 v5.1 的線框結構與粗細控制功能。

# ================= 1. 資料庫設定 =================
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "shengic",
    "password": "sirirat",
    "database": "doopler",
    "port": 3306
}

def get_db_engine():
    """建立 SQLAlchemy 引擎"""
    try:
        pwd = quote_plus(DB_CONFIG['password'])
        db_url = f"mysql+pymysql://{DB_CONFIG['user']}:{pwd}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        return create_engine(db_url)
    except Exception as e:
        print(f"[Critical Error] 資料庫連線設定失敗: {e}")
        traceback.print_exc()
        sys.exit(1)

def select_target_date():
    """查詢資料庫中可用的日期清單"""
    engine = get_db_engine()
    sql = """
    SELECT 
        DATE(start_time) as dt, 
        COUNT(*) as cnt,
        MIN(start_time) as t_start,
        MAX(start_time) as t_end
    FROM wind_profile_header 
    GROUP BY dt 
    ORDER BY dt DESC 
    LIMIT 15
    """
    try:
        print(f"[-] 正在連接資料庫讀取日期清單...")
        with engine.connect() as conn:
            result = conn.execute(text(sql)).fetchall()
            
        if not result:
            print("[Error] 資料庫中找不到任何 wind_profile_header 紀錄。")
            sys.exit(1)

        print("\n=== 可用觀測日期清單 ===")
        print(f"{'ID':<4} {'日期':<12} {'紀錄數':<8} {'時間範圍'}")
        print("-" * 60)
        
        date_options = {}
        for idx, row in enumerate(result, 1):
            dt_str = row[0].strftime('%Y-%m-%d')
            t_s = row[2].strftime('%H:%M') if row[2] else "--:--"
            t_e = row[3].strftime('%H:%M') if row[3] else "--:--"
            t_range = f"{t_s} - {t_e}"
            
            date_options[str(idx)] = dt_str
            print(f"{idx:<4} {dt_str:<12} {row[1]:<8} {t_range} UTC")
        
        print("-" * 60)
        user_input = input("請輸入 ID (1-15) 或日期 (YYYY-MM-DD): ").strip()
        return date_options.get(user_input, user_input)

    except Exception as e:
        print(f"[Error] 讀取日期清單時發生錯誤:")
        traceback.print_exc()
        sys.exit(1)

def get_wind_data(target_date):
    """撈取指定日期的 VAD 數據"""
    engine = get_db_engine()
    sql = """
    SELECT 
        h.start_time,
        f.range_gate_index,
        (f.range_gate_index + 0.5) * COALESCE(h.range_gate_length_m, 30.0) as height_agl,
        f.u_ms, f.v_ms, f.w_ms, f.speed_ms, f.speed_total_ms
    FROM vad_gate_fit f
    JOIN wind_profile_header h ON f.header_id = h.header_id
    WHERE DATE(h.start_time) = %s 
      AND f.status = 'ok'
    ORDER BY h.start_time, f.range_gate_index
    """
    try:
        print(f"[-] 正在讀取 {target_date} 的數據 (含 speed_total_ms)...")
        df = pd.read_sql(sql, engine, params=(target_date,))
        print(f"[-] 成功讀取 {len(df)} 筆原始數據。")
        return df
    except Exception as e:
        print(f"[Critical Error] 資料庫查詢失敗。")
        traceback.print_exc()
        sys.exit(1)

def calculate_wireframe_arrows(x_start, y_start, z_start, u, v, w, scale_factor):
    """
    核心幾何引擎：計算線框箭頭的座標
    """
    n = len(x_start)
    
    # 1. 計算終點 (Tip)
    x_end = x_start + u * scale_factor
    y_end = y_start + v * scale_factor
    z_end = z_start + w * scale_factor
    
    # 2. 計算向量長度與歸一化方向
    vec_len = np.sqrt(u**2 + v**2 + w**2) * scale_factor
    vec_len[vec_len == 0] = 1e-9
    ux, uy, uz = u * scale_factor / vec_len, v * scale_factor / vec_len, w * scale_factor / vec_len
    
    # 3. 計算箭頭參數
    HEAD_RATIO = 0.2  # 箭頭佔比
    HEAD_WIDTH_RATIO = 0.08 # 箭頭張開寬度
    
    head_len = vec_len * HEAD_RATIO
    head_width = vec_len * HEAD_WIDTH_RATIO
    
    # 4. 建立正交基底
    ref_x = np.zeros_like(ux)
    ref_y = np.zeros_like(uy)
    ref_z = np.ones_like(uz)
    
    mask_z = np.abs(uz) > 0.9
    ref_x[mask_z] = 1.0
    ref_z[mask_z] = 0.0
    
    ox = uy * ref_z - uz * ref_y
    oy = uz * ref_x - ux * ref_z
    oz = ux * ref_y - uy * ref_x
    
    o_len = np.sqrt(ox**2 + oy**2 + oz**2)
    o_len[o_len == 0] = 1.0
    ox, oy, oz = ox/o_len, oy/o_len, oz/o_len
    
    ox2 = uy * oz - uz * oy
    oy2 = uz * ox - ux * oz
    oz2 = ux * oy - uy * ox
    
    # 5. 計算箭頭幾何點
    bx = x_end - ux * head_len
    by = y_end - uy * head_len
    bz = z_end - uz * head_len
    
    # 十字箭頭 (Cross Wings)
    w1x, w1y, w1z = bx + ox * head_width, by + oy * head_width, bz + oz * head_width
    w2x, w2y, w2z = bx - ox * head_width, by - oy * head_width, bz - oz * head_width
    w3x, w3y, w3z = bx + ox2 * head_width, by + oy2 * head_width, bz + oz2 * head_width
    w4x, w4y, w4z = bx - ox2 * head_width, by - oy2 * head_width, bz - oz2 * head_width

    # 6. 堆疊座標 (包含 NaN 用於斷線)
    nan = np.full(n, np.nan)
    
    # 這裡的順序很重要：我們每一根箭頭都由 5 個部分組成
    # 每一個部分都包含 (起點, 終點, NaN)
    # 所以每一根箭頭佔用 15 個點
    
    x_all = np.column_stack((x_start, x_end, nan, x_end, w1x, nan, x_end, w2x, nan, x_end, w3x, nan, x_end, w4x, nan)).flatten()
    y_all = np.column_stack((y_start, y_end, nan, y_end, w1y, nan, y_end, w2y, nan, y_end, w3y, nan, y_end, w4y, nan)).flatten()
    z_all = np.column_stack((z_start, z_end, nan, z_end, w1z, nan, z_end, w2z, nan, z_end, w3z, nan, z_end, w4z, nan)).flatten()
                             
    return x_all, y_all, z_all

def generate_dynamic_plot(df, target_date):
    if df.empty:
        print("[Warning] DataFrame 為空，無法繪圖。")
        return

    try:
        print("[-] 正在進行數據前處理...")
        
        if 'speed_total_ms' not in df.columns:
            raise ValueError("資料庫缺少 'speed_total_ms' 欄位。")

        # 資料前處理
        numeric_cols = ['u_ms', 'v_ms', 'w_ms', 'height_agl', 'speed_total_ms']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        initial_count = len(df)
        df.dropna(subset=['height_agl', 'speed_total_ms'], inplace=True)
        df = df[df['speed_total_ms'] < 150].copy() 
        
        if len(df) < initial_count:
            print(f"[-] 已過濾 {initial_count - len(df)} 筆無效數據。")

        df['start_time'] = pd.to_datetime(df['start_time'])
        df['time_str'] = df['start_time'].dt.strftime('%H:%M:%S')
        timestamps = df['time_str'].unique()

        if len(timestamps) == 0:
            print("[Error] 無有效時間戳記。")
            return

        # ================= Z 軸歸一化 =================
        max_h_real = df['height_agl'].max()
        if pd.isna(max_h_real) or max_h_real == 0: max_h_real = 1000.0
        
        Z_VISUAL_MAX = 6.0
        df['z_plot'] = (df['height_agl'] / max_h_real) * Z_VISUAL_MAX
        df['pos_x'] = 0.0
        df['pos_y'] = 0.0

        # ================= 視覺縮放 =================
        max_speed = df['speed_total_ms'].max()
        if pd.isna(max_speed) or max_speed == 0: max_speed = 10.0
        
        VISUAL_SCALE = 1.0 / max_speed 
        print(f"[-] Max Speed: {max_speed:.1f} m/s (mapped to 1.0 unit)")

        # 顏色範圍設定
        cmax = df['speed_total_ms'].quantile(0.98)
        if pd.isna(cmax): cmax = 10
        cmin = 0

        # 數據導出
        script_dir = os.path.dirname(os.path.abspath(__file__))
        txt_path = os.path.join(script_dir, "wind_vector_3d.txt")
        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"Export Date: {target_date}\n")
                f.write(f"Note: Wireframe Engine v5.2 (Colored).\n")
                export_cols = ['time_str', 'height_agl', 'z_plot', 'u_ms', 'v_ms', 'w_ms', 'speed_total_ms']
                f.write(df[[c for c in export_cols if c in df.columns]].to_string(index=False))
        except: pass

        # ================= 建立動畫幀 =================
        print(f"[-] 正在生成彩色線框向量動畫幀 ({len(timestamps)} steps)...")
        frames = []
        
        DEFAULT_LINE_WIDTH = 6

        for t in timestamps:
            df_t = df[df['time_str'] == t]
            if df_t.empty: continue
            
            # 計算幾何座標
            lx, ly, lz = calculate_wireframe_arrows(
                df_t['pos_x'].values, 
                df_t['pos_y'].values, 
                df_t['z_plot'].values,
                df_t['u_ms'].values,
                df_t['v_ms'].values,
                df_t['w_ms'].values,
                VISUAL_SCALE
            )
            
            # [KEY LOGIC] 顏色矩陣擴展
            # 我們有 N 個向量，每個向量由 15 個點組成 (包含 NaN)
            # 我們需要一個長度為 N*15 的顏色陣列
            speeds = df_t['speed_total_ms'].values
            line_colors = np.repeat(speeds, 15)
            
            frames.append(go.Frame(
                data=[
                    # Trace 0: Wireframe Vectors (Lines) - 這裡設定顏色
                    go.Scatter3d(
                        x=lx, y=ly, z=lz,
                        mode='lines',
                        line=dict(
                            color=line_colors, # 使用擴展後的顏色陣列
                            width=DEFAULT_LINE_WIDTH,
                            colorscale='Jet',
                            cmin=cmin, 
                            cmax=cmax,
                            showscale=True, # 在線條上顯示 Colorbar
                            colorbar=dict(
                                title=dict(text='Speed (m/s)', font=dict(family="Georgia")),
                                tickfont=dict(family="Georgia"),
                                len=0.8
                            )
                        ), 
                        name='Vector',
                        hoverinfo='skip'
                    ),
                    # Trace 1: Markers (僅用於 Hover 顯示數值，不顯示顏色條)
                    go.Scatter3d(
                        x=df_t['pos_x'], y=df_t['pos_y'], z=df_t['z_plot'],
                        mode='markers',
                        marker=dict(
                            size=3, 
                            color=df_t['speed_total_ms'], 
                            colorscale='Jet', 
                            cmin=cmin, cmax=cmax,
                            showscale=False # 避免重複顯示 Colorbar
                        ),
                        text=df_t.apply(lambda r: f"Total: {r['speed_total_ms']:.1f} m/s<br>H: {r['height_agl']:.0f} m", axis=1),
                        hoverinfo='text'
                    )
                ],
                name=str(t)
            ))

        # ================= 建立基礎圖表 =================
        if frames:
            frame0_data = frames[0].data
        else:
            frame0_data = []

        fig = go.Figure(data=frame0_data, frames=frames)

        # ================= Z 軸標籤還原 =================
        tick_vals = np.linspace(0, Z_VISUAL_MAX, 7) 
        tick_text = np.linspace(0, max_h_real, 7)   
        tick_text_str = [f"{int(v)}m" for v in tick_text]

        fig.update_layout(
            title=dict(text=f"3D Dynamic Wind Vectors (Colored): {target_date} (v{__version__})", 
                       x=0.5, font=dict(family="Georgia", size=22)),
            scene=dict(
                xaxis=dict(title=dict(text='X (Loc)', font=dict(family="Georgia")), range=[-1.2, 1.2]), 
                yaxis=dict(title=dict(text='Y (Loc)', font=dict(family="Georgia")), range=[-1.2, 1.2]), 
                zaxis=dict(
                    title=dict(text='Height AGL', font=dict(family="Georgia")),
                    range=[0, Z_VISUAL_MAX * 1.1],
                    tickmode='array', tickvals=tick_vals, ticktext=tick_text_str
                ),
                aspectmode='manual',
                aspectratio=dict(x=1, y=1, z=3.0),
                camera=dict(eye=dict(x=1.6, y=1.6, z=0.5))
            ),
            sliders=[
                # Slider 1: Time
                dict(
                    active=0, currentvalue={"prefix": "Time: ", "font": {"family": "Georgia"}},
                    pad={"t": 50},
                    steps=[dict(method='animate', label=t, 
                                args=[[t], dict(mode='immediate', frame=dict(duration=0, redraw=True))]) 
                           for t in timestamps]
                ),
                # Slider 2: Line Thickness
                dict(
                    active=2,
                    currentvalue={"prefix": "Thickness: ", "font": {"family": "Georgia"}},
                    pad={"t": 100},
                    steps=[
                        dict(method="restyle", label="Fine (2px)", args=["line.width", 2]),
                        dict(method="restyle", label="Medium (4px)", args=["line.width", 4]),
                        dict(method="restyle", label="Bold (6px)", args=["line.width", 6]),
                        dict(method="restyle", label="Thick (8px)", args=["line.width", 8]),
                        dict(method="restyle", label="Heavy (10px)", args=["line.width", 10]),
                        dict(method="restyle", label="Extra (15px)", args=["line.width", 15]),
                    ]
                )
            ],
            updatemenus=[dict(
                type="buttons", x=0.05, y=0,
                buttons=[
                    dict(label="▶ Play", method="animate", args=[None, dict(frame=dict(duration=150, redraw=True), fromcurrent=True)]),
                    dict(label="|| Pause", method="animate", args=[[None], dict(mode="immediate", frame=dict(duration=0, redraw=False))])
                ]
            )]
        )

        script_dir = os.path.dirname(os.path.abspath(__file__))
        html_path = os.path.join(script_dir, "wind_vector_3d.html")
        fig.write_html(html_path)
        print(f"[Success] 檔案已輸出: {html_path}")

    except Exception as e:
        print(f"[Critical Error] 繪圖過程失敗: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    print(f"=== 3D 風場向量分析 v{__version__} ===")
    selected_date = select_target_date()
    data = get_wind_data(selected_date)
    generate_dynamic_plot(data, selected_date)