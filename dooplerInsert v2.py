#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# File: dooperInsert v2.py
# Version: 2.0.0 (GUI folder picker, chronological processing, header_id PK, logging)
#
# Purpose:
#   Open a folder chooser, then iterate files named "Wind_Profile_*.hpl" (non-recursive,
#   sorted by timestamp in filename) and insert for each file:
#     (A) one shared header row into table doopler.wind_profile_header
#     (B) N_ray × N_gate gate rows into table doopler.wind_profile_gate
#   using MySQL (host: localhost / user: shengic / pw: sirir / db: doopler).
#
# Assumptions:
#   - wind_profile_gate has AUTO_INCREMENT PK id, and
#     UNIQUE(header_id, ray_idx, range_gate_index) for idempotent gate upserts.
#   - wind_profile_header uses PRIMARY KEY header_id (NOT id).
#   - For de-dup across reruns, it's recommended to add UNIQUE(filename, start_time)
#     on wind_profile_header so the same HPL reuses the same header_id.
#   - Tables already exist in database doopler (no DDL here).
#
# Line 1–19 mapping (Name → MySQL field → TABLE → Example Value)
#   (Examples from Wind_Profile_254_20240131_183101.hpl; runtime values parsed from each file.)
#
#  1. Filename                         → filename                     → doopler.wind_profile_header → Wind_Profile_254_20240131_183101.hpl
#  2. System ID                        → system_id                    → doopler.wind_profile_header → 254
#  3. Number of gates                  → num_gates                    → doopler.wind_profile_header → 333
#  4. Range gate length (m)            → range_gate_length_m          → doopler.wind_profile_header → 18.0
#  5. Gate length (pts)                → gate_length_pts              → doopler.wind_profile_header → 12
#  6. Pulses/ray                       → pulses_per_ray               → doopler.wind_profile_header → 20000
#  7. No. of rays in file              → num_rays_in_file             → doopler.wind_profile_header → 6
#  8. Scan type                        → scan_type                    → doopler.wind_profile_header → Wind profile
#  9. Focus range                      → focus_range                  → doopler.wind_profile_header → 65535
# 10. Start time                       → start_time                   → doopler.wind_profile_header → 20240131 18:31:07.68
# 11. Resolution (m/s)                 → velocity_resolution_ms       → doopler.wind_profile_header → 0.0764
# 12. Range of measurement (center...) → range_center_formula         → doopler.wind_profile_header → (range_gate + 0.5) * gate_length
# 13. Data line 1 fields (definition)  → (not stored; parsed for reference)
# 14. Data line 1 format               → data_line1_format            → doopler.wind_profile_header → f9.6,1x,f6.2,1x,f6.2
# 15. Data line 2 fields (definition)  → (not stored; parsed for reference)
# 16. Data line 2 format               → data_line2_format            → doopler.wind_profile_header → i3,1x,f6.4,1x,f8.6,1x,e12.6,1x,f6.4 - repeat for no. gates
# 17. Instrument spectral width (m/s)  → instrument_spectral_width_ms → doopler.wind_profile_header → 0.458645
#
# 18. Data line 1 values (per ray) → stored per-row in doopler.wind_profile_gate:
#     Decimal time (hours) → decimal_time_hours → doopler.wind_profile_gate → e.g., 18.51851389
#     Azimuth (degrees)    → azimuth_deg        → doopler.wind_profile_gate → e.g., 0.00
#     Elevation (degrees)  → elevation_deg      → doopler.wind_profile_gate → e.g., 75.00
#     Pitch (degrees)      → pitch_deg          → doopler.wind_profile_gate → e.g., 0.10
#     Roll (degrees)       → roll_deg           → doopler.wind_profile_gate → e.g., 0.13
#
# 19. Data line 2 values (per gate) → stored per-row in doopler.wind_profile_gate:
#     Range Gate           → range_gate_index       → doopler.wind_profile_gate → e.g., 0
#     Doppler (m/s)        → doppler_ms             → doopler.wind_profile_gate → e.g., 19.8746
#     Intensity (SNR + 1)  → intensity_snr_plus1    → doopler.wind_profile_gate → e.g., 1.173442
#     Beta (m^-1 sr^-1)    → beta_m_inv_sr_inv      → doopler.wind_profile_gate → e.g., 9.765609E-6
#     Spectral Width (m/s) → spectral_width_ms      → doopler.wind_profile_gate → e.g., 0.0764
#
# Run:
#   "D:\\mypython\\.venv\\Scripts\\python.exe" -m pip install PyMySQL
#   "D:\\mypython\\.venv\\Scripts\\python.exe" "D:\\mypython\\doopler\\dooperInsert v2.py"
#

import logging
import re
from datetime import datetime
from typing import Dict, List, Tuple
from pathlib import Path

import pymysql

# Try GUI folder picker (falls back to console input if unavailable)
try:
    import tkinter as tk
    from tkinter import filedialog
except Exception:
    tk = None
    filedialog = None

# =========================
# Config (hardcoded DB)
# =========================
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "shengic"
DB_PASSWORD = "sirirat"
DB_NAME = "doopler"

LOG_FILE = "dooperInsert_v2.log"

# =========================
# Logging
# =========================
logger = logging.getLogger("dooperInsertV2")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
ch = logging.StreamHandler(); ch.setFormatter(_fmt)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8"); fh.setFormatter(_fmt)
logger.addHandler(ch); logger.addHandler(fh)

# =========================
# Parsing helpers
# =========================
HEADER_KEYS = [
    "Filename","System ID","Number of gates","Range gate length (m)","Gate length (pts)",
    "Pulses/ray","No. of rays in file","Scan type","Focus range","Start time","Resolution (m/s)",
]

def parse_start_time(s: str) -> datetime:
    m = re.match(r"^(\d{8})\s+(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?$", s.strip())
    if not m: raise ValueError(f"Unrecognized start time format: {s!r}")
    ymd, hh, mm, ss, frac = m.groups()
    usec = int(((frac or "0") + "000000")[:6])
    return datetime(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]), int(hh), int(mm), int(ss), usec)

def load_lines(hpl_path: str) -> List[str]:
    logger.info("Reading HPL: %s", hpl_path)
    with open(hpl_path, "r", errors="ignore") as f:
        lines = [ln.rstrip("\n") for ln in f]
    logger.info("Read %d lines.", len(lines))
    return lines

def extract_header(lines: List[str]) -> Tuple[Dict[str, str], int]:
    logger.info("Extracting header...")
    header: Dict[str, str] = {}
    for ln in lines[:200]:
        if ":" in ln:
            k, v = ln.split(":", 1); k, v = k.strip(), v.strip()
            if k in HEADER_KEYS or k in ["Data line 1","Data line 2"]:
                header[k] = v
        if "Range of measurement" in ln:
            header["Range of measurement"] = ln.split("=",1)[-1].strip() if "=" in ln else ln.strip()
    i1 = next((i for i, ln in enumerate(lines[:200]) if ln.strip().startswith("Data line 1")), None)
    i2 = next((i for i, ln in enumerate(lines[:240]) if ln.strip().startswith("Data line 2")), None)
    if i1 is not None and i1+1 < len(lines): header["Data line 1 format"] = lines[i1+1].strip()
    if i2 is not None and i2+1 < len(lines): header["Data line 2 format"] = lines[i2+1].strip()
    idx_sw = None
    for i, ln in enumerate(lines[:300]):
        if "Instrument spectral width" in ln:
            m = re.search(r"Instrument spectral width\s*=\s*([0-9.]+)", ln)
            if m: header["Instrument spectral width"] = m.group(1)
            idx_sw = i; break
    if idx_sw is None: raise RuntimeError("Cannot find 'Instrument spectral width' in header.")
    data_start = idx_sw + 1
    for k in ["Filename","System ID","Number of gates","No. of rays in file","Start time"]:
        if k not in header: raise RuntimeError(f"Missing header key: {k}")
    logger.info("Header OK. Rays=%s, Gates=%s", header["No. of rays in file"], header["Number of gates"])
    return header, data_start

def parse_data_blocks(lines: List[str], start_idx: int, num_rays: int, num_gates: int):
    idx = start_idx
    for ray in range(num_rays):
        parts = lines[idx].strip().split()
        if len(parts) < 5: raise RuntimeError(f"Ray {ray}: expected 5 values in DL1 at line[{idx+1}] got: {lines[idx]!r}")
        tdec, azi, ele, pit, rol = map(float, parts[:5])
        gates = []
        for g in range(num_gates):
            sp = lines[idx+1+g].strip().split()
            if len(sp) < 5: raise RuntimeError(f"Ray {ray}, gate {g}: bad DL2: {lines[idx+1+g]!r}")
            try: rg = int(float(sp[0]))
            except ValueError: rg = g
            gates.append((rg, float(sp[1]), float(sp[2]), float(sp[3]), float(sp[4])))
        logger.info("Parsed ray %d with %d gates", ray, len(gates))
        yield (ray, tdec, azi, ele, pit, rol, gates)
        idx += 1 + num_gates

def create_import_run(conn, folder_path, files_count):
    sql = "INSERT INTO `import_run` (folder_path, files_count) VALUES (%s, %s)"
    with conn.cursor() as cur:
        cur.execute(sql, (folder_path, files_count))
        import_id = cur.lastrowid
    conn.commit()
    logger.info("Import run created. import_id = %s", import_id)
    return import_id

# =========================
# DB helpers
# =========================

def get_connection():
    logger.info("Connecting: %s@%s:%s db=%s", DB_USER, DB_HOST, DB_PORT, DB_NAME)
    conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER,
                           password=DB_PASSWORD, database=DB_NAME,
                           charset="utf8mb4", autocommit=False,
                           cursorclass=pymysql.cursors.DictCursor)
    logger.info("Connected.")
    return conn

def table_exists(conn, t: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=%s", (t,))
        return cur.fetchone() is not None

def header_unique_exists(conn) -> bool:
    q = """
    SELECT 1
    FROM information_schema.statistics s
    WHERE s.table_schema = DATABASE()
      AND s.table_name = 'wind_profile_header'
      AND s.non_unique = 0
      AND s.index_name <> 'PRIMARY'
    GROUP BY s.index_name
    HAVING GROUP_CONCAT(s.column_name ORDER BY s.seq_in_index) = 'filename,start_time'
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchone() is not None

def upsert_header_and_get_header_id(conn, h: Dict[str, str], import_id: int) -> int:
    """
    將 HPL 檔頭資訊寫入 wind_profile_header 表，並回傳 header_id。
    已加入 import_id 以支援批次管理與級聯刪除。
    """
    # 1. 定義 SQL 語句
    # 使用 ON DUPLICATE KEY UPDATE 確保 ID 的唯一性與可追蹤性
    sql = """
    INSERT INTO `doopler`.`wind_profile_header`
      (import_id, filename, system_id, num_gates, range_gate_length_m, gate_length_pts, 
       pulses_per_ray, num_rays_in_file, scan_type, focus_range, start_time, 
       velocity_resolution_ms, range_center_formula, data_line1_format, 
       data_line2_format, instrument_spectral_width_ms)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE 
      import_id = VALUES(import_id), -- 更新為最後一次匯入的批次 ID
      header_id = LAST_INSERT_ID(header_id) -- [重要] 確保重複時回傳現有 ID
    """
    
    # 2. 準備參數 (依照 SQL 順序對應 HPL 欄位)
    params = (
        import_id,                            # 新增的批次管理 ID
        h["Filename"],                        # .hpl 檔名
        int(h["System ID"]),                  # 設備 ID
        int(h["Number of gates"]),            # 高度層數
        float(h["Range gate length (m)"]),    # 每一層的厚度 (公尺)
        int(h["Gate length (pts)"]),          # 點數設定
        int(h["Pulses/ray"]),                 # 每條光束脈衝數
        int(h["No. of rays in file"]),        # 一個掃描循環的射線數
        h["Scan type"],                       # 掃描模式 (如 Wind profile)
        int(h["Focus range"]),                # 焦距設定
        parse_start_time(h["Start time"]),    # 解析後的 datetime 物件
        float(h["Resolution (m/s)"]),         # 速度解析度
        h.get("Range of measurement"),        # 高度中心點公式
        h.get("Data line 1 format"),          # 數據格式定義 1
        h.get("Data line 2 format"),          # 數據格式定義 2
        float(h["Instrument spectral width"]) # 儀器頻譜寬度
    )
    
    # 3. 執行並回傳
    with conn.cursor() as cur:
        logger.info("Upserting header for file: %s", h["Filename"])
        cur.execute(sql, params)
        # 即使觸發了 UPDATE，LAST_INSERT_ID 配合上述 SQL 也能拿到正確的 PK
        header_id = cur.lastrowid
        logger.info("Header processed. header_id = %s", header_id)
        return header_id

def upsert_gate_rows(conn, header_id: int, ray_idx: int,
                     dl1_vals: Tuple[float,float,float,float,float],
                     gates: List[Tuple[int,float,float,float,float]]):
    tdec, azi, ele, pit, rol = dl1_vals
    sql = """
    INSERT INTO `doopler`.`wind_profile_gate`
      (header_id, ray_idx, range_gate_index, doppler_ms, intensity_snr_plus1,
       beta_m_inv_sr_inv, spectral_width_ms, decimal_time_hours, azimuth_deg,
       elevation_deg, pitch_deg, roll_deg)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      doppler_ms=VALUES(doppler_ms),
      intensity_snr_plus1=VALUES(intensity_snr_plus1),
      beta_m_inv_sr_inv=VALUES(beta_m_inv_sr_inv),
      spectral_width_ms=VALUES(spectral_width_ms),
      decimal_time_hours=VALUES(decimal_time_hours),
      azimuth_deg=VALUES(azimuth_deg),
      elevation_deg=VALUES(elevation_deg),
      pitch_deg=VALUES(pitch_deg),
      roll_deg=VALUES(roll_deg)
    """
    payload = [(header_id, ray_idx, rg, dop, inten, beta, sw, tdec, azi, ele, pit, rol)
               for (rg, dop, inten, beta, sw) in gates]
    with conn.cursor() as cur:
        affected = cur.executemany(sql, payload)
        logger.info("Ray %d upsert affected rows: %s", ray_idx, affected)

# =========================
# File + folder processing
# =========================

def select_folder() -> str:
    """Open a GUI folder picker (tkinter). If not available, fall back to console input."""
    if tk is None or filedialog is None:
        print("tkinter not available. Falling back to console input.")
        return input("Enter folder path containing Wind_Profile_*.hpl files: ").strip().strip('"')
    try:
        root = tk.Tk(); root.withdraw()
        try: root.attributes('-topmost', True)
        except Exception: pass
        folder = filedialog.askdirectory(title="Select folder containing Wind_Profile_*.hpl files")
        root.destroy()
        return folder
    except Exception:
        print("Folder dialog failed. Falling back to console input.")
        return input("Enter folder path containing Wind_Profile_*.hpl files: ").strip().strip('"')

def process_file(conn, path: Path) -> Tuple[int,int]:
    logger.info("=== Processing: %s ===", path.name)
    lines = load_lines(str(path))
    header, data_start = extract_header(lines)
    num_rays = int(header["No. of rays in file"])
    num_gates = int(header["Number of gates"])

    header_id = upsert_header_and_get_header_id(conn, header)
    total = 0
    for (ray, tdec, azi, ele, pit, rol, gates) in parse_data_blocks(lines, data_start, num_rays, num_gates):
        upsert_gate_rows(conn, header_id, ray, (tdec, azi, ele, pit, rol), gates)
        total += len(gates)
    conn.commit()
    logger.info("Committed file: %s | header_id=%s | gates=%s", path.name, header_id, total)
    return header_id, total

# =========================
# Main
# =========================

def main():
    try:
        folder = select_folder()
        if not folder:
            logger.error("No folder selected. Exiting.")
            return
        
        root = Path(folder)
        pattern = re.compile(r'^Wind_Profile_[0-9]+_([0-9]{8})_([0-9]{6})\.hpl$', re.IGNORECASE)
        candidates = [p for p in root.iterdir() if p.is_file() and pattern.match(p.name)]
        files = sorted(candidates, key=lambda p: datetime.strptime(''.join(pattern.match(p.name).groups()), '%Y%m%d%H%M%S'))
        
        if not files:
            logger.warning("No matching .hpl files found in %s", root)
            return

        conn = get_connection()
        try:
            for t in ("wind_profile_header", "wind_profile_gate", "import_run"):
                if not table_exists(conn, t):
                    logger.error("Table '%s' is missing. Please check your SQL schema.", t)
                    return

            # [Action] Create the batch ID for this session
            import_id = create_import_run(conn, folder, len(files))

            grand_total = 0
            processed_count = 0
            for p in files:
                try:
                    header_id, count = process_file(conn, p, import_id)
                    grand_total += count
                    processed_count += 1
                except Exception as e:
                    logger.exception("Error processing file %s: %s", p.name, e)

            logger.info("IMPORT COMPLETED.")
            logger.info("Batch ID: %d", import_id)
            logger.info("Files processed: %d/%d", processed_count, len(files))
            logger.info("Total gate rows inserted: %d", grand_total)

        finally:
            conn.close()
            logger.info("Database connection closed.")

    except Exception as e:
        logger.exception("A fatal error occurred: %s", e)

if __name__ == "__main__":
    main()
