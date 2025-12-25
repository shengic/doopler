#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: dooplerInsert_v3.py
Version: 3.2.0
Features: 
1. Single-Line Console Refresh: Uses carriage returns (\r) to update progress on one line.
2. Batch Management: Creates an entry in 'import_run' for every session.
3. Cascading Deletion: Deleting an import_id removes all associated headers and gates.
4. Chronological Processing: Files sorted by filename timestamp.
5. GUI Folder Picker: Easy selection of data directories.
"""

import logging
import re
import sys
import pymysql
from datetime import datetime
from typing import Dict, List, Tuple
from pathlib import Path

# Try GUI folder picker (falls back to console input if unavailable)
try:
    import tkinter as tk
    from tkinter import filedialog
except Exception:
    tk = None
    filedialog = None

# =========================
# Configuration
# =========================
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "shengic"
DB_PASSWORD = "sirirat"
DB_NAME = "doopler"

LOG_FILE = "dooplerInsert_v3.log"

# =========================
# Logging Setup
# =========================
logger = logging.getLogger("dooplerInsertV3")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

# File Handler: Keeps full sequential history
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(_fmt)
logger.addHandler(fh)

# We manually handle console output to achieve the "Single-Line Refresh" effect.
def print_progress(msg: str):
    """Refreshes the current console line."""
    # ljust ensures we clear any characters from longer previous lines
    sys.stdout.write(f"\r{msg.ljust(110)}")
    sys.stdout.flush()

# =========================
# Parsing Helpers
# =========================
HEADER_KEYS = [
    "Filename", "System ID", "Number of gates", "Range gate length (m)", "Gate length (pts)",
    "Pulses/ray", "No. of rays in file", "Scan type", "Focus range", "Start time", "Resolution (m/s)",
]

def parse_start_time(s: str) -> datetime:
    m = re.match(r"^(\d{8})\s+(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?$", s.strip())
    if not m: raise ValueError(f"Unrecognized start time format: {s!r}")
    ymd, hh, mm, ss, frac = m.groups()
    usec = int(((frac or "0") + "000000")[:6])
    return datetime(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]), int(hh), int(mm), int(ss), usec)

def load_lines(hpl_path: str) -> List[str]:
    # We log file reads to the log file, but not the console for clean display
    logger.info("Reading HPL file: %s", hpl_path)
    with open(hpl_path, "r", errors="ignore") as f:
        lines = [ln.rstrip("\n") for ln in f]
    return lines

def extract_header(lines: List[str]) -> Tuple[Dict[str, str], int]:
    header: Dict[str, str] = {}
    for ln in lines[:200]:
        if ":" in ln:
            k, v = ln.split(":", 1); k, v = k.strip(), v.strip()
            if k in HEADER_KEYS or k in ["Data line 1", "Data line 2"]:
                header[k] = v
        if "Range of measurement" in ln:
            header["Range of measurement"] = ln.split("=", 1)[-1].strip() if "=" in ln else ln.strip()
    
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
    if idx_sw is None: raise RuntimeError("Instrument spectral width not found in header.")
    return header, idx_sw + 1

def parse_data_blocks(lines: List[str], start_idx: int, num_rays: int, num_gates: int):
    idx = start_idx
    for ray in range(num_rays):
        parts = lines[idx].strip().split()
        if len(parts) < 5: raise RuntimeError(f"Ray {ray}: Bad Data line 1 at index {idx}")
        tdec, azi, ele, pit, rol = map(float, parts[:5])
        gates = []
        for g in range(num_gates):
            sp = lines[idx+1+g].strip().split()
            if len(sp) < 5: raise RuntimeError(f"Ray {ray}, Gate {g}: Bad Data line 2")
            try: rg = int(float(sp[0]))
            except ValueError: rg = g
            gates.append((rg, float(sp[1]), float(sp[2]), float(sp[3]), float(sp[4])))
        yield (ray, tdec, azi, ele, pit, rol, gates)
        idx += 1 + num_gates

# =========================
# Database Functions
# =========================

def get_connection():
    return pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER,
                           password=DB_PASSWORD, database=DB_NAME,
                           charset="utf8mb4", autocommit=False,
                           cursorclass=pymysql.cursors.DictCursor)

def table_exists(conn, t: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=%s", (t,))
        return cur.fetchone() is not None

def create_import_run(conn, folder_path: str, files_count: int) -> int:
    """Create a batch entry and return the import_id"""
    sql = "INSERT INTO `import_run` (folder_path, files_count) VALUES (%s, %s)"
    with conn.cursor() as cur:
        cur.execute(sql, (folder_path, files_count))
        import_id = cur.lastrowid
    conn.commit()  # Commit immediately so foreign keys can reference it
    return import_id

def upsert_header_and_get_header_id(conn, h: Dict[str, str], import_id: int) -> int:
    """Insert or update header with association to import_id"""
    sql = """
    INSERT INTO `doopler`.`wind_profile_header`
      (import_id, filename, system_id, num_gates, range_gate_length_m, gate_length_pts, 
       pulses_per_ray, num_rays_in_file, scan_type, focus_range, start_time, 
       velocity_resolution_ms, range_center_formula, data_line1_format, 
       data_line2_format, instrument_spectral_width_ms)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE 
        import_id = VALUES(import_id),
        header_id = LAST_INSERT_ID(header_id)
    """
    params = (
        import_id, h["Filename"], int(h["System ID"]), int(h["Number of gates"]),
        float(h["Range gate length (m)"]), int(h["Gate length (pts)"]),
        int(h["Pulses/ray"]), int(h["No. of rays in file"]), h["Scan type"],
        int(h["Focus range"]), parse_start_time(h["Start time"]),
        float(h["Resolution (m/s)"]), h.get("Range of measurement"),
        h.get("Data line 1 format"), h.get("Data line 2 format"),
        float(h["Instrument spectral width"])
    )
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.lastrowid

def upsert_gate_rows(conn, header_id: int, ray_idx: int, dl1: tuple, gates: list):
    tdec, azi, ele, pit, rol = dl1
    sql = """
    INSERT INTO `doopler`.`wind_profile_gate`
      (header_id, ray_idx, range_gate_index, doppler_ms, intensity_snr_plus1,
       beta_m_inv_sr_inv, spectral_width_ms, decimal_time_hours, azimuth_deg,
       elevation_deg, pitch_deg, roll_deg)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE 
        doppler_ms=VALUES(doppler_ms), intensity_snr_plus1=VALUES(intensity_snr_plus1),
        beta_m_inv_sr_inv=VALUES(beta_m_inv_sr_inv), spectral_width_ms=VALUES(spectral_width_ms)
    """
    payload = [(header_id, ray_idx, rg, dop, inten, beta, sw, tdec, azi, ele, pit, rol)
               for (rg, dop, inten, beta, sw) in gates]
    with conn.cursor() as cur:
        cur.executemany(sql, payload)

# =========================
# Execution Flow
# =========================

def process_file(conn, path: Path, import_id: int, idx: int, total: int) -> int:
    # Single-line refresh for console
    print_progress(f"[{idx}/{total}] Processing: {path.name}...")
    
    lines = load_lines(str(path))
    header, data_start = extract_header(lines)
    num_rays = int(header["No. of rays in file"])
    num_gates = int(header["Number of gates"])

    header_id = upsert_header_and_get_header_id(conn, header, import_id)
    file_gates = 0
    for (ray, tdec, azi, ele, pit, rol, gates) in parse_data_blocks(lines, data_start, num_rays, num_gates):
        upsert_gate_rows(conn, header_id, ray, (tdec, azi, ele, pit, rol), gates)
        file_gates += len(gates)
    
    conn.commit()
    
    # Second update for console once success
    print_progress(f"[{idx}/{total}] Success: {path.name} ({file_gates} gates)")
    logger.info("Successfully processed file: %s (Total Gates: %d)", path.name, file_gates)
    return file_gates

def select_folder() -> str:
    if tk is None: return input("Enter folder path: ").strip().strip('"')
    root = tk.Tk(); root.withdraw(); root.attributes('-topmost', True)
    folder = filedialog.askdirectory(title="Select folder with .hpl files")
    root.destroy()
    return folder

def main():
    try:
        folder = select_folder()
        if not folder:
            print("No folder selected. Exiting.")
            return
        
        root = Path(folder)
        pattern = re.compile(r'^Wind_Profile_[0-9]+_([0-9]{8})_([0-9]{6})\.hpl$', re.IGNORECASE)
        candidates = [p for p in root.iterdir() if p.is_file() and pattern.match(p.name)]
        files = sorted(candidates, key=lambda p: datetime.strptime(''.join(pattern.match(p.name).groups()), '%Y%m%d%H%M%S'))
        
        if not files:
            print(f"No matching .hpl files found in {root}")
            return

        conn = get_connection()
        try:
            for t in ("wind_profile_header", "wind_profile_gate", "import_run"):
                if not table_exists(conn, t):
                    print(f"Error: Table '{t}' is missing. Check your SQL schema.")
                    return

            # Initialize Batch Session
            import_id = create_import_run(conn, folder, len(files))
            print(f"Session Started | Import ID: {import_id}\n")

            grand_total = 0
            processed_count = 0
            for i, p in enumerate(files, 1):
                try:
                    count = process_file(conn, p, import_id, i, len(files))
                    grand_total += count
                    processed_count += 1
                except Exception as e:
                    print(f"\n[Error] Failed processing {p.name}: {e}")
                    logger.exception("Error processing file %s: %s", p.name, e)

            # Final summary after loops
            print(f"\n\nIMPORT COMPLETED.")
            print(f"Batch ID: {import_id}")
            print(f"Files processed: {processed_count}/{len(files)}")
            print(f"Total gate rows inserted: {grand_total}")
            logger.info("IMPORT COMPLETED. ID: %d, Total Gates: %d", import_id, grand_total)

        finally:
            conn.close()
            logger.info("Database connection closed.")

    except Exception as e:
        print(f"\nA fatal error occurred: {e}")
        logger.exception("A fatal error occurred: %s", e)

if __name__ == "__main__":
    main()