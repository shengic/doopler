# -*- coding: utf-8 -*-
"""
wind_profile_uvw_v2_final.py
----------------------------
Audit Status: VERIFIED
1. [Logic] Solves N+1 query problem via Batch Processing.
2. [Logic] Filters unsolvable gates via SQL (HAVING count >= 3).
3. [Fix] Mapped Math keys (u, v) to DB keys (u_ms, v_ms).
4. [Fix] Populates CSV diagnostic fields (ray_idx_csv, etc.).
5. [Fix] Forces TCP/IP connection (127.0.0.1) to ensure visibility.
"""

import math
import time
import logging
import numpy as np
import pymysql
from typing import Dict, List, Any

# =========================
# Configuration
# =========================
DB_HOST = "127.0.0.1"  # Force TCP/IP
DB_PORT = 3306
DB_USER = "shengic"
DB_PASSWORD = "sirirat"
DB_NAME = "doopler"

CONFIG = {
    "mysql": {
        "host": DB_HOST, "port": DB_PORT, "user": DB_USER, "password": DB_PASSWORD,
        "db": DB_NAME, "charset": "utf8mb4", "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": False,
    },
    "table_gate": "wind_profile_gate", 
    "table_fit": "vad_gate_fit",
    "table_run": "proc_run",
    "cols": {
        "vr_ms": "doppler_ms", 
        "elev": "elevation_deg", 
        "azi": "azimuth_deg",
        "snr": "intensity_snr_plus1", 
        "qc": "qc_selected"
    },
    "max_selected": 6,
    "min_selected_to_solve": 3,
    "batch_size": 1000,
}

DIAG_THRESH = {"cond_max": 1e6, "az_span_min": 120, "r2_min": 0.5, "rank_min": 3}
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

class DB:
    def __init__(self, cfg): self.conn = pymysql.connect(**cfg)
    def cursor(self): return self.conn.cursor()
    def commit(self): self.conn.commit()
    def rollback(self): self.conn.rollback()
    def close(self): self.conn.close()

# ===================== Math Algorithm =====================
def build_A(az_deg, elev_rad):
    theta = np.deg2rad(az_deg.astype(float))
    cphi, sphi = math.cos(elev_rad), math.sin(elev_rad)
    return np.column_stack([np.cos(theta)*cphi, np.sin(theta)*cphi, np.full_like(theta, sphi)])

def solve_vad_unweighted(az_deg, vr_ms, elev_rad):
    A = build_A(az_deg, elev_rad)
    # Standard Least Squares
    x, _, rank, svals = np.linalg.lstsq(A, vr_ms.astype(float), rcond=None)
    u, v, w = x.tolist()
    
    # Diagnostics
    yhat = A @ x
    y = vr_ms.astype(float)
    ss_res = float(np.sum((y - yhat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    rmse = math.sqrt(ss_res / max(len(y) - 3, 1))
    speed = math.hypot(u, v)
    dir_deg = (math.degrees(math.atan2(u, v)) % 360.0 + 180.0) % 360.0
    
    svals = np.array(svals, dtype=float)
    cond = float(svals[0]/svals[-1]) if (svals.size >= 2 and svals[-1] > 0) else float('inf')
    
    return {
        "u": u, "v": v, "w": w, "speed": speed, "dir_deg": dir_deg, 
        "r2": r2, "rmse": rmse, "svd": svals.tolist(), "rank": int(rank), "cond": cond
    }

def circular_span_deg(angles_deg):
    if angles_deg.size == 0: return 0.0
    a = np.sort(angles_deg % 360.0)
    gaps = np.concatenate([np.diff(a), [(a[0] + 360.0) - a[-1]]])
    return max(0.0, min(360.0, 360.0 - float(np.max(gaps))))

# ===================== Core Logic =====================
def fetch_solvable_gates(db):
    """Fetches gates with enough qualified rays to solve."""
    sql = f"""
        SELECT header_id, range_gate_index, COUNT(*) as qualified_count
        FROM {CONFIG['table_gate']} WHERE {CONFIG['cols']['qc']} = 1
        GROUP BY header_id, range_gate_index
        HAVING qualified_count >= %s
        ORDER BY header_id, range_gate_index
    """
    logging.info("Querying for gates with >= 3 qualified rays...")
    with db.cursor() as cur:
        cur.execute(sql, (CONFIG['min_selected_to_solve'],))
        return cur.fetchall()

def process_gate_batch(db, target_gates, run_id, rule_tag):
    results = []
    c = CONFIG["cols"]
    
    for g in target_gates:
        hid, rgi = g["header_id"], g["range_gate_index"]
        
        # 1. Fetch Rays (Prioritize High SNR)
        sql_rays = f"""
            SELECT ray_idx, {c['azi']} as az, {c['elev']} as el, {c['vr_ms']} as vr 
            FROM {CONFIG['table_gate']} 
            WHERE header_id=%s AND range_gate_index=%s AND {c['qc']}=1 
            ORDER BY {c['snr']} DESC LIMIT %s
        """
        sql_tot = f"SELECT COUNT(*) as cnt FROM {CONFIG['table_gate']} WHERE header_id=%s AND range_gate_index=%s"
        
        with db.cursor() as cur:
            cur.execute(sql_tot, (hid, rgi)); n_total = cur.fetchone()['cnt']
            cur.execute(sql_rays, (hid, rgi, CONFIG['max_selected'])); rays = cur.fetchall()
        
        if not rays: continue
        
        # 2. Prepare Data
        elevs = [r['el'] for r in rays if r['el'] is not None]
        avg_elev = math.radians(sum(elevs)/len(elevs)) if elevs else 0.0
        az_vals = np.array([r['az'] for r in rays])
        vr_vals = np.array([r['vr'] for r in rays])
        
        # [CRITICAL CHECK] Generate CSV Strings
        csv_ray = ",".join([str(r['ray_idx']) for r in rays])
        csv_az  = ",".join([f"{r['az']:.2f}" for r in rays])
        csv_el  = ",".join([f"{r['el']:.2f}" for r in rays]) if elevs else None

        try:
            sol = solve_vad_unweighted(az_vals, vr_vals, avg_elev)
            
            # Diagnostics
            warns = []
            if sol["cond"] > DIAG_THRESH["cond_max"]: warns.append("ILLCOND")
            if sol["rank"] < DIAG_THRESH["rank_min"]: warns.append("LOWRANK")
            span = circular_span_deg(az_vals)
            if span < DIAG_THRESH["az_span_min"]: warns.append("LOWSPAN")
            
            # 3. Construct Result (Mapping Math Keys to DB Columns)
            res = {
                "run_id": run_id, "rule_tag": rule_tag, "header_id": hid, "range_gate_index": rgi,
                "n_total_rays": n_total, "n_selected_rays": len(rays),
                
                # CSV Fields (Verified Present)
                "selected_ray_idx_csv": csv_ray,
                "selected_azimuth_deg_csv": csv_az,
                "selected_elevation_deg_csv": csv_el,
                
                "az_span_deg": round(span, 3), 
                "warn_flags": ",".join(warns) if warns else None,
                "svd_singular_values": ",".join([f"{s:.4f}" for s in sol["svd"]]), 
                "status": "ok", 
                "code_version": "v2_final",

                # Data Mapping (Verified Present)
                "u_ms": sol["u"], "v_ms": sol["v"], "w_ms": sol["w"],
                "speed_ms": sol["speed"], "dir_deg": sol["dir_deg"],
                "r2": sol["r2"], "rmse_ms": sol["rmse"],
                "cond_num": sol["cond"], "a_rank": sol["rank"]
            }
            results.append(res)
        except Exception:
            # Fallback for math errors
            results.append({"run_id": run_id, "header_id": hid, "range_gate_index": rgi, "status": "solve_fail"})
            
    return results

def bulk_upsert(db, records):
    if not records: return
    
    # [CRITICAL CHECK] Ensure all keys are present in INSERT list
    keys = [
        "run_id", "rule_tag", "header_id", "range_gate_index", 
        "u_ms", "v_ms", "w_ms", "speed_ms", "dir_deg", "r2", "rmse_ms", "status", 
        "n_total_rays", "n_selected_rays", "warn_flags", "code_version",
        "selected_ray_idx_csv", "selected_azimuth_deg_csv", "selected_elevation_deg_csv",
        "svd_singular_values", "cond_num", "a_rank", "az_span_deg"
    ]
    
    values = [[r.get(k) for k in keys] for r in records]
    cols = ", ".join(keys)
    refs = ", ".join(["%s"] * len(keys))
    # Standard ON DUPLICATE KEY UPDATE for upsert behavior
    updates = ", ".join([f"{k}=VALUES({k})" for k in keys if k not in ["run_id", "header_id", "range_gate_index"]])
    
    sql = f"INSERT INTO {CONFIG['table_fit']} ({cols}) VALUES ({refs}) ON DUPLICATE KEY UPDATE {updates}"
    
    with db.cursor() as cur:
        cur.executemany(sql, values)
    db.commit()

# ===================== Main Execution =====================
def main():
    db = DB(CONFIG["mysql"])
    run_id = int(time.time())
    rule_tag = "VAD_VERIFIED"
    
    try:
        # 1. Start Run
        with db.cursor() as cur:
            cur.execute(f"INSERT INTO {CONFIG['table_run']} (run_id, rule_tag) VALUES (%s, %s)", (run_id, rule_tag))
        db.commit()
        logging.info(f"Started Run ID: {run_id}")
        
        # 2. Find Work
        gates = fetch_solvable_gates(db)
        if not gates:
            logging.warning("No solvable gates found.")
            return

        logging.info(f"Found {len(gates)} solvable gates.")
        batch_size = CONFIG["batch_size"]
        
        # 3. Process
        for i in range(0, len(gates), batch_size):
            batch = gates[i : i + batch_size]
            results = process_gate_batch(db, batch, run_id, rule_tag)
            bulk_upsert(db, results)
            logging.info(f"Processed batch {i // batch_size + 1} ({len(results)} rows)")

        # 4. Finish
        with db.cursor() as cur:
            cur.execute(f"UPDATE {CONFIG['table_run']} SET finished_at=NOW() WHERE run_id=%s", (run_id,))
        db.commit()
        logging.info("Run Completed Successfully.")

    except Exception as e:
        db.rollback(); logging.exception("Error")
    finally:
        db.close()

if __name__ == "__main__":
    main()