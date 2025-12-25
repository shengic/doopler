# -*- coding: utf-8 -*-
# qc_tagging_v2.py
# Version: 2.3 (Dashboard Integrated + Single Line Progress)

import math
import sys
import pymysql
import logging
import re
from collections import defaultdict
from typing import List, Dict, Tuple, Any

# =========================
# Logging & Global Setup
# =========================
# Ensure logger is defined at the module level
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("qc_tagging_v2")

# ---------------------------
# Parameters (tune as needed)
# ---------------------------
SNR_MIN = 0.015            
K_SW = 1.5
TILT_ABS_MAX = 2.0         
TILT_RSS_MAX = 2.5         
ELEV_MIN, ELEV_MAX = 10.0, 89.9 
AZ_DUP_TOL = 0.1           
VR_ABS_MAX = 60.0          
MAD_K = 3.5
MIN_RAYS = 3               
MIN_SPAN_DEG = 120.0       
BIN_DEG = 10.0
MIN_NONEMPTY_BINS = 3      
VERT_THR = 2.0             
NEIGHBOR_STEP = 1          

# =========================
# Config (Matches doopler.sql)
# =========================
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "shengic"
DB_PASSWORD = "sirirat"
DB_NAME = "doopler"

# ---------------
# Rule registry
# ---------------
RULE_REGISTRY = {}

def register(name):
    def deco(func):
        RULE_REGISTRY[name] = func
        return func
    return deco

# ---------- Utilities ----------
def safe_float(x, default=None):
    try:
        return float(x) if x is not None else default
    except:
        return default

def median(values):
    vals = sorted(values)
    n = len(vals)
    if n == 0: return None
    mid = n // 2
    return vals[mid] if n % 2 == 1 else 0.5 * (vals[mid-1] + vals[mid])

def mad(values, med=None):
    if not values: return None
    if med is None: med = median(values)
    dev = [abs(v - med) for v in values]
    return median(dev)

def norm360(a):
    if a is None: return None
    a = float(a) % 360.0
    return 0.0 if (abs(a - 360.0) <= AZ_DUP_TOL or abs(a) <= AZ_DUP_TOL) else a

def circular_span_deg(unique_az_deg):
    n = len(unique_az_deg)
    if n <= 1: return 0.0
    s = sorted(unique_az_deg)
    gaps = [(s[(i+1) % n] - s[i]) % 360.0 for i in range(n)]
    return 360.0 - max(gaps)

def snap_azimuths_per_gate(rows_for_gate, tol=AZ_DUP_TOL):
    tmp = []
    for r in rows_for_gate:
        az = norm360(r.get('azimuth_deg'))
        tmp.append((r['range_gate_index'], r['ray_idx'], az))
    tmp.sort(key=lambda x: (9999.0 if x[2] is None else x[2]))

    seen, canon_by_rowkey, dup_by_rowkey = [], {}, {}
    for gi, ray_idx, az in tmp:
        key = (gi, ray_idx)
        if az is None:
            dup_by_rowkey[key] = True; canon_by_rowkey[key] = None; continue
        rep = next((c for c in seen if min(abs(az-c), 360-abs(az-c)) <= tol), None)
        if rep is None:
            seen.append(az); canon_by_rowkey[key] = az; dup_by_rowkey[key] = False
        else:
            canon_by_rowkey[key] = rep; dup_by_rowkey[key] = True
    return canon_by_rowkey, dup_by_rowkey, sorted(seen)

# ----------------------
# Rule Functions
# ----------------------
@register('check_nulls')
def check_nulls(row, ctx):
    for key in ('doppler_ms','azimuth_deg','elevation_deg'):
        if row.get(key) is None: return False, f"{key}=NULL"
    return True, None

@register('check_snr_min')
def check_snr_min(row, ctx):
    snr = safe_float(row.get('intensity_snr_plus1'), 1.0) - 1.0
    return (snr >= SNR_MIN), None if (snr >= SNR_MIN) else f"snr={snr:.3f}"

@register('check_spectral_width_max')
def check_spectral_width_max(row, ctx):
    sw = safe_float(row.get('spectral_width_ms'), 0.0)
    instr_sw = safe_float(ctx.get('instrument_spectral_width_ms'), 0.0)
    thr = K_SW * (instr_sw if instr_sw > 0 else 1.0)
    return (sw <= thr), None if (sw <= thr) else f"sw={sw:.3f}"

@register('check_pitch_roll_max')
def check_pitch_roll_max(row, ctx):
    m = max(abs(safe_float(row.get('pitch_deg'), 0.0)), abs(safe_float(row.get('roll_deg'), 0.0)))
    return (m <= TILT_ABS_MAX), None if (m <= TILT_ABS_MAX) else f"tilt={m:.2f}"

@register('check_elevation_range')
def check_elevation_range(row, ctx):
    elev = safe_float(row.get('elevation_deg'))
    ok = (elev is not None and ELEV_MIN <= elev <= ELEV_MAX)
    return ok, None if ok else "elev_out"

@register('check_azimuth_duplicate_guard')
def check_azimuth_duplicate_guard(row, ctx):
    is_dup = ctx.get('az_dup_by_rowkey', {}).get((row['range_gate_index'], row['ray_idx']), False)
    return (not is_dup), None if not is_dup else "dup_az"

@register('check_velocity_bounds')
def check_velocity_bounds(row, ctx):
    vr = safe_float(row.get('doppler_ms'))
    ok = (vr is not None and abs(vr) <= VR_ABS_MAX)
    return ok, None if ok else "vr_out"

@register('check_gate_outlier_mad')
def check_gate_outlier_mad(row, ctx):
    bad = ctx['mad_fail_by_rowkey'].get((row['range_gate_index'], row['ray_idx']), False)
    return (not bad), None

@register('check_azimuth_coverage_gate')
def check_azimuth_coverage_gate(row, ctx):
    info = ctx['coverage_by_gate'].get(row['range_gate_index'], {'count':0,'span':0.0})
    ok = (info['count'] >= MIN_RAYS and info['span'] >= MIN_SPAN_DEG)
    return ok, None

@register('check_vertical_consistency')
def check_vertical_consistency(row, ctx):
    val = ctx['vert_metric_by_gate'].get(row['range_gate_index'])
    ok = (val is None or val <= VERT_THR)
    return ok, None

@register('check_gate_uniform_bin_fill')
def check_gate_uniform_bin_fill(row, ctx):
    info = ctx['binfill_by_gate'].get(row['range_gate_index'], {'nonempty':0})
    return (info['nonempty'] >= MIN_NONEMPTY_BINS), None

# ----------------------
# Logic & DB Helpers
# ----------------------

def get_connection():
    """Provides a consistent connection for main() and external dashboard"""
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, 
        db=DB_NAME, charset="utf8mb4", autocommit=False, 
        cursorclass=pymysql.cursors.DictCursor
    )

def fetch_pending_headers(conn, limit=1000):
    """Finds headers that need QC"""
    sql = "SELECT DISTINCT header_id FROM wind_profile_gate WHERE qc_selected=0 AND qc_failed_rule_count=0 LIMIT %s"
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [r['header_id'] for r in cur.fetchall()]

def precompute_gate_context(rows, header):
    by_gate = defaultdict(list)
    for r in rows: by_gate[r['range_gate_index']].append(r)

    canon_az_by_rowkey, dup_by_rowkey, unique_canon_by_gate = {}, {}, {}
    for gi, lst in by_gate.items():
        c, d, u = snap_azimuths_per_gate(lst)
        canon_az_by_rowkey.update(c); dup_by_rowkey.update(d); unique_canon_by_gate[gi] = u

    mad_fail_by_rowkey, gate_medians = {}, {}
    for gi, lst in by_gate.items():
        vrs = [safe_float(x.get('doppler_ms')) for x in lst if x.get('doppler_ms') is not None]
        med = median(vrs) if vrs else None
        gate_medians[gi] = med
        m = max(mad(vrs, med) if vrs else 0.05, 0.05)
        if med is not None:
            for x in lst:
                vr = safe_float(x.get('doppler_ms'))
                mad_fail_by_rowkey[(gi, x['ray_idx'])] = (vr is None or (abs(vr - med) / (1.4826 * m)) > MAD_K)

    coverage_by_gate, binfill_by_gate = {}, {}
    for gi in by_gate.keys():
        uniq = unique_canon_by_gate.get(gi, [])
        coverage_by_gate[gi] = {'count': len(uniq), 'span': circular_span_deg(uniq)}
        binfill_by_gate[gi] = {'nonempty': len(set(int(a // BIN_DEG) for a in uniq))}

    vert_metric_by_gate = {}
    gis = sorted(by_gate.keys())
    for idx, gi in enumerate(gis):
        nbrs = [gate_medians.get(gis[idx+s]) for s in [-1, 1] if 0 <= idx+s < len(gis)]
        nbrs = [v for v in nbrs if v is not None]
        cur = gate_medians.get(gi)
        vert_metric_by_gate[gi] = abs(cur - (sum(nbrs)/len(nbrs))) if (cur is not None and nbrs) else None

    return {
        'mad_fail_by_rowkey': mad_fail_by_rowkey, 'coverage_by_gate': coverage_by_gate,
        'binfill_by_gate': binfill_by_gate, 'az_dup_by_rowkey': dup_by_rowkey,
        'vert_metric_by_gate': vert_metric_by_gate, 'instrument_spectral_width_ms': header.get('instrument_spectral_width_ms')
    }

def run_qc_process(conn):
    """Primary execution logic for QC tagging"""
    with conn.cursor() as cur:
        cur.execute("SELECT rule_id, def_name FROM vad_rule_qc WHERE is_active=1")
        rules = [(int(r['rule_id']), r['def_name'], RULE_REGISTRY[r['def_name']]) for r in cur.fetchall() if r['def_name'] in RULE_REGISTRY]
    
    pending = fetch_pending_headers(conn, 2000)
    if not pending:
        logger.info("No pending header IDs found.")
        return

    print(f"Found {len(pending)} pending header IDs. Starting...")
    for idx, hid in enumerate(pending, 1):
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM wind_profile_header WHERE header_id=%s", (hid,))
                header = cur.fetchone()
                cur.execute("SELECT * FROM wind_profile_gate WHERE header_id=%s", (hid,))
                rows = cur.fetchall()
            
            ctx = precompute_gate_context(rows, header)
            updates, pass_cnt, fail_cnt = [], 0, 0
            for i, row in enumerate(rows, 1):
                f_ids = [r_id for r_id, name, func in rules if not func(row, ctx)[0]]
                if f_ids: fail_cnt += 1
                else: pass_cnt += 1
                updates.append((1 if not f_ids else 0, ",".join(map(str, f_ids)) if f_ids else None, len(f_ids), hid, row['range_gate_index'], row['ray_idx']))
                
                if i % 100 == 0 or i == len(rows):
                    sys.stdout.write(f"\r[Batch {idx}/{len(pending)}] Header {hid} | Pass: {pass_cnt} Fail: {fail_cnt}".ljust(100))
                    sys.stdout.flush()

            with conn.cursor() as cur:
                cur.executemany("UPDATE wind_profile_gate SET qc_selected=%s, qc_failed_rules_csv=%s, qc_failed_rule_count=%s WHERE header_id=%s AND range_gate_index=%s AND ray_idx=%s", updates)
            conn.commit()
        except Exception as e:
            conn.rollback(); logger.error(f"Error on Header {hid}: {e}")

# =========================
# Entry Points
# =========================

def main():
    """Consolidated main entry point for script and dashboard"""
    try:
        conn = get_connection()
        run_qc_process(conn)
        conn.close()
        print("\n[DONE] QC Tagging Finished.")
    except Exception as e:
        logger.error(f"QC process failed: {e}")
        raise e 

if __name__ == "__main__":
    main()