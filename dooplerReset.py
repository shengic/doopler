#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: dooplerReset.py
Version: 1.2.0
Description: Wipes Lidar observation data and resets AUTO_INCREMENT counters to 1.
Preserves: Configuration tables like 'vad_rule_qc'.
"""

import pymysql
import logging
import sys

# Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "shengic",
    "password": "sirirat",
    "database": "doopler",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# Tables to be cleared (Child tables first, then Parent tables)
TABLES_TO_WIPE = [
    "vad_gate_fit",
    "wind_profile_gate",
    "wind_profile_header",
    "proc_run",
    "import_run"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ResetTool")

def get_row_count(cur, table_name):
    """Retrieves current row count for a table."""
    try:
        cur.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
        result = cur.fetchone()
        return result['cnt']
    except:
        return 0

def reset_database():
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            print("\n" + "!" * 60)
            print(" SYSTEM RESET: DATA DELETION & ID COUNTER RECOVERY")
            print("!" * 60)
            print("\nThe following tables will be COMPLETELY WIPED:")
            
            # Show current data volume
            for table in TABLES_TO_WIPE:
                count = get_row_count(cur, table)
                print(f" - {table.ljust(25)} : {str(count).rjust(10)} rows found")
            
            print("\n" + "-" * 60)
            print("EFFECTS:")
            print("1. All data listed above will be PERMANENTLY DELETED.")
            print("2. ALL AUTO_INCREMENT ID COUNTERS WILL RESET TO 1.")
            print("3. Configuration table 'vad_rule_qc' will be preserved.")
            print("-" * 60)

            # Updated Confirmation Logic
            confirm = input("\nType 'yes' or 'y' to confirm (any other key to abort): ").strip().lower()
            
            if confirm not in ['yes', 'y']:
                print("\n>>> Reset cancelled by user. No changes were made.")
                return

            print("\nProcessing reset, please wait...")
            
            # 1. Temporarily disable foreign key checks to allow TRUNCATE
            cur.execute("SET FOREIGN_KEY_CHECKS = 0;")

            # 2. Execute TRUNCATE (Wipes data AND resets the ID counter to 1)
            for table in TABLES_TO_WIPE:
                logger.info(f"Wiping {table} and resetting ID counter to 1...")
                cur.execute(f"TRUNCATE TABLE `{table}`;")

            # 3. Restore foreign key integrity
            cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
            
            conn.commit()
            
            print("\n" + "=" * 60)
            print(" SUCCESS: DATABASE RESET COMPLETED")
            print(" All observation tables are now empty.")
            print(" The next data insertion will start from ID = 1.")
            print("=" * 60)

    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        logger.error(f"Reset failed: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    reset_database()