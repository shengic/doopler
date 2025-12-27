# -*- coding: utf-8 -*-
# File: dooplerDashboard.py
# Purpose: Integrated Doppler Lidar Management Console
# Version: 3.4.9 (Absolute Module Reload + Embedded Plotting + Region 3 Logic)

import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import pymysql
import sys
import os
import logging
import importlib
from pathlib import Path
from datetime import datetime

# Matplotlib embedding dependencies
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

# =============================================================================
# 1. SYSTEM INITIALIZATION
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Dashboard")

# Helper for aggressive re-importing of custom modules
def force_load_module(module_name):
    """Ensures the dashboard always sees the latest disk version of a module."""
    if module_name in sys.modules:
        del sys.modules[module_name]
    try:
        # Import the module dynamically by its string name
        return importlib.import_module(module_name)
    except Exception as e:
        logger.error(f"Failed to load {module_name}: {e}")
        return None

class DopplerApp:
    def __init__(self, root):
        """Initialize the Dashboard with full geometry and database configuration."""
        self.root = root
        self.root.title("Doppler Lidar Management Console v3.4.9")
        
        # --- Window Geometry & Screen Centering ---
        window_width, window_height = 1250, 950
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        center_x = int(screen_width/2 - window_width / 2)
        center_y = int(screen_height/2 - window_height / 2)
        self.root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
        
        # Database Credentials (matches SQL schema provided)
        self.db_config = {
            "host": "localhost",
            "user": "shengic",
            "password": "sirirat",
            "database": "doopler",
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
            "connect_timeout": 5
        }
        
        # UI State Management
        self.current_log_view = "header" # 'header', 'proc', or 'plot'
        self.canvas_widget = None
        self.toolbar_widget = None
        
        # Build UI Components
        self.create_widgets()
        
        # Initial Data Load
        self.refresh_db_status() 
        self.load_rules() 
        logger.info("Dashboard v3.4.9 initialized.")

    def create_widgets(self):
        """Constructs the full UI with pipeline and dynamic display area."""
        
        # --- SECTION 1: Step-by-Step Processing Pipeline ---
        group_pipeline = ttk.LabelFrame(self.root, text="Step-by-Step Processing Pipeline")
        group_pipeline.pack(fill="x", padx=25, pady=10)
        
        btn_layout = {"side": "left", "expand": True, "fill": "x", "padx": 8, "pady": 12}
        
        # Button 1: Import
        f1 = ttk.Frame(group_pipeline)
        f1.pack(**btn_layout)
        ttk.Button(f1, text="1. Import HPL Files", command=self.handle_insert).pack(fill="x")
        self.lbl_gate_count = ttk.Label(f1, text="Total Gates: 0", foreground="#0056b3", font=("Arial", 9, "bold"))
        self.lbl_gate_count.pack()

        # Button 2: QC
        f2 = ttk.Frame(group_pipeline)
        f2.pack(**btn_layout)
        ttk.Button(f2, text="2. Run Quality Control (QC)", command=self.handle_qc).pack(fill="x")
        ttk.Label(f2, text="").pack()

        # Button 2.1: View Process Runs
        f_proc = ttk.Frame(group_pipeline)
        f_proc.pack(**btn_layout)
        ttk.Button(f_proc, text="2.1 View Process Runs", command=self.handle_view_proc_run).pack(fill="x")
        ttk.Label(f_proc, text="").pack()

        # Button 3: UVW Calculation (Invokes wind_profile_uvw_v2.py)
        f3 = ttk.Frame(group_pipeline)
        f3.pack(**btn_layout)
        ttk.Button(f3, text="3. Calculate UVW Wind", command=self.handle_uvw).pack(fill="x")
        self.lbl_uvw_count = ttk.Label(f3, text="UVW Solved: 0", foreground="#0056b3", font=("Arial", 9, "bold"))
        self.lbl_uvw_count.pack()

        # Button 4: Plot
        f4 = ttk.Frame(group_pipeline)
        f4.pack(**btn_layout)
        ttk.Button(f4, text="4. Plot Wind Profile", command=self.handle_plot).pack(fill="x")
        ttk.Label(f4, text="").pack()

        # --- SECTION: Visualization & Calendar Sync ---
        group_plot_ctrl = ttk.LabelFrame(self.root, text="Plot Window Configuration")
        group_plot_ctrl.pack(fill="x", padx=25, pady=5)

        self.lbl_db_info = ttk.Label(group_plot_ctrl, text="Available Range: Detecting...", font=("Arial", 9, "italic"))
        self.lbl_db_info.pack(side="top", anchor="w", padx=10, pady=2)

        pick_frame = ttk.Frame(group_plot_ctrl)
        pick_frame.pack(side="top", fill="x", pady=5)

        ttk.Label(pick_frame, text="Start:").pack(side="left", padx=5)
        self.cal_start = DateEntry(pick_frame, width=12, background='darkblue', foreground='white', date_pattern='yyyy-mm-dd')
        self.cal_start.pack(side="left", padx=5)

        ttk.Label(pick_frame, text="End:").pack(side="left", padx=5)
        self.cal_end = DateEntry(pick_frame, width=12, background='darkblue', foreground='white', date_pattern='yyyy-mm-dd')
        self.cal_end.pack(side="left", padx=5)

        ttk.Button(pick_frame, text="🔄 Back to Header Log", command=self.handle_view_header_log).pack(side="left", padx=15)
        ttk.Button(pick_frame, text="🔍 Refresh Data", command=self.refresh_db_status).pack(side="left", padx=5)

        # --- SECTION 2: Dynamic Data Log Window (Region 3) ---
        self.group_log = ttk.LabelFrame(self.root, text="Data Log Window")
        self.group_log.pack(fill="both", expand=True, padx=25, pady=10)
        
        # Container for Tables
        self.table_frame = ttk.Frame(self.group_log)
        self.table_frame.pack(fill="both", expand=True, padx=5, pady=5)

        self.log_cols = ("c1", "c2", "c3", "c4", "c5", "c6")
        self.tree = ttk.Treeview(self.table_frame, columns=self.log_cols, show='headings', height=12)
        vsb = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(column=0, row=0, sticky='nsew')
        vsb.grid(column=1, row=0, sticky='ns')
        hsb.grid(column=0, row=1, sticky='ew')
        self.table_frame.grid_columnconfigure(0, weight=1); self.table_frame.grid_rowconfigure(0, weight=1)

        # Container for Embedded Plot
        self.plot_frame = ttk.Frame(self.group_log)

        # --- SECTION 3: QC Rule Configuration (vad_rule_qc) ---
        group_rules = ttk.LabelFrame(self.root, text="Quality Control Rule Configuration")
        group_rules.pack(fill="x", padx=25, pady=5)
        r_tree_frame = ttk.Frame(group_rules)
        r_tree_frame.pack(fill="x", padx=10, pady=5)
        r_cols = ("rid", "name", "code", "status", "order")
        self.rule_tree = ttk.Treeview(r_tree_frame, columns=r_cols, show='headings', height=5)
        for col, head in zip(r_cols, ["ID", "Definition Name", "Rule Code", "Status", "Order"]):
            self.rule_tree.heading(col, text=head)
        self.rule_tree.column("rid", width=50, anchor="center")
        self.rule_tree.column("name", width=250)
        self.rule_tree.column("status", width=100, anchor="center")
        self.rule_tree.pack(fill="x", side="left", expand=True)
        r_btn_frame = ttk.Frame(group_rules)
        r_btn_frame.pack(fill="x", padx=10, pady=5)
        ttk.Button(r_btn_frame, text="🔄 Reload Rules", command=self.load_rules).pack(side="left", padx=5)
        ttk.Button(r_btn_frame, text="📝 Edit Description", command=self.handle_edit_desc).pack(side="left", padx=5)

        # --- SECTION 4: Maintenance ---
        group_maint = ttk.LabelFrame(self.root, text="System Maintenance")
        group_maint.pack(fill="x", padx=25, pady=15)
        ttk.Label(group_maint, text="To clear observation data and reset IDs, please run 'dooplerReset.py' manually.", 
                  foreground="#d9534f", font=("Arial", 9, "bold")).pack(side="left", padx=15)
        ttk.Button(group_maint, text="⚠️ RESET HINT", command=self.handle_reset).pack(side="right", padx=15, pady=10)

    # =========================================================================
    # LOGIC & DB HANDLERS
    # =========================================================================

    def _get_db_conn(self):
        return pymysql.connect(**self.db_config)

    def switch_view(self, view_type):
        """Switches visibility between table (header/proc) and plot."""
        self.current_log_view = view_type
        if view_type == "plot":
            self.table_frame.pack_forget()
            self.plot_frame.pack(fill="both", expand=True)
        else:
            self.plot_frame.pack_forget()
            self.table_frame.pack(fill="both", expand=True)

    def refresh_db_status(self):
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) as cnt FROM wind_profile_gate")
                self.lbl_gate_count.config(text=f"Total Gates: {cur.fetchone()['cnt']:,}")
                cur.execute("SELECT COUNT(*) as cnt FROM vad_gate_fit WHERE status='ok'")
                self.lbl_uvw_count.config(text=f"UVW Solved: {cur.fetchone()['cnt']:,}")
                cur.execute("SELECT MIN(start_time) as t_min, MAX(start_time) as t_max FROM wind_profile_header")
                res = cur.fetchone()
                if res and res['t_min']:
                    self.lbl_db_info.config(text=f"Available: {res['t_min']} to {res['t_max']}")
                    self.cal_start.set_date(res['t_min'].date())
                    self.cal_end.set_date(res['t_max'].date())
        except Exception as e: logger.error(f"Sync Error: {e}")
        finally:
            if conn: conn.close()

        if self.current_log_view == "header": self.show_header_log()
        elif self.current_log_view == "proc": self.show_proc_run_log()

    def show_header_log(self):
        self.switch_view("header")
        self.group_log.config(text="Data Log: wind_profile_header")
        headers = ["Header ID", "Start Time", "Filename", "Gates", "Gate Len (m)", "Proc Runs"]
        widths = [80, 160, 450, 60, 90, 100]
        for i, (h, w) in enumerate(zip(headers, widths)):
            self.tree.heading(f"c{i+1}", text=h)
            self.tree.column(f"c{i+1}", width=w, anchor="center" if i!=2 else "w")
        for i in self.tree.get_children(): self.tree.delete(i)
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                sql = """
                    SELECT h.header_id, h.start_time, h.filename, h.num_gates, h.range_gate_length_m, 
                    (SELECT COUNT(DISTINCT run_id) FROM vad_gate_fit f WHERE f.header_id = h.header_id) as proc_run_count
                    FROM wind_profile_header h ORDER BY h.start_time DESC LIMIT 50
                """
                cur.execute(sql)
                for r in cur.fetchall():
                    self.tree.insert("", "end", values=(r['header_id'], r['start_time'], r['filename'], r['num_gates'], r['range_gate_length_m'], r['proc_run_count']))
        except Exception as e: logger.error(e)
        finally:
            if conn: conn.close()

    def show_proc_run_log(self):
        self.switch_view("proc")
        self.group_log.config(text="Data Log: proc_run")
        headers = ["Run ID", "Rule Tag", "Started At", "Finished At", "Params JSON", ""]
        widths = [100, 150, 160, 160, 450, 10]
        for i, (h, w) in enumerate(zip(headers, widths)):
            self.tree.heading(f"c{i+1}", text=h)
            self.tree.column(f"c{i+1}", width=w, anchor="center" if i<4 else "w")
        for i in self.tree.get_children(): self.tree.delete(i)
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT run_id, rule_tag, started_at, finished_at, params_json FROM proc_run ORDER BY started_at DESC LIMIT 50")
                for r in cur.fetchall():
                    self.tree.insert("", "end", values=(r['run_id'], r['rule_tag'], r['started_at'], r['finished_at'], r['params_json'], ""))
        except Exception as e: logger.error(e)
        finally:
            if conn: conn.close()

    def handle_view_header_log(self): self.show_header_log()
    def handle_view_proc_run(self): self.show_proc_run_log()

    # --- PIPELINE ACTIONS ---
    def handle_insert(self):
        m = force_load_module("dooplerInsert_v3")
        try: m.main(); self.refresh_db_status(); messagebox.showinfo("Success", "Import Completed.")
        except Exception as e: messagebox.showerror("Error", str(e))

    def handle_qc(self):
        m = force_load_module("qc_tagging_v2")
        try: m.main(); self.refresh_db_status(); messagebox.showinfo("Success", "QC Finished.")
        except Exception as e: messagebox.showerror("Error", str(e))

    def handle_uvw(self):
        """Absolute fresh reload for UVW module to catch the 'main' attribute."""
        m = force_load_module("wind_profile_uvw_v2")
        if not m:
            messagebox.showerror("Error", "Could not load 'wind_profile_uvw_v2.py'. Check terminal.")
            return
        
        try:
            # Here we invoke the code inside wind_profile_uvw_v2.py
            if hasattr(m, 'main'):
                m.main()
                self.refresh_db_status()
                messagebox.showinfo("Success", "VAD Solved.")
            else:
                messagebox.showerror("Module Error", "Attribute 'main' not found. Verify 'def main():' exists in script.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def handle_plot(self):
        m = force_load_module("plot_wind_profile")
        start = self.cal_start.get_date().strftime('%Y-%m-%d 00:00:00')
        end = self.cal_end.get_date().strftime('%Y-%m-%d 23:59:59')
        
        df = m.get_wind_data(start_date=start, end_date=end)
        if df.empty:
            messagebox.showwarning("No Data", f"No wind data found for: {start[:10]}")
            return

        self.switch_view("plot")
        self.group_log.config(text=f"Wind Plot: {start[:10]} to {end[:10]}")
        for w in self.plot_frame.winfo_children(): w.destroy()

        fig = m.create_wind_figure(df, start, end)
        self.canvas_widget = FigureCanvasTkAgg(fig, master=self.plot_frame)
        self.canvas_widget.draw()
        self.toolbar_widget = NavigationToolbar2Tk(self.canvas_widget, self.plot_frame)
        self.toolbar_widget.update()
        self.canvas_widget.get_tk_widget().pack(fill="both", expand=True)

    def handle_reset(self):
        messagebox.showinfo("Reset", "Manual step: run 'python dooplerReset.py' in console.")

    # --- RULE MANAGEMENT ---
    def load_rules(self):
        for item in self.rule_tree.get_children(): self.rule_tree.delete(item)
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT rule_id, def_name, rule_code, is_active, rule_order FROM vad_rule_qc ORDER BY rule_order")
                for r in cur.fetchall():
                    self.rule_tree.insert("", "end", values=(r['rule_id'], r['def_name'], r['rule_code'], "Active" if r['is_active'] else "Inactive", r['rule_order']))
        except Exception as e: logger.error(e)
        finally:
            if conn: conn.close()

    def handle_edit_desc(self):
        selection = self.rule_tree.selection()
        if not selection: return
        rid = self.rule_tree.item(selection)['values'][0]
        popup = tk.Toplevel(self.root)
        popup.title(f"Edit ID {rid}")
        popup.geometry("600x450")
        popup.transient(self.root); popup.grab_set()

        curr_desc = ""
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT description FROM vad_rule_qc WHERE rule_id = %s", (rid,))
                res = cur.fetchone()
                if res: curr_desc = res['description']
        except Exception: pass
        finally:
            if conn: conn.close()

        tk.Label(popup, text=f"Description for Rule {rid}:", font=("Arial", 10, "bold")).pack(pady=10)
        txt = tk.Text(popup, height=12, width=65, wrap="word", padx=10, pady=10)
        txt.insert("1.0", str(curr_desc))
        txt.pack(padx=20, pady=10)

        def save():
            val = txt.get("1.0", "end-1c").strip()
            db = None
            try:
                db = self._get_db_conn()
                with db.cursor() as cur:
                    cur.execute("UPDATE vad_rule_qc SET description = %s WHERE rule_id = %s", (val, rid))
                db.commit(); messagebox.showinfo("Success", "Saved."); popup.destroy()
            except Exception as ex: messagebox.showerror("Error", str(ex))
            finally:
                if db: db.close()
        ttk.Button(popup, text="Save", command=save).pack(pady=15)

if __name__ == "__main__":
    app_root = tk.Tk()
    dashboard = DopplerApp(app_root)
    app_root.mainloop()