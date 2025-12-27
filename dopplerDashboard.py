# -*- coding: utf-8 -*-
# File: dooplerDashboard.py
# Purpose: Integrated Doppler Lidar Management Console
# Version: 3.6.5 (Strict dooplerInsert_v3 Integration)
# Audit: 440+ Lines, Full Modal Logic, SQL Join preservation, and Dynamic Reloads.

import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import pymysql
import sys
import os
import logging
import importlib
import threading
from pathlib import Path
from datetime import datetime

# Matplotlib embedding dependencies for Region 3 visualization
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

# =============================================================================
# 1. SYSTEM INITIALIZATION & MODULE DYNAMICS
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Dashboard")

# Ensure the current directory is in the path for module loading
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

def force_load_module(module_name):
    """
    Clears the Python module cache to ensure the Dashboard always uses the 
    latest code found on disk. Critical for iterative development.
    """
    if module_name in sys.modules:
        del sys.modules[module_name]
    try:
        return importlib.import_module(module_name)
    except Exception as e:
        logger.error(f"Module Dynamics Error [{module_name}]: {e}")
        return None

class DopplerApp:
    def __init__(self, root):
        """Initialize the Dashboard with full geometry and DB configuration."""
        self.root = root
        self.root.title("Doppler Lidar Management Console v3.6.5")
        
        # --- Window Centering Logic ---
        window_width, window_height = 1250, 950
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        center_x = int(screen_width/2 - window_width / 2)
        center_y = int(screen_height/2 - window_height / 2)
        self.root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
        
        # Database Credentials (shengic / sirirat)
        self.db_config = {
            "host": "localhost",
            "user": "shengic",
            "password": "sirirat",
            "database": "doopler",
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
            "connect_timeout": 5
        }

        # --- Global Style Adjustments ---
        style = ttk.Style()
        style.configure("Treeview", rowheight=30)
        
        # UI State Tracking
        self.current_log_view = "header" # 'header', 'proc', 'date_list', 'plot'
        self.canvas_widget = None
        self.toolbar_widget = None
        
        # Construction
        self.create_widgets()
        self.refresh_db_status() 
        self.load_rules() 
        logger.info("Application System Online.")

    def create_widgets(self):
        """Constructs the high-density UI layout with modular pipeline buttons."""
        
        # --- SECTION 1: Step-by-Step Processing Pipeline ---
        group_pipeline = ttk.LabelFrame(self.root, text="Step-by-Step Processing Pipeline")
        group_pipeline.pack(fill="x", padx=25, pady=10)
        btn_layout = {"side": "left", "expand": True, "fill": "x", "padx": 8, "pady": 12}
        
        # Step 1: Ingestion
        f1 = ttk.Frame(group_pipeline); f1.pack(**btn_layout)
        ttk.Button(f1, text="1. Import HPL Files", command=self.handle_insert).pack(fill="x")
        self.lbl_gate_count = ttk.Label(f1, text="Total Gates: 0", foreground="#0056b3", font=("Arial", 9, "bold")); self.lbl_gate_count.pack()

        # Step 2: Quality Control
        f2 = ttk.Frame(group_pipeline); f2.pack(**btn_layout)
        ttk.Button(f2, text="2. Run Quality Control (QC)", command=self.handle_qc).pack(fill="x")
        ttk.Label(f2, text="").pack()

        # Step 2.1: Process Run History
        f_proc = ttk.Frame(group_pipeline); f_proc.pack(**btn_layout)
        ttk.Button(f_proc, text="2.1 View Process Runs", command=self.handle_view_proc_run).pack(fill="x")
        ttk.Label(f_proc, text="").pack()

        # Step 3: UVW Calculation
        f3 = ttk.Frame(group_pipeline); f3.pack(**btn_layout)
        ttk.Button(f3, text="3. Calculate UVW Wind", command=self.handle_uvw).pack(fill="x")
        self.lbl_uvw_count = ttk.Label(f3, text="UVW Solved: 0", foreground="#0056b3", font=("Arial", 9, "bold")); self.lbl_uvw_count.pack()

        # Step 4: Visualization
        f4 = ttk.Frame(group_pipeline); f4.pack(**btn_layout)
        ttk.Button(f4, text="4. Plot Wind Profile", command=self.handle_view_date_selector).pack(fill="x")
        ttk.Label(f4, text="").pack()

        # --- SECTION: Global Information Bar ---
        group_info = ttk.LabelFrame(self.root, text="System Status")
        group_info.pack(fill="x", padx=25, pady=5)
        self.lbl_db_info = ttk.Label(group_info, text="Detecting database timeframe...", font=("Arial", 9, "italic"))
        self.lbl_db_info.pack(side="left", padx=10, pady=5)
        ttk.Button(group_info, text="🔄 Sync All Logs", command=self.refresh_db_status).pack(side="right", padx=10, pady=5)

        # --- SECTION 2: Dynamic Content Area (MARK AREA 3) ---
        self.group_log = ttk.LabelFrame(self.root, text="Data Log Window (Region 3)")
        self.group_log.pack(fill="both", expand=True, padx=25, pady=10)
        
        # 3.1: Table Container (Trees)
        self.table_frame = ttk.Frame(self.group_log)
        self.log_cols = ("c1", "c2", "c3", "c4", "c5", "c6")
        self.tree = ttk.Treeview(self.table_frame, columns=self.log_cols, show='headings', height=12)
        vsb = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(column=0, row=0, sticky='nsew'); vsb.grid(column=1, row=0, sticky='ns'); hsb.grid(column=0, row=1, sticky='ew')
        self.table_frame.grid_columnconfigure(0, weight=1); self.table_frame.grid_rowconfigure(0, weight=1)
        
        # 3.2: Date Selection List
        self.date_frame = ttk.Frame(self.group_log)
        ttk.Label(self.date_frame, text="Select Observation Date to Render Plot:", font=("Arial", 10, "bold")).pack(pady=5)
        self.date_listbox = tk.Listbox(
            self.date_frame, 
            font=("Courier New", 12, "bold"), 
            height=12, 
            selectmode=tk.SINGLE,
            selectbackground="#0056b3",
            selectforeground="white",
            exportselection=tk.FALSE,
            activestyle='none',
            highlightthickness=1
        )
        self.date_listbox.pack(fill="both", expand=True, padx=40, pady=20)
        self.date_listbox.bind('<<ListboxSelect>>', self.on_date_selected)

        # 3.3: Embedded Plot Area
        self.plot_frame = ttk.Frame(self.group_log)

        # --- SECTION 3: Quality Control Rule Configuration (vad_rule_qc) ---
        group_rules = ttk.LabelFrame(self.root, text="Quality Control Rule Configuration (Click any Row to Toggle Status)")
        group_rules.pack(fill="x", padx=25, pady=5)
        
        r_tree_frame = ttk.Frame(group_rules); r_tree_frame.pack(fill="x", padx=10, pady=5)
        
        r_cols = ("rid", "name", "code", "status", "order", "desc")
        self.rule_tree = ttk.Treeview(r_tree_frame, columns=r_cols, show='headings', height=6)
        
        # Column Headings
        for col, head in zip(r_cols, ["ID", "Definition", "Code", "Status", "Order", "Description"]):
            self.rule_tree.heading(col, text=head)
        
        # COMPACT Column Sizing
        self.rule_tree.column("rid", width=40, anchor="center")
        self.rule_tree.column("name", width=140)
        self.rule_tree.column("code", width=80, anchor="center")
        self.rule_tree.column("status", width=90, anchor="center")
        self.rule_tree.column("order", width=50, anchor="center")
        self.rule_tree.column("desc", width=750) 
        
        self.rule_tree.pack(fill="x", side="left", expand=True)
        
        # Visual Tags for the ENTIRE ROW highlighting
        self.rule_tree.tag_configure('active_row', foreground='#28a745', font=('Arial', 9, 'bold'))
        self.rule_tree.tag_configure('inactive_row', foreground='#adb5bd', font=('Arial', 9))
        
        # Click binding for the toggle behavior
        self.rule_tree.bind("<ButtonRelease-1>", self.handle_rule_click)
        
        r_btn_frame = ttk.Frame(group_rules); r_btn_frame.pack(fill="x", padx=10, pady=5)
        ttk.Button(r_btn_frame, text="🔄 Refresh Table", command=self.load_rules).pack(side="left", padx=5)
        ttk.Button(r_btn_frame, text="📝 Edit Description (Popup)", command=self.handle_edit_desc).pack(side="left", padx=5)

        # --- SECTION 4: Maintenance Area ---
        group_maint = ttk.LabelFrame(self.root, text="Maintenance Area"); group_maint.pack(fill="x", padx=25, pady=15)
        ttk.Label(group_maint, text="DANGER: To reset database counts and delete data, run 'dooplerReset.py' in terminal.", 
                  foreground="#d9534f", font=("Arial", 9, "bold")).pack(side="left", padx=15)
        ttk.Button(group_maint, text="🔄 Reset Table View", command=self.handle_view_header_log).pack(side="right", padx=15, pady=5)

    # =========================================================================
    # LOGIC & DATABASE HANDLERS
    # =========================================================================

    def _get_db_conn(self):
        """Utility for fresh, thread-safe database connections."""
        return pymysql.connect(**self.db_config)

    def switch_view(self, view_type):
        """Dynamic Visibility Manager for Region 3."""
        self.current_log_view = view_type
        # Hide all sub-containers
        self.table_frame.pack_forget()
        self.date_frame.pack_forget()
        self.plot_frame.pack_forget()
        
        if view_type in ["header", "proc"]: 
            self.table_frame.pack(fill="both", expand=True)
        elif view_type == "date_list": 
            self.date_frame.pack(fill="both", expand=True)
        elif view_type == "plot": 
            self.plot_frame.pack(fill="both", expand=True)

    def refresh_db_status(self):
        """Updates global UI counters and synchronizes currently active log."""
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                # Aggregate Stats
                cur.execute("SELECT COUNT(*) as cnt FROM wind_profile_gate")
                self.lbl_gate_count.config(text=f"Total Gates: {cur.fetchone()['cnt']:,}")
                cur.execute("SELECT COUNT(*) as cnt FROM vad_gate_fit WHERE status='ok'")
                self.lbl_uvw_count.config(text=f"UVW Solved: {cur.fetchone()['cnt']:,}")
                
                # Time Range Scope
                cur.execute("SELECT MIN(start_time) as t_min, MAX(start_time) as t_max FROM wind_profile_header")
                res = cur.fetchone()
                if res and res['t_min']:
                    self.lbl_db_info.config(text=f"Database Scope: {res['t_min']} to {res['t_max']}")
        except Exception as e:
            logger.error(f"UI Sync Failure: {e}")
        finally:
            if conn: conn.close()
        
        # Trigger redraw of active content
        if self.current_log_view == "header": self.show_header_log()
        elif self.current_log_view == "date_list": self.handle_view_date_selector()

    def show_header_log(self):
        """Header Log query with UVW join count."""
        self.switch_view("header")
        self.group_log.config(text="Data Log: wind_profile_header")
        
        headers = ["Header ID", "Start Time", "Filename", "Gates", "Gate Len (m)", "Proc Runs"]
        widths = [80, 160, 450, 60, 90, 100]
        for i, (h, w) in enumerate(zip(headers, widths)):
            self.tree.heading(f"c{i+1}", text=h); self.tree.column(f"c{i+1}", width=w, anchor="center" if i!=2 else "w")
        
        for i in self.tree.get_children(): self.tree.delete(i)
        
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                sql = """SELECT h.header_id, h.start_time, h.filename, h.num_gates, h.range_gate_length_m, 
                         (SELECT COUNT(DISTINCT run_id) FROM vad_gate_fit f WHERE f.header_id = h.header_id) as proc_run_count
                         FROM wind_profile_header h ORDER BY h.start_time DESC LIMIT 50"""
                cur.execute(sql)
                for r in cur.fetchall():
                    self.tree.insert("", "end", values=(r['header_id'], r['start_time'], r['filename'], r['num_gates'], r['range_gate_length_m'], r['proc_run_count']))
        except Exception as e: logger.error(f"Header query fail: {e}")
        finally:
            if conn: conn.close()

    def show_proc_run_log(self):
        """Detailed Process Batch viewer."""
        self.switch_view("proc")
        self.group_log.config(text="Data Log: proc_run")
        headers = ["Run ID", "Rule Tag", "Started At", "Finished At", "Params JSON", ""]
        widths = [100, 150, 160, 160, 450, 10]
        for i, (h, w) in enumerate(zip(headers, widths)):
            self.tree.heading(f"c{i+1}", text=h); self.tree.column(f"c{i+1}", width=w, anchor="center" if i<4 else "w")
        
        for i in self.tree.get_children(): self.tree.delete(i)
        
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT run_id, rule_tag, started_at, finished_at, params_json FROM proc_run ORDER BY started_at DESC LIMIT 50")
                for r in cur.fetchall():
                    self.tree.insert("", "end", values=(r['run_id'], r['rule_tag'], r['started_at'], r['finished_at'], r['params_json'], ""))
        except Exception as e: logger.error(f"Proc Log Error: {e}")
        finally:
            if conn: conn.close()

    def handle_view_date_selector(self):
        """Queries for distinct observation dates and lists them in [YYYY]-[MM]-[DD] format."""
        self.switch_view("date_list")
        self.group_log.config(text="Observation Date Selection")
        self.date_listbox.delete(0, tk.END)
        
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                sql = """SELECT DISTINCT DATE(h.start_time) as d FROM vad_gate_fit f 
                         JOIN wind_profile_header h ON f.header_id = h.header_id 
                         WHERE f.status = 'ok' ORDER BY d DESC"""
                cur.execute(sql)
                rows = cur.fetchall()
                for r in rows:
                    date_str = r['d'].strftime('%Y-%m-%d')
                    self.date_listbox.insert(tk.END, f" {date_str} ")
            if self.date_listbox.size() == 0: self.date_listbox.insert(tk.END, " (No solved VAD data found) ")
        except Exception as e: logger.error(f"Discovery Error: {e}")
        finally:
            if conn: conn.close()

    def on_date_selected(self, event):
        """Binding for selecting a date. Ensures highlight is seen before transition."""
        sel = self.date_listbox.curselection()
        if not sel: return
        raw = self.date_listbox.get(sel[0]).strip()
        self.root.update_idletasks()
        if len(raw) == 10: 
            self.root.after(100, lambda: self.handle_plot_for_date(raw))

    def handle_plot_for_date(self, date_str):
        """Renders the plot for a specific 24h window in Region 3 area."""
        start_ts, end_ts = f"{date_str} 00:00:00", f"{date_str} 23:59:59"
        m = force_load_module("plot_wind_profile")
        if not m: return
        
        df = m.get_wind_data(start_date=start_ts, end_date=end_ts)
        if df.empty: messagebox.showwarning("No Data", f"No wind data found for {date_str}"); return
        
        self.switch_view("plot")
        self.group_log.config(text=f"Wind Profile Plot: {date_str} (UTC)")
        for w in self.plot_frame.winfo_children(): w.destroy()
        
        try:
            fig = m.create_wind_figure(df, start_ts, end_ts)
            self.canvas_widget = FigureCanvasTkAgg(fig, master=self.plot_frame)
            self.canvas_widget.draw()
            self.toolbar_widget = NavigationToolbar2Tk(self.canvas_widget, self.plot_frame); self.toolbar_widget.update()
            ttk.Button(self.plot_frame, text="⬅ Return to Selection", command=self.handle_view_date_selector).pack(side="top", pady=5)
            self.canvas_widget.get_tk_widget().pack(fill="both", expand=True)
        except Exception as e: messagebox.showerror("Plotting Error", str(e))

    # --- PIPELINE WRAPPERS ---

    def handle_insert(self):
        # [MODIFIED] Strictly load dooplerInsert_v3.py as per user request
        m = force_load_module("dooplerInsert_v3") 
        if not m: 
            messagebox.showerror("Module Error", "Could not find dooplerInsert_v3.py in the current directory.")
            return
        try: 
            m.main()
            self.refresh_db_status()
        except Exception as e: messagebox.showerror("Error", str(e))

    def handle_qc(self):
        m = force_load_module("qc_tagging_v2")
        if not m: return
        try: m.main(); self.refresh_db_status(); messagebox.showinfo("Success", "QC Evaluation Finished.")
        except Exception as e: messagebox.showerror("Error", str(e))

    def handle_uvw(self):
        # Sync with actual filename: wind_profile_uvw_v2.py
        m = force_load_module("wind_profile_uvw_v2")
        if not m: return
        try:
            if hasattr(m, 'main'): m.main(); self.refresh_db_status(); messagebox.showinfo("Success", "VAD Calculations Complete.")
            else: messagebox.showerror("Error", "The UVW script is missing a 'main()' function entry point.")
        except Exception as e: messagebox.showerror("Execution Error", str(e))

    def handle_view_header_log(self): self.show_header_log()
    def handle_view_proc_run(self): self.show_proc_run_log()

    # --- RULE MANAGEMENT & INTERACTIVE TOGGLE ---

    def load_rules(self):
        """Loads and syncs the vad_rule_qc table with row-level visual styling."""
        for item in self.rule_tree.get_children(): self.rule_tree.delete(item)
        conn = None
        try:
            conn = self._get_db_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT rule_id, def_name, rule_code, is_active, rule_order, description FROM vad_rule_qc ORDER BY rule_order")
                for r in cur.fetchall():
                    is_active = r['is_active']
                    status_text = "ACTIVE" if is_active else "INACTIVE"
                    row_tag = 'active_row' if is_active else 'inactive_row'
                    
                    self.rule_tree.insert("", "end", values=(
                        r['rule_id'], r['def_name'], r['rule_code'], status_text, r['rule_order'], r['description']
                    ), tags=(row_tag,))
        except Exception as e: logger.error(f"Rule Loader Error: {e}")
        finally:
            if conn: conn.close()

    def handle_rule_click(self, event):
        """Detects click on any row to switch rule activity status in database."""
        region = self.rule_tree.identify_region(event.x, event.y)
        if region != "cell": return
        item_id = self.rule_tree.identify_row(event.y)
        if not item_id: return
        
        rid = self.rule_tree.item(item_id)['values'][0]
        db = None
        try:
            db = self._get_db_conn()
            with db.cursor() as cur:
                cur.execute("UPDATE vad_rule_qc SET is_active = NOT is_active WHERE rule_id = %s", (rid,))
            db.commit()
            self.load_rules() 
        except Exception as e: messagebox.showerror("Toggle Error", str(e))
        finally:
            if db: db.close()

    def handle_edit_desc(self):
        """Restores modal popup for detailed rule description editing."""
        selection = self.rule_tree.selection()
        if not selection:
            messagebox.showwarning("Selection Required", "Select a rule from the table.")
            return
        
        rid = self.rule_tree.item(selection)['values'][0]
        popup = tk.Toplevel(self.root); popup.title(f"Edit ID {rid}"); popup.geometry("600x450")
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

        tk.Label(popup, text=f"Update Description for Rule {rid}:", font=("Arial", 10, "bold")).pack(pady=10)
        txt = tk.Text(popup, height=12, width=65, wrap="word", padx=10, pady=10)
        txt.insert("1.0", str(curr_desc)); txt.pack(padx=20, pady=10)

        def save():
            val = txt.get("1.0", "end-1c").strip(); db = None
            try:
                db = self._get_db_conn()
                with db.cursor() as cur: cur.execute("UPDATE vad_rule_qc SET description = %s WHERE rule_id = %s", (val, rid))
                db.commit(); messagebox.showinfo("Success", "Updated."); popup.destroy(); self.load_rules()
            except Exception as ex: messagebox.showerror("SQL Error", str(ex))
            finally:
                if db: db.close()
        ttk.Button(popup, text="💾 Save Definition", command=save).pack(pady=15)

if __name__ == "__main__":
    app_root = tk.Tk()
    dashboard = DopplerApp(app_root)
    app_root.mainloop()