import tkinter as tk
from tkinter import ttk, messagebox
import pymysql
import sys
import os
import logging
from pathlib import Path
from tkcalendar import DateEntry

# --- Processing Module Imports ---
# These aliases link the dashboard buttons to your physical .py files
try:
    import dooplerInsert_v3 as importer
    import qc_tagging_v2 as qc
    import wind_profile_uvw_v2 as uvw 
    import plot_wind_profile as plotter # Fixed: Explicit import for Step 4
except ImportError as e:
    # Diagnostic print if a module is missing in the current directory
    print(f"Critical Warning: One or more processing modules failed to import: {e}")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Dashboard")

class DopplerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Doppler Lidar Management Console v3.2")
        
        # --- Window Centering Logic ---
        window_width, window_height = 1200, 850
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        center_x = int(screen_width/2 - window_width / 2)
        center_y = int(screen_height/2 - window_height / 2)
        self.root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
        
        # Database Configuration matching your schema
        self.db_config = {
            "host": "localhost",
            "user": "shengic",
            "password": "sirirat",
            "database": "doopler",
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor
        }
        
        self.create_widgets()
        self.load_rules() # Initialize table with vad_rule_qc data
    def refresh_db_status(self):
        """Detects time range and latest filename using compatible SQL queries"""
        try:
            conn = pymysql.connect(**self.db_config)
            with conn.cursor() as cur:
                # Query 1: Get the absolute timeframe range
                cur.execute("SELECT MIN(start_time) as t_min, MAX(start_time) as t_max FROM wind_profile_header")
                range_res = cur.fetchone()
                
                # Query 2: Get only the filename from the most recent record
                cur.execute("SELECT filename FROM wind_profile_header ORDER BY start_time DESC LIMIT 1")
                file_res = cur.fetchone()
                
                # Update the UI labels
                if range_res and range_res['t_min']:
                    self.lbl_db_info.config(text=f"Available: {range_res['t_min']} to {range_res['t_max']}")
                else:
                    self.lbl_db_info.config(text="Available: No data found in database.")

                if file_res:
                    # Append the filename hint to the label
                    current_text = self.lbl_db_info.cget("text")
                    self.lbl_db_info.config(text=f"{current_text} | Last File: {file_res['filename']}")
                    
            conn.close()
            logger.info("Database status refreshed successfully.")
        except Exception as e:
            logger.error(f"Status check failed: {e}")
            messagebox.showerror("DB Error", f"Could not refresh status: {e}")

    def handle_plot(self):
            """Overwrites your old handle_plot to pass calendar dates"""
            # Formats the calendar date to the MySQL DATETIME format
            start = self.cal_start.get_date().strftime('%Y-%m-%d 00:00:00')
            end = self.cal_end.get_date().strftime('%Y-%m-%d 23:59:59')
            try:
                # Calls the plotter with specific date parameters
                plotter.main(start_date=start, end_date=end) 
                messagebox.showinfo("Visualization", f"Charts generated for: {start} to {end}")
            except Exception as e:
                logger.error(f"Plotting failed: {e}")
                messagebox.showerror("Plotting Error", f"Failed: {e}")

    def create_widgets(self):
        """Constructs the full UI with hierarchy and scrollbars"""
        
        # --- SECTION 1: Step-by-Step Processing Pipeline ---
        group_pipeline = ttk.LabelFrame(self.root, text="Step-by-Step Processing Pipeline")
        group_pipeline.pack(fill="x", padx=25, pady=15)

        # --- NEW SECTION: Plotting Range & Info ---
        group_plot_ctrl = ttk.LabelFrame(self.root, text="4. Plot Configuration")
        group_plot_ctrl.pack(fill="x", padx=25, pady=5)

        # Status Label for available range
        self.lbl_db_info = ttk.Label(group_plot_ctrl, text="Available: Detecting...", font=("Arial", 9, "italic"))
        self.lbl_db_info.pack(side="top", anchor="w", padx=10)

        # Date Pickers
        pick_frame = ttk.Frame(group_plot_ctrl)
        pick_frame.pack(side="top", fill="x", pady=5)

        ttk.Label(pick_frame, text="Start Date:").pack(side="left", padx=5)
        self.cal_start = DateEntry(pick_frame, width=12, background='darkblue', foreground='white', borderwidth=2, date_pattern='yyyy-mm-dd')
        self.cal_start.pack(side="left", padx=5)

        ttk.Label(pick_frame, text="End Date:").pack(side="left", padx=5)
        self.cal_end = DateEntry(pick_frame, width=12, background='darkblue', foreground='white', borderwidth=2, date_pattern='yyyy-mm-dd')
        self.cal_end.pack(side="left", padx=5)

        ttk.Button(pick_frame, text="🔄 Check DB", command=self.refresh_db_status).pack(side="left", padx=10)
        
        btn_layout = {"side": "left", "padx": 10, "pady": 20, "expand": True, "fill": "x"}
        
        ttk.Button(group_pipeline, text="1. Import HPL Files", 
                   command=self.handle_insert).pack(**btn_layout)
        
        ttk.Button(group_pipeline, text="2. Run Quality Control (QC)", 
                   command=self.handle_qc).pack(**btn_layout)
        
        ttk.Button(group_pipeline, text="3. Calculate UVW Wind", 
                   command=self.handle_uvw).pack(**btn_layout)
        
        ttk.Button(group_pipeline, text="4. Plot Wind Profile", 
                   command=self.handle_plot).pack(**btn_layout)

        # --- SECTION 2: QC Rule Configuration (vad_rule_qc) ---
        group_rules = ttk.LabelFrame(self.root, text="QC Rule Configuration (vad_rule_qc)")
        group_rules.pack(fill="both", expand=True, padx=25, pady=10)
        
        # Container for Treeview and Dual Scrollbars
        tree_frame = ttk.Frame(group_rules)
        tree_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Table Schema Mapping
        columns = ("id", "def_name", "rule_code", "active", "order", "desc")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')
        
        # Headings
        self.tree.heading("id", text="Rule ID")
        self.tree.heading("def_name", text="Definition Name")
        self.tree.heading("rule_code", text="Rule Code")
        self.tree.heading("active", text="Status")
        self.tree.heading("order", text="Process Order")
        self.tree.heading("desc", text="Description / Physics Formula")
        
        # Formatting for readability and long text
        self.tree.column("id", width=80, anchor="center")
        self.tree.column("def_name", width=180)
        self.tree.column("rule_code", width=150)
        self.tree.column("active", width=100, anchor="center")
        self.tree.column("order", width=110, anchor="center")
        self.tree.column("desc", width=950) 

        # Scrollbar Logic
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        self.tree.grid(column=0, row=0, sticky='nsew')
        vsb.grid(column=1, row=0, sticky='ns')
        hsb.grid(column=0, row=1, sticky='ew')
        
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(0, weight=1)

        # Rule Management Toolbar
        btn_rule_grp = ttk.Frame(group_rules)
        btn_rule_grp.pack(fill="x", padx=10, pady=5)
        
        ttk.Button(btn_rule_grp, text="Refresh Rules from DB", 
                   command=self.load_rules).pack(side="left", padx=5)
        
        ttk.Button(btn_rule_grp, text="Edit Selected Description", 
                   command=self.handle_edit_desc).pack(side="left", padx=5)

        # --- SECTION 3: System Maintenance / Danger Zone ---
        # Includes safety warning label and ID reset button
        group_maint = ttk.LabelFrame(self.root, text="System Maintenance / Danger Zone")
        group_maint.pack(fill="x", padx=25, pady=15)
        
        ttk.Label(group_maint, text="CRITICAL: Resetting will PERMANENTLY clear observation data and reset all ID counters.", 
                  foreground="#d9534f", font=("Arial", 9, "bold")).pack(side="left", padx=15)
        
        ttk.Button(group_maint, text="⚠️ SYSTEM RESET (ID=1)", 
                   command=self.handle_reset).pack(side="right", padx=15, pady=10)

    # --- PIPELINE ACTION HANDLERS ---

    def handle_insert(self):
        """Triggers the raw HPL data ingestion"""
        try:
            importer.main()
            messagebox.showinfo("Import Progress", "Data import completed successfully.")
        except Exception as e:
            logger.error(f"Import failed: {e}")
            messagebox.showerror("Execution Error", f"Import script failed: {e}")

    def handle_qc(self):
        """Triggers the QC tagging logic"""
        try:
            qc.main()
            messagebox.showinfo("QC Status", "Quality Control analysis is finished.")
            self.load_rules()
        except Exception as e:
            logger.error(f"QC failed: {e}")
            messagebox.showerror("Execution Error", f"QC analysis failed: {e}")

    def handle_uvw(self):
        """Triggers VAD retrieval calculations"""
        try:
            uvw.main()
            messagebox.showinfo("VAD Progress", "UVW Wind calculation and storage complete.")
        except Exception as e:
            logger.error(f"UVW failed: {e}")
            messagebox.showerror("Execution Error", f"UVW calculation failed: {e}")

    # def handle_plot(self):
    #     """Triggers the visualization module"""
    #     try:
    #         plotter.main() # Alias defined at top of script
    #         messagebox.showinfo("Visualization", "Wind profile charts generated.")
    #     except Exception as e:
    #         logger.error(f"Plotting failed: {e}")
    #         messagebox.showerror("Plotting Error", f"Failed to generate plots: {e}")

    def handle_reset(self):
        """Safety confirmation before database wipe"""
        if messagebox.askyesno("Confirm Wipe", "Are you sure you want to delete ALL data and reset IDs to 1?"):
            messagebox.showwarning("Direct Access", "Please execute 'python dooplerReset.py' in a shell for safety.")

    # --- DATABASE OPERATIONS ---

    def load_rules(self):
        """Syncs the UI table with MySQL 'vad_rule_qc'"""
        for item in self.tree.get_children():
            self.tree.delete(item)
            
        try:
            conn = pymysql.connect(**self.db_config)
            with conn.cursor() as cur:
                sql = "SELECT rule_id, def_name, rule_code, is_active, rule_order, description FROM vad_rule_qc ORDER BY rule_order"
                cur.execute(sql)
                for row in cur.fetchall():
                    status = "Active" if row['is_active'] else "Inactive"
                    self.tree.insert("", "end", values=(
                        row['rule_id'], row['def_name'], row['rule_code'], 
                        status, row['rule_order'], row['description']
                    ))
            conn.close()
        except Exception as e:
            messagebox.showerror("DB Error", f"Failed to sync with database: {e}")

    def handle_edit_desc(self):
        """Opens a modal popup to edit description strings"""
        selection = self.tree.selection()
        if not selection:
            messagebox.showwarning("Target Missing", "Select a rule row to modify.")
            return
        
        row_data = self.tree.item(selection)
        r_id, d_name, r_code, status, order, current_desc = row_data['values']

        edit_popup = tk.Toplevel(self.root)
        edit_popup.title(f"Editing Description: {d_name}")
        edit_popup.geometry("700x500")
        edit_popup.transient(self.root)
        edit_popup.grab_set()

        tk.Label(edit_popup, text=f"Update Rule ID {r_id}:", font=("Arial", 11, "bold")).pack(pady=20)
        desc_box = tk.Text(edit_popup, height=15, width=75, wrap="word", padx=15, pady=15)
        desc_box.insert("1.0", str(current_desc))
        desc_box.pack(padx=25, pady=10)

        def save_changes():
            updated_desc = desc_box.get("1.0", "end-1c").strip()
            try:
                db = pymysql.connect(**self.db_config)
                with db.cursor() as cursor:
                    sql = "UPDATE vad_rule_qc SET description = %s WHERE rule_id = %s"
                    cursor.execute(sql, (updated_desc, r_id))
                db.commit()
                db.close()
                messagebox.showinfo("Success", "Rule description saved."); edit_popup.destroy(); self.load_rules()
            except Exception as db_err:
                messagebox.showerror("SQL Error", f"Database update failed: {db_err}")

        ttk.Button(edit_popup, text="💾 Save to Database", command=save_changes).pack(pady=25)

# --- APPLICATION ENTRY POINT ---
if __name__ == "__main__":
    app_root = tk.Tk()
    dashboard = DopplerApp(app_root)
    app_root.mainloop()