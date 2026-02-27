"""
Import data from WFM_Report_H1 & Q3.xlsx into SQLite database.
All data stays private in the local database - not exposed to public.
"""

import sqlite3
import openpyxl
import os
import datetime

EXCEL_PATH = "/Users/harshjeewani/Library/CloudStorage/OneDrive-RateGainTravelTechnologies/WFM_Report_H1 & Q3.xlsx"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wfm_data.db")


def safe_float(val):
    if val is None or val == '-' or val == ' ' or val == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_str(val):
    if val is None:
        return None
    return str(val).strip()


def safe_date(val):
    if val is None or val == '-' or val == ' ':
        return None
    if isinstance(val, datetime.datetime):
        return val.strftime('%Y-%m-%d')
    return str(val)


def create_tables(conn):
    c = conn.cursor()

    # Cost Summary
    c.execute('''CREATE TABLE IF NOT EXISTS cost_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        serial_no INTEGER,
        team_manager TEXT,
        product_team TEXT,
        leader TEXT,
        num_employees_q2 INTEGER,
        num_employees_q3 INTEGER,
        cumulative_cost_q3 REAL,
        cumulative_cost_h1 REAL,
        cumulative_cost_q2 REAL,
        cumulative_cost_q1 REAL,
        cost_per_emp_q3 REAL,
        cost_per_emp_q2 REAL,
        remarks TEXT
    )''')

    # DA&DK Pivot - CTC
    c.execute('''CREATE TABLE IF NOT EXISTS dadk_ctc (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        row_label TEXT,
        apr_25 REAL, may_25 REAL, jun_25 REAL, jul_25 REAL,
        aug_25 REAL, sep_25 REAL, oct_25 REAL, nov_25 REAL,
        dec_25 REAL, jan_26 REAL, feb_26 REAL, mar_26 REAL,
        variance REAL
    )''')

    # DA&DK Pivot - Head Count
    c.execute('''CREATE TABLE IF NOT EXISTS dadk_headcount (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        row_label TEXT,
        apr_25 INTEGER, may_25 INTEGER, jun_25 INTEGER, jul_25 INTEGER,
        aug_25 INTEGER, sep_25 INTEGER, oct_25 INTEGER, nov_25 INTEGER,
        dec_25 INTEGER, jan_26 INTEGER, feb_26 INTEGER, mar_26 INTEGER
    )''')

    # DA&DK Pivot - New Joiners
    c.execute('''CREATE TABLE IF NOT EXISTS dadk_new_joiners (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        designation TEXT,
        headcount INTEGER
    )''')

    # Productivity Emp
    c.execute('''CREATE TABLE IF NOT EXISTS productivity_emp (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        emp_id TEXT,
        employee_name TEXT,
        joining_date TEXT,
        designation TEXT,
        tenure TEXT,
        shift_role TEXT,
        team_name TEXT,
        manager_name TEXT,
        team_lead_by TEXT,
        q1_performance REAL,
        q2_performance REAL,
        q3_performance REAL,
        exit_type TEXT,
        last_working_day TEXT,
        comment TEXT,
        q3_ctc REAL,
        status TEXT,
        date_of_exit TEXT,
        status_q_wise TEXT,
        take TEXT
    )''')

    # Adara DevOps
    c.execute('''CREATE TABLE IF NOT EXISTS adara_devops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        emp_id TEXT,
        employee_name TEXT,
        joining_date TEXT,
        tenure TEXT,
        designation TEXT,
        location TEXT,
        utilisation REAL,
        comments TEXT
    )''')

    # DevOps Shared Services - Uptime
    c.execute('''CREATE TABLE IF NOT EXISTS devops_uptime (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        system_product TEXT,
        availability_level REAL,
        total_downtime REAL,
        downtime_per_year_hrs REAL,
        downtime_per_quarter_hrs REAL,
        downtime_per_month_hrs REAL
    )''')

    # DevOps Shared Services - Tickets
    c.execute('''CREATE TABLE IF NOT EXISTS devops_tickets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        system_product TEXT,
        availability_level REAL,
        total_downtime REAL,
        downtime_per_year_hrs REAL,
        est_tickets_per_year REAL,
        est_tickets_per_quarter REAL,
        est_tickets_per_month REAL,
        est_tickets_per_week REAL
    )''')

    # IT HelpDesk
    c.execute('''CREATE TABLE IF NOT EXISTS it_helpdesk (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT,
        num_tickets INTEGER,
        tickets_per_engineer REAL,
        num_emp REAL
    )''')

    # IT HelpDesk Notes
    c.execute('''CREATE TABLE IF NOT EXISTS it_helpdesk_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        note TEXT
    )''')

    # SoHo Monitoring
    c.execute('''CREATE TABLE IF NOT EXISTS soho_monitoring (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        monitor TEXT,
        yearmonth TEXT,
        index_score REAL,
        handling_time_score REAL,
        response_rate_score REAL
    )''')

    # SoHo Client Success
    c.execute('''CREATE TABLE IF NOT EXISTS soho_client_success (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cs_team TEXT,
        high_risk_pct REAL,
        high_risk_clients REAL,
        recent_escalations REAL,
        total_cancelations REAL,
        nps_bcv_score REAL,
        nps_team_score REAL,
        avg_client_age REAL,
        upsells REAL,
        total_score REAL,
        average REAL,
        target REAL
    )''')

    # SoHo Social Content Strategy
    c.execute('''CREATE TABLE IF NOT EXISTS soho_social_content (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_member TEXT,
        escalations REAL,
        nps_score REAL,
        index_score REAL,
        average REAL,
        target REAL
    )''')

    # Customer Experience
    c.execute('''CREATE TABLE IF NOT EXISTS customer_experience (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        serial_no INTEGER,
        emp_id INTEGER,
        name TEXT,
        art_l1_hrs REAL,
        reopen_pct REAL,
        nps REAL,
        csat REAL,
        quality REAL,
        productivity_w1 REAL,
        productivity_w2 REAL,
        productivity_w3 REAL,
        productivity_w4 REAL,
        productivity_w5 REAL,
        total_tickets REAL,
        avg_daily_tickets REAL,
        working_days REAL
    )''')

    # Customer Experience Notes
    c.execute('''CREATE TABLE IF NOT EXISTS customer_experience_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        note TEXT
    )''')

    # TA Team - Open Positions
    c.execute('''CREATE TABLE IF NOT EXISTS ta_open_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT,
        new_count INTEGER,
        replacement_count INTEGER,
        new_pct REAL,
        replacement_pct REAL,
        offered_new_pct REAL,
        offered_replacement_pct REAL,
        offered_from_closing INTEGER
    )''')

    # TA Team - Leader Positions
    c.execute('''CREATE TABLE IF NOT EXISTS ta_leader_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        leader_name TEXT,
        open_positions INTEGER
    )''')

    # TA Team - Partner Performance
    c.execute('''CREATE TABLE IF NOT EXISTS ta_partner_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ta_partner TEXT,
        offered INTEGER,
        joinings INTEGER,
        avg_time_to_offer INTEGER
    )''')

    conn.commit()


def import_cost_summary(wb, conn):
    ws = wb["Cost Summary"]
    c = conn.cursor()
    c.execute("DELETE FROM cost_summary")

    for row in range(3, 23):  # rows 3-22
        serial_no = ws.cell(row=row, column=1).value
        if serial_no is None:
            continue
        c.execute('''INSERT INTO cost_summary
            (serial_no, team_manager, product_team, leader, num_employees_q2, num_employees_q3,
             cumulative_cost_q3, cumulative_cost_h1, cumulative_cost_q2, cumulative_cost_q1,
             cost_per_emp_q3, cost_per_emp_q2, remarks)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            serial_no,
            safe_str(ws.cell(row=row, column=2).value),
            safe_str(ws.cell(row=row, column=3).value),
            safe_str(ws.cell(row=row, column=4).value),
            ws.cell(row=row, column=5).value,
            ws.cell(row=row, column=6).value,
            safe_float(ws.cell(row=row, column=7).value),
            safe_float(ws.cell(row=row, column=8).value),
            safe_float(ws.cell(row=row, column=9).value),
            safe_float(ws.cell(row=row, column=10).value),
            safe_float(ws.cell(row=row, column=11).value),
            safe_float(ws.cell(row=row, column=12).value),
            safe_str(ws.cell(row=row, column=13).value)
        ))
    conn.commit()
    print(f"  Cost Summary: {c.rowcount} rows imported")


def import_dadk_pivot(wb, conn):
    ws = wb["DA&DK_Pivot"]
    c = conn.cursor()

    # CTC rows 5-7
    c.execute("DELETE FROM dadk_ctc")
    for row in range(5, 8):
        label = safe_str(ws.cell(row=row, column=1).value)
        if label is None:
            continue
        c.execute('''INSERT INTO dadk_ctc
            (row_label, apr_25, may_25, jun_25, jul_25, aug_25, sep_25,
             oct_25, nov_25, dec_25, jan_26, feb_26, mar_26, variance)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            label,
            safe_float(ws.cell(row=row, column=2).value),
            safe_float(ws.cell(row=row, column=3).value),
            safe_float(ws.cell(row=row, column=4).value),
            safe_float(ws.cell(row=row, column=5).value),
            safe_float(ws.cell(row=row, column=6).value),
            safe_float(ws.cell(row=row, column=7).value),
            safe_float(ws.cell(row=row, column=8).value),
            safe_float(ws.cell(row=row, column=9).value),
            safe_float(ws.cell(row=row, column=10).value),
            safe_float(ws.cell(row=row, column=11).value),
            safe_float(ws.cell(row=row, column=12).value),
            safe_float(ws.cell(row=row, column=13).value),
            safe_float(ws.cell(row=row, column=14).value),
        ))

    # Head Count rows 11-13
    c.execute("DELETE FROM dadk_headcount")
    for row in range(11, 14):
        label = safe_str(ws.cell(row=row, column=1).value)
        if label is None:
            continue
        c.execute('''INSERT INTO dadk_headcount
            (row_label, apr_25, may_25, jun_25, jul_25, aug_25, sep_25,
             oct_25, nov_25, dec_25, jan_26, feb_26, mar_26)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            label,
            ws.cell(row=row, column=2).value,
            ws.cell(row=row, column=3).value,
            ws.cell(row=row, column=4).value,
            ws.cell(row=row, column=5).value,
            ws.cell(row=row, column=6).value,
            ws.cell(row=row, column=7).value,
            ws.cell(row=row, column=8).value,
            ws.cell(row=row, column=9).value,
            ws.cell(row=row, column=10).value,
            ws.cell(row=row, column=11).value,
            ws.cell(row=row, column=12).value,
            ws.cell(row=row, column=13).value,
        ))

    # New Joiners rows 19-29
    c.execute("DELETE FROM dadk_new_joiners")
    for row in range(19, 31):
        designation = safe_str(ws.cell(row=row, column=3).value)
        hc = ws.cell(row=row, column=4).value
        if designation and hc is not None:
            c.execute("INSERT INTO dadk_new_joiners (designation, headcount) VALUES (?,?)",
                      (designation, hc))

    conn.commit()
    print("  DA&DK Pivot: imported")


def import_productivity_emp(wb, conn):
    ws = wb["Productivity_Emp"]
    c = conn.cursor()
    c.execute("DELETE FROM productivity_emp")

    count = 0
    for row in range(2, ws.max_row + 1):
        emp_id = ws.cell(row=row, column=1).value
        if emp_id is None:
            continue
        c.execute('''INSERT INTO productivity_emp
            (emp_id, employee_name, joining_date, designation, tenure, shift_role,
             team_name, manager_name, team_lead_by, q1_performance, q2_performance,
             q3_performance, exit_type, last_working_day, comment, q3_ctc, status,
             date_of_exit, status_q_wise, take)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            safe_str(emp_id),
            safe_str(ws.cell(row=row, column=2).value),
            safe_date(ws.cell(row=row, column=3).value),
            safe_str(ws.cell(row=row, column=4).value),
            safe_str(ws.cell(row=row, column=5).value),
            safe_str(ws.cell(row=row, column=6).value),
            safe_str(ws.cell(row=row, column=7).value),
            safe_str(ws.cell(row=row, column=8).value),
            safe_str(ws.cell(row=row, column=9).value),
            safe_float(ws.cell(row=row, column=11).value),
            safe_float(ws.cell(row=row, column=12).value),
            safe_float(ws.cell(row=row, column=13).value),
            safe_str(ws.cell(row=row, column=14).value),
            safe_date(ws.cell(row=row, column=15).value),
            safe_str(ws.cell(row=row, column=16).value),
            safe_float(ws.cell(row=row, column=17).value),
            safe_str(ws.cell(row=row, column=18).value),
            safe_date(ws.cell(row=row, column=19).value),
            safe_str(ws.cell(row=row, column=20).value),
            safe_str(ws.cell(row=row, column=21).value),
        ))
        count += 1
    conn.commit()
    print(f"  Productivity_Emp: {count} rows imported")


def import_adara_devops(wb, conn):
    ws = wb["Adara-Devops"]
    c = conn.cursor()
    c.execute("DELETE FROM adara_devops")

    count = 0
    for row in range(3, ws.max_row + 1):
        emp_id = ws.cell(row=row, column=1).value
        if emp_id is None:
            continue
        c.execute('''INSERT INTO adara_devops
            (emp_id, employee_name, joining_date, tenure, designation, location,
             utilisation, comments)
            VALUES (?,?,?,?,?,?,?,?)''', (
            safe_str(emp_id),
            safe_str(ws.cell(row=row, column=2).value),
            safe_date(ws.cell(row=row, column=3).value),
            safe_str(ws.cell(row=row, column=4).value),
            safe_str(ws.cell(row=row, column=5).value),
            safe_str(ws.cell(row=row, column=6).value),
            safe_float(ws.cell(row=row, column=7).value),
            safe_str(ws.cell(row=row, column=8).value),
        ))
        count += 1
    conn.commit()
    print(f"  Adara-Devops: {count} rows imported")


def import_devops_shared(wb, conn):
    ws = wb["DevOps – Shared Services"]
    c = conn.cursor()

    # Uptime rows 6-13
    c.execute("DELETE FROM devops_uptime")
    for row in range(6, 14):
        product = safe_str(ws.cell(row=row, column=1).value)
        if product is None:
            continue
        c.execute('''INSERT INTO devops_uptime
            (system_product, availability_level, total_downtime,
             downtime_per_year_hrs, downtime_per_quarter_hrs, downtime_per_month_hrs)
            VALUES (?,?,?,?,?,?)''', (
            product,
            safe_float(ws.cell(row=row, column=2).value),
            safe_float(ws.cell(row=row, column=3).value),
            safe_float(ws.cell(row=row, column=4).value),
            safe_float(ws.cell(row=row, column=5).value),
            safe_float(ws.cell(row=row, column=6).value),
        ))

    # Tickets rows 38-47
    c.execute("DELETE FROM devops_tickets")
    for row in range(38, 48):
        product = safe_str(ws.cell(row=row, column=1).value)
        if product is None and ws.cell(row=row, column=5).value is None:
            continue
        c.execute('''INSERT INTO devops_tickets
            (system_product, availability_level, total_downtime,
             downtime_per_year_hrs, est_tickets_per_year, est_tickets_per_quarter,
             est_tickets_per_month, est_tickets_per_week)
            VALUES (?,?,?,?,?,?,?,?)''', (
            product,
            safe_float(ws.cell(row=row, column=2).value),
            safe_float(ws.cell(row=row, column=3).value),
            safe_float(ws.cell(row=row, column=4).value),
            safe_float(ws.cell(row=row, column=5).value),
            safe_float(ws.cell(row=row, column=6).value),
            safe_float(ws.cell(row=row, column=7).value),
            safe_float(ws.cell(row=row, column=8).value),
        ))

    conn.commit()
    print("  DevOps Shared Services: imported")


def import_it_helpdesk(wb, conn):
    ws = wb["IT HelpDesk"]
    c = conn.cursor()
    c.execute("DELETE FROM it_helpdesk")
    c.execute("DELETE FROM it_helpdesk_notes")

    count = 0
    for row in range(2, 20):  # rows 2-19
        month = safe_str(ws.cell(row=row, column=1).value)
        if month is None:
            continue
        c.execute('''INSERT INTO it_helpdesk
            (month, num_tickets, tickets_per_engineer, num_emp)
            VALUES (?,?,?,?)''', (
            month,
            ws.cell(row=row, column=2).value,
            safe_float(ws.cell(row=row, column=3).value),
            safe_float(ws.cell(row=row, column=4).value),
        ))
        count += 1

    # Notes
    for row in [37, 38, 39]:
        note = safe_str(ws.cell(row=row, column=2).value)
        if note:
            c.execute("INSERT INTO it_helpdesk_notes (note) VALUES (?)", (note,))

    conn.commit()
    print(f"  IT HelpDesk: {count} rows imported")


def import_soho(wb, conn):
    ws = wb["SoHo Team"]
    c = conn.cursor()
    c.execute("DELETE FROM soho_monitoring")
    c.execute("DELETE FROM soho_client_success")
    c.execute("DELETE FROM soho_social_content")

    # Monitoring data (cols A-E)
    count = 0
    for row in range(3, 27):
        monitor = safe_str(ws.cell(row=row, column=1).value)
        if monitor is None:
            continue
        c.execute('''INSERT INTO soho_monitoring
            (monitor, yearmonth, index_score, handling_time_score, response_rate_score)
            VALUES (?,?,?,?,?)''', (
            monitor,
            safe_str(ws.cell(row=row, column=2).value),
            safe_float(ws.cell(row=row, column=3).value),
            safe_float(ws.cell(row=row, column=4).value),
            safe_float(ws.cell(row=row, column=5).value),
        ))
        count += 1

    # Client Success (cols G-R, rows 3-8)
    for row in range(3, 9):
        team = safe_str(ws.cell(row=row, column=7).value)
        if team is None:
            continue
        c.execute('''INSERT INTO soho_client_success
            (cs_team, high_risk_pct, high_risk_clients, recent_escalations,
             total_cancelations, nps_bcv_score, nps_team_score, avg_client_age,
             upsells, total_score, average, target)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (
            team,
            safe_float(ws.cell(row=row, column=8).value),
            safe_float(ws.cell(row=row, column=9).value),
            safe_float(ws.cell(row=row, column=10).value),
            safe_float(ws.cell(row=row, column=11).value),
            safe_float(ws.cell(row=row, column=12).value),
            safe_float(ws.cell(row=row, column=13).value),
            safe_float(ws.cell(row=row, column=14).value),
            safe_float(ws.cell(row=row, column=15).value),
            safe_float(ws.cell(row=row, column=16).value),
            safe_float(ws.cell(row=row, column=17).value),
            safe_float(ws.cell(row=row, column=18).value),
        ))

    # Social Content Strategy (rows 15-21)
    for row in range(15, 22):
        team_member = safe_str(ws.cell(row=row, column=7).value)
        if team_member is None:
            continue
        c.execute('''INSERT INTO soho_social_content
            (team_member, escalations, nps_score, index_score, average, target)
            VALUES (?,?,?,?,?,?)''', (
            team_member,
            safe_float(ws.cell(row=row, column=8).value),
            safe_float(ws.cell(row=row, column=9).value),
            safe_float(ws.cell(row=row, column=10).value),
            safe_float(ws.cell(row=row, column=11).value),
            safe_float(ws.cell(row=row, column=12).value),
        ))

    conn.commit()
    print(f"  SoHo Team: {count} monitoring rows imported")


def import_customer_experience(wb, conn):
    ws = wb["Customer Experience - Tushar"]
    c = conn.cursor()
    c.execute("DELETE FROM customer_experience")
    c.execute("DELETE FROM customer_experience_notes")

    count = 0
    for row in range(2, 12):
        serial_no = ws.cell(row=row, column=1).value
        if serial_no is None or not isinstance(serial_no, (int, float)):
            continue
        c.execute('''INSERT INTO customer_experience
            (serial_no, emp_id, name, art_l1_hrs, reopen_pct, nps, csat, quality,
             productivity_w1, productivity_w2, productivity_w3, productivity_w4,
             productivity_w5, total_tickets, avg_daily_tickets, working_days)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            int(serial_no),
            ws.cell(row=row, column=2).value,
            safe_str(ws.cell(row=row, column=3).value),
            safe_float(ws.cell(row=row, column=4).value),
            safe_float(ws.cell(row=row, column=5).value),
            safe_float(ws.cell(row=row, column=6).value),
            safe_float(ws.cell(row=row, column=7).value),
            safe_float(ws.cell(row=row, column=8).value),
            safe_float(ws.cell(row=row, column=9).value),
            safe_float(ws.cell(row=row, column=10).value),
            safe_float(ws.cell(row=row, column=11).value),
            safe_float(ws.cell(row=row, column=12).value),
            safe_float(ws.cell(row=row, column=13).value),
            safe_float(ws.cell(row=row, column=14).value),
            safe_float(ws.cell(row=row, column=15).value),
            safe_float(ws.cell(row=row, column=16).value),
        ))
        count += 1

    # Notes
    for row in [13, 14]:
        note = safe_str(ws.cell(row=row, column=1).value)
        if note:
            c.execute("INSERT INTO customer_experience_notes (note) VALUES (?)", (note,))

    conn.commit()
    print(f"  Customer Experience: {count} rows imported")


def import_ta_team(conn):
    """Import TA Team data (from visual snapshot — Excel tab is empty)."""
    c = conn.cursor()
    c.execute("DELETE FROM ta_open_positions")
    c.execute("DELETE FROM ta_leader_positions")
    c.execute("DELETE FROM ta_partner_performance")

    # Open Positions by Month
    positions_data = [
        ("Oct'25", 59, 25, 70, 30, 28, 72, 17),
        ("Nov'25", 49, 31, 61, 39, 33, 67, 45),
        ("Dec'25", 58, 41, 59, 41, 42, 58, 31),
    ]
    for row in positions_data:
        c.execute('''INSERT INTO ta_open_positions
            (month, new_count, replacement_count, new_pct, replacement_pct,
             offered_new_pct, offered_replacement_pct, offered_from_closing)
            VALUES (?,?,?,?,?,?,?,?)''', row)

    # Leader Wise Open Positions
    leader_data = [
        ("Jay Roger Wardle", 5), ("Bhanu Chopra", 11), ("Rohan Mittal", 3),
        ("Deepak Kapoor", 14), ("Ashish Sikka", 8), ("Yogeesh Chandra", 5),
        ("Sachin Garg", 7), ("Anurag Jain", 2), ("Fiza Malick", 23),
        ("Toby Marich", 3), ("Vinay Varma", 5), ("Mark K Rabe", 3),
        ("Carla Shaw", 2), ("Sahil Sharma", 10), ("Pankaj Tiwari", 1),
    ]
    for name, positions in leader_data:
        c.execute("INSERT INTO ta_leader_positions (leader_name, open_positions) VALUES (?,?)",
                  (name, positions))

    # TA Partner Performance
    partner_data = [
        ("Shruti Sinha", 40, 36, 37), ("Prateek Panjwani", 34, 27, 27),
        ("Anjali Sharma", 32, 31, 29), ("Karnika Daniel", 28, 15, 45),
        ("Abha Chhabra", 21, 15, 25), ("Roshni Das Jad", 16, 13, 32),
        ("Vyas Ahuja", 16, 12, 42), ("Manpreet Anand", 13, 11, 29),
        ("Kanika Kaushal", 9, 4, 40), ("Bridget Pederson", 4, 0, 51),
    ]
    for row in partner_data:
        c.execute('''INSERT INTO ta_partner_performance
            (ta_partner, offered, joinings, avg_time_to_offer)
            VALUES (?,?,?,?)''', row)

    conn.commit()
    print(f"  TA Team: {len(positions_data)} position rows, {len(leader_data)} leaders, {len(partner_data)} partners imported")


def main():
    print(f"Loading Excel from: {EXCEL_PATH}")
    wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True)

    print(f"Creating database at: {DB_PATH}")
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    print("\nImporting data...")
    import_cost_summary(wb, conn)
    import_dadk_pivot(wb, conn)
    import_productivity_emp(wb, conn)
    import_adara_devops(wb, conn)
    import_devops_shared(wb, conn)
    import_it_helpdesk(wb, conn)
    import_soho(wb, conn)
    import_customer_experience(wb, conn)
    import_ta_team(conn)

    conn.close()
    print("\n✅ Data import complete! Database saved to:", DB_PATH)


if __name__ == "__main__":
    main()
