"""
WFM Report Dashboard - RateGain
Secure, interactive dashboard for CEO-level productivity analysis.
Data is stored in local SQLite database - not exposed publicly.
"""

import sqlite3
import os
import json
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wfm_data.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def index():
    return render_template("dashboard.html")


# ─── Cost Summary ───
@app.route("/api/cost_summary")
def api_cost_summary():
    conn = get_db()
    rows = conn.execute("SELECT * FROM cost_summary ORDER BY serial_no").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


# ─── DA&DK Pivot ───
@app.route("/api/dadk_ctc")
def api_dadk_ctc():
    conn = get_db()
    rows = conn.execute("SELECT * FROM dadk_ctc").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/dadk_headcount")
def api_dadk_headcount():
    conn = get_db()
    rows = conn.execute("SELECT * FROM dadk_headcount").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/dadk_new_joiners")
def api_dadk_new_joiners():
    conn = get_db()
    rows = conn.execute("SELECT * FROM dadk_new_joiners").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


# ─── Productivity Emp ───
@app.route("/api/productivity_emp")
def api_productivity_emp():
    conn = get_db()
    team = request.args.get("team", "")
    status_filter = request.args.get("status", "")
    manager = request.args.get("manager", "")

    query = "SELECT * FROM productivity_emp WHERE 1=1"
    params = []
    if team:
        query += " AND team_name = ?"
        params.append(team)
    if status_filter:
        query += " AND status = ?"
        params.append(status_filter)
    if manager:
        query += " AND team_lead_by = ?"
        params.append(manager)
    query += " ORDER BY team_name, employee_name"

    rows = conn.execute(query, params).fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/productivity_teams")
def api_productivity_teams():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT team_name FROM productivity_emp WHERE team_name IS NOT NULL ORDER BY team_name").fetchall()
    data = [r["team_name"] for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/productivity_managers")
def api_productivity_managers():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT team_lead_by FROM productivity_emp WHERE team_lead_by IS NOT NULL ORDER BY team_lead_by").fetchall()
    data = [r["team_lead_by"] for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/productivity_grouped")
def api_productivity_grouped():
    """Return productivity data grouped by manager with summary + employee details."""
    conn = get_db()
    team = request.args.get("team", "")
    status_filter = request.args.get("status", "")
    manager = request.args.get("manager", "")

    query = "SELECT * FROM productivity_emp WHERE team_lead_by IS NOT NULL"
    params = []
    if team:
        query += " AND team_name = ?"
        params.append(team)
    if status_filter:
        query += " AND status = ?"
        params.append(status_filter)
    if manager:
        query += " AND team_lead_by = ?"
        params.append(manager)
    query += " ORDER BY team_lead_by, employee_name"

    rows = conn.execute(query, params).fetchall()
    all_data = [dict(r) for r in rows]
    conn.close()

    # Group by manager
    managers = {}
    for emp in all_data:
        mgr = emp["team_lead_by"]
        if mgr not in managers:
            managers[mgr] = {
                "manager": mgr,
                "team_name": emp.get("team_name", ""),
                "employees": [],
                "total": 0,
                "active": 0,
                "inactive": 0,
                "q1_scores": [],
                "q2_scores": [],
                "q3_scores": [],
            }
        managers[mgr]["employees"].append(emp)
        managers[mgr]["total"] += 1
        if emp.get("status") == "Active":
            managers[mgr]["active"] += 1
        else:
            managers[mgr]["inactive"] += 1
        if emp.get("q1_performance"):
            managers[mgr]["q1_scores"].append(emp["q1_performance"])
        if emp.get("q2_performance"):
            managers[mgr]["q2_scores"].append(emp["q2_performance"])
        if emp.get("q3_performance"):
            managers[mgr]["q3_scores"].append(emp["q3_performance"])

    # Build summary for each manager
    result = []
    for mgr_name, info in managers.items():
        avg_q1 = round(sum(info["q1_scores"]) / len(info["q1_scores"]), 2) if info["q1_scores"] else None
        avg_q2 = round(sum(info["q2_scores"]) / len(info["q2_scores"]), 2) if info["q2_scores"] else None
        avg_q3 = round(sum(info["q3_scores"]) / len(info["q3_scores"]), 2) if info["q3_scores"] else None
        result.append({
            "manager": mgr_name,
            "team_name": info["team_name"],
            "total": info["total"],
            "active": info["active"],
            "inactive": info["inactive"],
            "avg_q1": avg_q1,
            "avg_q2": avg_q2,
            "avg_q3": avg_q3,
            "employees": info["employees"],
        })

    result.sort(key=lambda x: x["manager"])
    return jsonify(result)


# ─── Adara DevOps ───
@app.route("/api/adara_devops")
def api_adara_devops():
    conn = get_db()
    location = request.args.get("location", "")
    query = "SELECT * FROM adara_devops WHERE 1=1"
    params = []
    if location:
        query += " AND location = ?"
        params.append(location)
    query += " ORDER BY employee_name"
    rows = conn.execute(query, params).fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


# ─── DevOps Shared Services ───
@app.route("/api/devops_uptime")
def api_devops_uptime():
    conn = get_db()
    rows = conn.execute("SELECT * FROM devops_uptime").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/devops_tickets")
def api_devops_tickets():
    conn = get_db()
    rows = conn.execute("SELECT * FROM devops_tickets").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


# ─── IT HelpDesk ───
@app.route("/api/it_helpdesk")
def api_it_helpdesk():
    conn = get_db()
    rows = conn.execute("SELECT * FROM it_helpdesk ORDER BY id").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/it_helpdesk_notes")
def api_it_helpdesk_notes():
    conn = get_db()
    rows = conn.execute("SELECT * FROM it_helpdesk_notes").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


# ─── SoHo Team ───
@app.route("/api/soho_monitoring")
def api_soho_monitoring():
    conn = get_db()
    monitor = request.args.get("monitor", "")
    query = "SELECT * FROM soho_monitoring WHERE 1=1"
    params = []
    if monitor:
        query += " AND monitor = ?"
        params.append(monitor)
    query += " ORDER BY monitor, yearmonth"
    rows = conn.execute(query, params).fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/soho_monitors")
def api_soho_monitors():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT monitor FROM soho_monitoring ORDER BY monitor").fetchall()
    data = [r["monitor"] for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/soho_client_success")
def api_soho_client_success():
    conn = get_db()
    rows = conn.execute("SELECT * FROM soho_client_success").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/soho_social_content")
def api_soho_social_content():
    conn = get_db()
    rows = conn.execute("SELECT * FROM soho_social_content").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


# ─── TA Team ───
@app.route("/api/ta_open_positions")
def api_ta_open_positions():
    conn = get_db()
    rows = conn.execute("SELECT * FROM ta_open_positions ORDER BY id").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/ta_leader_positions")
def api_ta_leader_positions():
    conn = get_db()
    rows = conn.execute("SELECT * FROM ta_leader_positions ORDER BY id").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/ta_partner_performance")
def api_ta_partner_performance():
    conn = get_db()
    rows = conn.execute("SELECT * FROM ta_partner_performance ORDER BY id").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


# ─── Customer Experience ───
@app.route("/api/customer_experience")
def api_customer_experience():
    conn = get_db()
    rows = conn.execute("SELECT * FROM customer_experience ORDER BY serial_no").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


@app.route("/api/customer_experience_notes")
def api_customer_experience_notes():
    conn = get_db()
    rows = conn.execute("SELECT * FROM customer_experience_notes").fetchall()
    data = [dict(r) for r in rows]
    conn.close()
    return jsonify(data)


if __name__ == "__main__":
    print("🚀 WFM Dashboard running at http://127.0.0.1:5000")
    print("🔒 Data is secured in local SQLite database")
    app.run(debug=True, host="127.0.0.1", port=5000)
