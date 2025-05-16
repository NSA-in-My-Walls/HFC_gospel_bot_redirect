from flask import Flask, redirect, request
import sqlite3
import time
import os
import requests

app = Flask(__name__)

# Configuration
DB_PATH = "click_log.db"
REDIRECT_URL = "https://houstonfaithchurch.com/believer-basics/do-you-know-jesus/"
GIST_ID = os.environ.get("GIST_ID")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# Initialize or restore database

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS clicks (
            ts REAL,
            ip TEXT,
            user_agent TEXT
        )
    """
    )
    conn.commit()
    conn.close()


def restore_from_gist():
    """Restore click log from GitHub Gist backup before starting the app."""
    if not GIST_ID or not GITHUB_TOKEN:
        return
    try:
        url = f"https://api.github.com/gists/{GIST_ID}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        files = r.json().get("files", {})
        if "click_backup.sql" not in files:
            return
        raw_url = files["click_backup.sql"]["raw_url"]
        sql = requests.get(raw_url).text
        conn = sqlite3.connect(DB_PATH)
        conn.executescript(sql)
        conn.commit()
        conn.close()
    except Exception as e:
        app.logger.error(f"Exception during gist restore: {e}")


def backup_to_gist():
    """Backup click log to GitHub Gist after each redirect."""
    if not GIST_ID or not GITHUB_TOKEN:
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        dump = "\n".join(conn.iterdump())
        conn.close()
        url = f"https://api.github.com/gists/{GIST_ID}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        data = {"files": {"click_backup.sql": {"content": dump}}}
        response = requests.patch(url, headers=headers, json=data)
        if response.status_code not in (200, 201):
            app.logger.error(f"Gist backup failed: {response.status_code} {response.text}")
    except Exception as e:
        app.logger.error(f"Exception during gist backup: {e}")

@app.route("/saved")
def track_and_redirect():
    """Log a click and redirect to the salvation resource."""
    ts = time.time()
    ip = request.remote_addr
    user_agent = request.headers.get("User-Agent", "")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO clicks VALUES (?, ?, ?)", (ts, ip, user_agent))
    conn.commit()
    conn.close()

    backup_to_gist()
    return redirect(REDIRECT_URL, code=302)

if __name__ == "__main__":
    restore_from_gist()
    init_db()
    # Grab Render’s assigned port (defaults to 5000 locally)
    port = int(os.environ.get("PORT", 5000))
    # Listen on 0.0.0.0 so Render’s proxy can reach you
    app.run(host="0.0.0.0", port=port, debug=True)
