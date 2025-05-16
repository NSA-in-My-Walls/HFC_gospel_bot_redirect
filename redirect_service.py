from flask import Flask, redirect, request
import sqlite3
import time
import os
import requests

app = Flask(__name__)

# Constants and config
DB_PATH = "click_log.db"
REDIRECT_URL = "https://houstonfaithchurch.com/believer-basics/do-you-know-jesus/"
# GitHub Gist settings for auto-backup
GIST_ID = os.environ.get("GIST_ID")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# Ensure database exists
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS clicks (
            ts REAL,
            ip TEXT,
            user_agent TEXT
        )
    """)
    conn.commit()
    conn.close()

# Dump SQLite to SQL text and push to GitHub Gist
def backup_to_gist():
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
    # Log the click
    ip = request.remote_addr
    user_agent = request.headers.get("User-Agent", "")
    ts = time.time()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO clicks VALUES (?, ?, ?)", (ts, ip, user_agent))
    conn.commit()
    conn.close()

    # Auto-backup to GitHub Gist
    backup_to_gist()

    # Redirect user
    return redirect(REDIRECT_URL, code=302)

if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=int(os.environ.get("PORT", 5000)))
