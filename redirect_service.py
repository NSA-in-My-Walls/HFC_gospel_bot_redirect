from flask import Flask, redirect, request
import sqlite3
import time
import os

app = Flask(__name__)

DB_PATH = "click_log.db"
REDIRECT_URL = "https://www.youtube.com/watch?v=c-L7akD6cMQ&t=3198s"

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

@app.route("/saved")
def track_and_redirect():
    ip = request.remote_addr
    user_agent = request.headers.get("User-Agent", "")
    ts = time.time()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO clicks VALUES (?, ?, ?)", (ts, ip, user_agent))
    conn.commit()
    conn.close()
    return redirect(REDIRECT_URL, code=302)

if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=int(os.environ.get("PORT", 5000)))
