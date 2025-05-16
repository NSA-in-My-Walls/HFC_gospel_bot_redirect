import os
import time
from flask import Flask, redirect, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# connect once, run all migrations, then keep it open for redirects
DATABASE_URL = os.environ['DATABASE_URL']
conn = psycopg2.connect(DATABASE_URL, sslmode='require')

with conn.cursor() as cur:
    # your bot’s metrics tables
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dm_log (
        ts        DOUBLE PRECISION,
        post_id   TEXT,
        "user"    TEXT,
        subreddit TEXT,
        status    TEXT,
        error     TEXT
    );""")
    cur.execute("CREATE TABLE IF NOT EXISTS run_log ( ts DOUBLE PRECISION );")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS error_log (
        ts      DOUBLE PRECISION,
        context TEXT,
        error   TEXT
    );""")

    # clicks table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clicks (
        ts         TIMESTAMPTZ,
        ip         TEXT,
        user_agent TEXT
    );""")

    conn.commit()

REDIRECT_URL = os.environ.get(
    'REDIRECT_URL',
    'https://houstonfaithchurch.com/believer-basics/do-you-know-jesus/'
)

@app.route('/saved')
def track_and_redirect():
    ts = time.time()
    ip = request.remote_addr
    ua = request.headers.get('User-Agent', '')

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clicks (ts, ip, user_agent) VALUES (to_timestamp(%s), %s, %s)",
            (ts, ip, ua)
        )
        conn.commit()

    return redirect(REDIRECT_URL, code=302)

if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=int(os.environ.get('PORT', 5000)),
            debug=True)
