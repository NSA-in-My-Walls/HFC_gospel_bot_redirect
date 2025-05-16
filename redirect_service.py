import os
import time
from flask import Flask, redirect, request, make_response
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

    # 1) Dedupe by cookie: only log the first hit per session
    if request.cookies.get('hfc_clicked'):
        return redirect(REDIRECT_URL, code=302)

    # 2) Skip known previewers / crawlers
    skip_bots = [
        'Slackbot', 'facebookexternalhit', 'Twitterbot', 'Discordbot',
        'LinkedInBot', 'WhatsApp', 'curl', 'wget'
    ]
    if any(bot in ua for bot in skip_bots):
        return redirect(REDIRECT_URL, code=302)

    # 3) Real user → log it
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clicks (ts, ip, user_agent) VALUES (to_timestamp(%s), %s, %s)",
            (ts, ip, ua)
        )
        conn.commit()

    # 4) Respond with a cookie to prevent duplicate logs in this session
    resp = make_response(redirect(REDIRECT_URL, code=302))
    resp.set_cookie('hfc_clicked', '1', max_age=60*60)  # cookie valid for 1 hour
    return resp

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000)),
        debug=True
    )
