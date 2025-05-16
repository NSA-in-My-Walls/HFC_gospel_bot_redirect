import os
import time
import requests
from flask import Flask, redirect, request, make_response
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Connect once, enable autocommit
DATABASE_URL = os.environ['DATABASE_URL']
conn = psycopg2.connect(DATABASE_URL, sslmode='require')
conn.autocommit = True

with conn.cursor() as cur:
    # Metrics tables
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

    # Ensure clicks table exists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clicks (
        ts         TIMESTAMPTZ,
        ip         TEXT,
        user_agent TEXT
    );""")
    # Add lat/lon columns if missing
    cur.execute("ALTER TABLE clicks ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;")
    cur.execute("ALTER TABLE clicks ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;")

REDIRECT_URL = os.environ.get(
    'REDIRECT_URL',
    'https://houstonfaithchurch.com/believer-basics/do-you-know-jesus/'
)

@app.route('/saved')
def track_and_redirect():
    ts = time.time()
    ip = request.headers.get('X-Forwarded-For', request.remote_addr).split(',')[0].strip()
    ua = request.headers.get('User-Agent', '')

    # Cookie dedupe
    if request.cookies.get('hfc_clicked'):
        return redirect(REDIRECT_URL, code=302)

    # Skip bots
    skip_bots = ['Slackbot','facebookexternalhit','Twitterbot','Discordbot',
                 'LinkedInBot','WhatsApp','curl','wget']
    if any(bot in ua for bot in skip_bots):
        return redirect(REDIRECT_URL, code=302)

    # Geolocate IP
    lat = lon = None
    try:
        resp = requests.get(f"https://ipapi.co/{ip}/json/")
        data = resp.json()
        lat = data.get('latitude')
        lon = data.get('longitude')
    except Exception:
        pass

    # Log click including lat/lon
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clicks (ts, ip, user_agent, lat, lon) VALUES (to_timestamp(%s), %s, %s, %s, %s)",
            (ts, ip, ua, lat, lon)
        )

    # Set cookie and redirect
    resp = make_response(redirect(REDIRECT_URL, code=302))
    resp.set_cookie('hfc_clicked','1', max_age=3600)
    return resp

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=True)
