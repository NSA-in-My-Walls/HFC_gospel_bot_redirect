import os
import time
import requests
import sys
from flask import Flask, redirect, request, make_response
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# connect once, enable autocommit so each INSERT stands alone
DATABASE_URL = os.environ['DATABASE_URL']
conn = psycopg2.connect(DATABASE_URL, sslmode='require')
conn.autocommit = True

# migrations: ensure clicks table has lat & lon
with conn.cursor() as cur:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clicks (
        ts         TIMESTAMPTZ,
        ip         TEXT,
        user_agent TEXT
    );""")
    # add lat/lon if they don’t exist
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
    ua = request.headers.get('User-Agent','')

    # 1) Dedupe by cookie
    if request.cookies.get('hfc_clicked'):
        return redirect(REDIRECT_URL, code=302)

    # 2) Skip known crawlers / previewers
    skip_bots = ['Slackbot','facebookexternalhit','Twitterbot','Discordbot',
                 'LinkedInBot','WhatsApp','curl','wget']
    if any(bot in ua for bot in skip_bots):
        return redirect(REDIRECT_URL, code=302)

    # 3) Geolocate IP: try text first, then JSON
    lat = lon = None
    try:
        r = requests.get(f"https://ipapi.co/{ip}/latlong/", timeout=2)
        if r.status_code == 200 and ',' in r.text:
            lat_str, lon_str = r.text.strip().split(',')
            lat, lon = float(lat_str), float(lon_str)
        else:
            rj = requests.get(f"https://ipapi.co/{ip}/json/", timeout=2).json()
            lat = rj.get('latitude')
            lon = rj.get('longitude')
    except Exception as e:
        sys.stderr.write(f"⚠️ Geo lookup failed for {ip}: {e}\n")
        sys.stderr.flush()

    # 4) Real user -> log it with whatever lat/lon we got
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clicks (ts, ip, user_agent, lat, lon) "
            "VALUES (to_timestamp(%s), %s, %s, %s, %s)",
            (ts, ip, ua, lat, lon)
        )
        sys.stderr.write(f"📝 Logged click: ip={ip} lat={lat} lon={lon}\n")
        sys.stderr.flush()

    # 5) Set cookie & redirect
    resp = make_response(redirect(REDIRECT_URL, code=302))
    resp.set_cookie('hfc_clicked', '1', max_age=60*60)
    return resp

if __name__ == '__main__':
    app.run(
        host='0.0.0.0', 
        port=int(os.environ.get('PORT', 5000)),
        debug=True
    )
