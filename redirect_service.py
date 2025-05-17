import os
import time
import requests
import sys
from flask import Flask, redirect, request, make_response
import psycopg2

app = Flask(__name__)

# single DB connection, autocommit on so each INSERT stands alone
DATABASE_URL = os.environ['DATABASE_URL']
conn = psycopg2.connect(DATABASE_URL, sslmode='require')
conn.autocommit = True

# ensure clicks table has lat & lon
with conn.cursor() as cur:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clicks (
        ts         TIMESTAMPTZ,
        ip         TEXT,
        user_agent TEXT,
        lat        DOUBLE PRECISION,
        lon        DOUBLE PRECISION
    );""")

REDIRECT_URL = os.environ.get(
    'REDIRECT_URL',
    'https://houstonfaithchurch.com/believer-basics/do-you-know-jesus/'
)

@app.route('/saved')
def track_and_redirect():
    ts = time.time()
    # respect X-Forwarded-For if you're behind a load-balancer
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    sys.stderr.write(f"🔍 Geo lookup for IP: {ip}\n"); sys.stderr.flush()
    ua = request.headers.get('User-Agent', '')

    # 1) Dedupe by cookie
    if request.cookies.get('hfc_clicked'):
        return redirect(REDIRECT_URL, code=302)

    # 2) Skip known bots
    skip_bots = ['Slackbot','facebookexternalhit','Twitterbot',
                 'Discordbot','LinkedInBot','WhatsApp','curl','wget']
    if any(bot in ua for bot in skip_bots):
        return redirect(REDIRECT_URL, code=302)

    # 4) Log click
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clicks (ts, ip, user_agent) "
            "VALUES (to_timestamp(%s), %s, %s)",
            (ts, ip, ua)
        )


    # 5) Set cookie & redirect
    resp = make_response(redirect(REDIRECT_URL, code=302))
    resp.set_cookie('hfc_clicked', '1', max_age=3600)
    return resp

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000)),
        debug=True
    )
