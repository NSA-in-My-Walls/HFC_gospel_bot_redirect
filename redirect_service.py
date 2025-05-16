import os
import time
from flask import Flask, redirect, request
import psycopg2
from psycopg2.extras import RealDictCursor

# Flask app
app = Flask(__name__)

# Database connection
DATABASE_URL = os.environ['DATABASE_URL']  # from Render Postgres
conn = psycopg2.connect(DATABASE_URL, sslmode='require')

# Ensure the clicks table exists
with conn.cursor() as cur:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clicks (
            ts TIMESTAMPTZ,
            ip TEXT,
            user_agent TEXT
        );
        """
    )
    conn.commit()

# Redirect target
REDIRECT_URL = os.environ.get(
    'REDIRECT_URL',
    'https://houstonfaithchurch.com/believer-basics/do-you-know-jesus/'
)

@app.route('/saved')
def track_and_redirect():
    ts = time.time()
    ip = request.remote_addr
    ua = request.headers.get('User-Agent', '')
    
    # Insert click into Postgres
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clicks (ts, ip, user_agent) VALUES (to_timestamp(%s), %s, %s)",
            (ts, ip, ua)
        )
        conn.commit()

    return redirect(REDIRECT_URL, code=302)

if __name__ == '__main__':
    # Local dev
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=True)
