import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from datetime import datetime
import requests

# Configuration
db_url = os.environ['DATABASE_URL']
engine = create_engine(db_url)

st.set_page_config(page_title='Outreach Dashboard', layout='wide')
st.title('📊 Houston Faith Church Outreach Dashboard')

# Load data
dm_log = pd.read_sql('SELECT * FROM dm_log', engine)
clicks_df = pd.read_sql('SELECT * FROM clicks', engine)

# Compute KPIs
now_ts = datetime.now().timestamp()

dms_all = len(dm_log)
dms_week = dm_log[dm_log['ts'] > (now_ts - 7*24*3600)].shape[0]
success_rate = dm_log['status'].eq('success').mean()
unique_users = dm_log['user'].nunique()

clicks_total = len(clicks_df)
clicks_week = clicks_df['ts'].apply(lambda t: t.timestamp() > (now_ts - 7*24*3600)).sum()

EST_RATE = 0.75
est_sal_all = int(clicks_total * EST_RATE)
est_sal_week = int(clicks_week * EST_RATE)

# Display metrics
cols = st.columns(4)
cols[0].metric('DMs This Week', dms_week)
cols[1].metric('DMs All Time', dms_all)
cols[2].metric('Success Rate', f"{success_rate:.1%}")
cols[3].metric('Unique Users', unique_users)

cols2 = st.columns(4)
cols2[0].metric('Clicks This Week', clicks_week)
cols2[1].metric('Clicks All Time', clicks_total)
cols2[2].metric('Est. Salvations Week', est_sal_week)
cols2[3].metric('Est. Salvations All Time', est_sal_all)

# Recent Clicks
st.subheader('Recent Clicks')
clicks_df['time'] = clicks_df['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')
st.dataframe(
    clicks_df[['time','ip','user_agent']]
      .sort_values('time', ascending=False)
      .head(10),
    use_container_width=True
)

# Backfill missing geolocation and persist to DB
@st.cache_data(ttl=3600)
def geolocate_batch(ips):
    coords = {}
    for ip in ips:
        try:
            r = requests.get(f"https://ipapi.co/{ip}/latlong/", timeout=2)
            if r.status_code == 200 and ',' in r.text:
                lat_str, lon_str = r.text.strip().split(',')
                coords[ip] = (float(lat_str), float(lon_str))
                continue
            j = requests.get(f"https://ipapi.co/{ip}/json/", timeout=2).json()
            if 'latitude' in j and 'longitude' in j:
                coords[ip] = (j['latitude'], j['longitude'])
        except Exception:
            pass
    return coords

# Identify IPs with null lat/lon
missing_ips = clicks_df.loc[clicks_df['lat'].isna() & clicks_df['lon'].isna(), 'ip'].unique().tolist()
ip_map = geolocate_batch(missing_ips)

# Persist backfilled coordinates
if ip_map:
    with engine.begin() as conn:
        for ip, (lat, lon) in ip_map.items():
            conn.execute(
                text("UPDATE clicks SET lat=:lat, lon=:lon WHERE ip=:ip AND lat IS NULL AND lon IS NULL"),
                {"lat": lat, "lon": lon, "ip": ip}
            )
    # reload after update
    clicks_df = pd.read_sql('SELECT * FROM clicks', engine)

# Click Geography
st.subheader('🌍 Click Geography')
geo = clicks_df.dropna(subset=['lat','lon'])
if not geo.empty:
    st.map(geo[['lat','lon']])
else:
    st.info("No geolocated clicks available yet.")

# Raw Click Data
st.subheader("🗄️ Raw Click Data (latest 10)")
st.dataframe(
    clicks_df[['time','ip','user_agent','lat','lon']]
      .sort_values('time', ascending=False)
      .head(10),
    use_container_width=True
)

# Recent DMs Sent
st.subheader('Recent DMs Sent')
dm_log['time'] = pd.to_datetime(dm_log['ts'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
st.dataframe(
    dm_log[['time','post_id','user','subreddit','status','error']]
      .sort_values('time', ascending=False)
      .head(10)
)

# Top Subreddits
st.subheader('Top Subreddits')
top_subs = (
    dm_log['subreddit']
      .value_counts()
      .rename_axis('subreddit')
      .reset_index(name='count')
)
st.dataframe(top_subs.head(10))
