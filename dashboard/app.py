import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

st.set_page_config(page_title="r/NYU Abuse Monitor (Proto)", layout="wide")

engine = create_engine("sqlite:///../nyu_reddit.sqlite")

@st.cache_data(ttl=60)
def fetch_daily():
    return pd.read_sql("SELECT * FROM daily_counts", engine)

st.title("r/NYU Abuse Timeseries — Minimal Prototype")

df = fetch_daily()
if df.empty:
    st.info("No data yet. Run ingest.py then classify.py.")
else:
    st.subheader("Counts per Day")
    st.dataframe(df.tail(30), use_container_width=True)
    for col in ["harassment_count","racist_count","sexist_count","toxic_count"]:
        st.line_chart(df.set_index("day")[col])

    st.subheader("Rates per 1k (last 90 days)")
    for col in ["harassment_count","racist_count","sexist_count","toxic_count"]:
        rate_col = col + "_per_1k"
        if rate_col not in df.columns:
            df[rate_col] = (df[col] / df["total_scored"].clip(lower=1)) * 1000
        st.line_chart(df.set_index("day")[rate_col])
