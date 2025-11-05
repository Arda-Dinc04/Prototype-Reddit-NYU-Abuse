#!/usr/bin/env python3
"""
Toxicity Analysis Dashboard
Streamlit dashboard for viewing toxicity classification results
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from datetime import datetime, timedelta
import json

@st.cache_data
def load_topic_mentions(db_path: str):
    conn = sqlite3.connect(db_path)
    dfm = pd.read_sql_query("SELECT * FROM topic_mentions_daily ORDER BY day", conn)
    conn.close()
    dfm["day"] = pd.to_datetime(dfm["day"])
    return dfm

@st.cache_data
def load_topic_mentions_cat(db_path: str):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM topic_mentions_cat_daily ORDER BY day", conn)
    conn.close()
    df["day"] = pd.to_datetime(df["day"])
    return df

# Page configuration
st.set_page_config(
    page_title="NYU Reddit Toxicity Analysis",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    """Load toxicity classification data from SQLite"""
    conn = sqlite3.connect("nyu_reddit_local.sqlite")
    
    # Get toxicity classifications with original data
    query = """
    SELECT 
        tc.*,
        CASE 
            WHEN tc.item_type = 'post' THEN p.raw_json
            WHEN tc.item_type = 'comment' THEN c.raw_json
        END as raw_json,
        CASE 
            WHEN tc.item_type = 'post' THEN p.created_utc
            WHEN tc.item_type = 'comment' THEN c.created_utc
        END as created_utc,
        CASE 
            WHEN tc.item_type = 'post' THEN p.score
            WHEN tc.item_type = 'comment' THEN c.score
        END as score
    FROM toxicity_classifications tc
    LEFT JOIN posts p ON tc.id = p.id AND tc.item_type = 'post'
    LEFT JOIN comments c ON tc.id = c.id AND tc.item_type = 'comment'
    WHERE tc.hate_speech >= 0.20
    ORDER BY tc.hate_speech DESC, tc.classification_timestamp DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert timestamps
    df['created_date'] = pd.to_datetime(df['created_utc'], unit='s')
    df['classification_date'] = pd.to_datetime(df['classification_timestamp'])
    
    # Add flagged column
    thresholds = get_toxicity_thresholds()
    df['is_flagged'] = df.apply(lambda row: is_flagged(row, thresholds), axis=1)
    df['flag_reasons'] = df.apply(lambda row: get_flag_reasons(row, thresholds), axis=1)
    
    return df

def get_toxicity_thresholds():
    """Get toxicity thresholds for flagging - optimized for dehatebert"""
    return {
        "non_hate": {"high": 0.20, "medium": 0.20},
        "hate_speech": {"high": 0.20, "medium": 0.20}
    }

def is_flagged(row, thresholds):
    """Check if an item is flagged based on thresholds - only flag hate_speech"""
    # Only flag based on hate_speech score, not non_hate
    hate_score = row.get('hate_speech', 0)
    hate_threshold = thresholds.get('hate_speech', {}).get('high', 0.50)
    return hate_score >= hate_threshold

def get_flag_reasons(row, thresholds):
    """Get reasons why an item was flagged - only hate_speech"""
    reasons = []
    hate_score = row.get('hate_speech', 0)
    hate_threshold = thresholds.get('hate_speech', {}).get('high', 0.50)
    if hate_score >= hate_threshold:
        reasons.append(f"hate_speech ({hate_score:.2f})")
    return ", ".join(reasons)

def extract_original_content(row):
    """Extract original title and body from raw JSON"""
    try:
        raw_data = json.loads(row['raw_json'])
        raw_content = raw_data.get('raw_data', {})
        
        title = raw_content.get('title', '')
        body = raw_content.get('body', '')
        author = raw_data.get('author', 'Unknown')
        
        return title, body, author
    except:
        return '', '', 'Unknown'

def main():
    st.title("🚨 NYU Reddit Flagged Content Dashboard")
    st.markdown("**Showing only items flagged for hate speech (score ≥ 0.20)**")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading data..."):
        df = load_data()
    
    if df.empty:
        st.error("No toxicity classification data found. Run the classification script first.")
        return
    
    thresholds = get_toxicity_thresholds()
    
    # Sidebar filters
    st.sidebar.header("🔧 Filters")
    
    # Date range filter
    min_date = df['created_date'].min().date()
    max_date = df['created_date'].max().date()
    
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df[(df['created_date'].dt.date >= start_date) & (df['created_date'].dt.date <= end_date)]
    else:
        df_filtered = df
    
    # Content type filter
    content_types = st.sidebar.multiselect(
        "Content Type",
        options=['post', 'comment'],
        default=['post', 'comment']
    )
    
    if content_types:
        df_filtered = df_filtered[df_filtered['item_type'].isin(content_types)]
    
    
    # Main dashboard
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Flagged Items", len(df_filtered))
    
    with col2:
        high_hate_count = len(df_filtered[df_filtered['hate_speech'] >= 0.50])
        st.metric("High Hate (≥0.50)", high_hate_count)
    
    with col3:
        medium_hate_count = len(df_filtered[(df_filtered['hate_speech'] >= 0.30) & (df_filtered['hate_speech'] < 0.50)])
        st.metric("Medium Hate (0.30-0.50)", medium_hate_count)
    
    with col4:
        low_hate_count = len(df_filtered[(df_filtered['hate_speech'] >= 0.20) & (df_filtered['hate_speech'] < 0.30)])
        st.metric("Low Hate (0.20-0.30)", low_hate_count)
    
    st.markdown("---")
    
    # Toxicity distribution charts
    st.subheader("📊 Toxicity Score Distribution")
    
    toxicity_labels = ['non_hate', 'hate_speech']
    
    # Create subplots for toxicity scores
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=toxicity_labels,
        specs=[[{"secondary_y": False}] * 2]
    )
    
    for i, label in enumerate(toxicity_labels):
        row = 1
        col = i + 1
        
        # Filter out deleted/removed items for score distribution
        valid_scores = df_filtered[
            (df_filtered['is_deleted'] == False) & 
            (df_filtered['is_removed'] == False) & 
            (df_filtered['is_empty'] == False)
        ][label]
        
        fig.add_trace(
            go.Histogram(
                x=valid_scores,
                nbinsx=20,
                name=label,
                showlegend=False
            ),
            row=row, col=col
        )
        
        # Add threshold line
        threshold = thresholds[label]["high"]
        fig.add_vline(
            x=threshold,
            line_dash="dash",
            line_color="red",
            row=row, col=col
        )
    
    fig.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    # Daily toxicity trends
    st.subheader("📈 Daily Toxicity Trends")
    
    daily_stats = df_filtered.groupby(df_filtered['created_date'].dt.date).agg({
        'is_flagged': 'sum',
        'id': 'count',
        'is_deleted': 'sum',
        'is_removed': 'sum'
    }).reset_index()
    
    daily_stats['flagged_rate'] = (daily_stats['is_flagged'] / daily_stats['id']) * 100
    
    fig_trends = make_subplots(
        rows=2, cols=1,
        subplot_titles=['Daily Flagged Items', 'Daily Flagged Rate (%)'],
        vertical_spacing=0.1
    )
    
    fig_trends.add_trace(
        go.Scatter(
            x=daily_stats['created_date'],
            y=daily_stats['is_flagged'],
            mode='lines+markers',
            name='Flagged Items',
            line=dict(color='red')
        ),
        row=1, col=1
    )
    
    fig_trends.add_trace(
        go.Scatter(
            x=daily_stats['created_date'],
            y=daily_stats['flagged_rate'],
            mode='lines+markers',
            name='Flagged Rate (%)',
            line=dict(color='orange')
        ),
        row=2, col=1
    )
    
    fig_trends.update_layout(height=500)
    st.plotly_chart(fig_trends, use_container_width=True)
    
    # Topic Mentions section
    st.markdown("---")
    st.subheader("🧵 Topic Mentions Over Time")

    # Load mentions
    mentions_df = load_topic_mentions("nyu_reddit_local.sqlite")
    if mentions_df.empty:
        st.info("Run `python src/compute_topic_mentions.py` to populate topic mentions.")
    else:
        # Default terms
        all_terms = sorted(mentions_df["term"].unique().tolist())
        default_terms = [t for t in ["black","asian","white","racism","financial aid"] if t in all_terms]

        # Date range filter for topics
        min_day = mentions_df["day"].min().date()
        max_day = mentions_df["day"].max().date()
        dr = st.date_input("Date Range (topics)", (min_day, max_day), min_value=min_day, max_value=max_day, key="topics_date")
        if isinstance(dr, tuple) and len(dr) == 2:
            start_day, end_day = dr
            mdf = mentions_df[(mentions_df["day"].dt.date >= start_day) & (mentions_df["day"].dt.date <= end_day)]
        else:
            mdf = mentions_df.copy()

        terms = st.multiselect("Terms to plot", options=all_terms, default=default_terms)
        metric = st.radio("Metric", ["Count", "Per 1k items"], horizontal=True)
        ycol = "count" if metric == "Count" else "rate_per_1k"

        if terms:
            plot_df = mdf[mdf["term"].isin(terms)]
            fig = px.line(plot_df, x="day", y=ycol, color="term", markers=True,
                          title=f"Topic mentions ({ycol})")
            fig.update_layout(hovermode="x unified", height=420)
            st.plotly_chart(fig, use_container_width=True)

            st.caption("Tip: counts are binary-per-item; a single post/comment mentioning a term counts once.")
        else:
            st.info("Select at least one term to visualize.")
    
    # Topic Categories section
    st.markdown("---")
    st.subheader("🧵 Topic Categories Over Time")
    
    DB_PATH = "nyu_reddit_local.sqlite"
    mcat = load_topic_mentions_cat(DB_PATH)
    
    if mcat.empty:
        st.info("Run `python src/compute_topic_mentions.py` to populate category mentions.")
    else:
        categories = ["race_ethnicity","countries","gender_sexuality","profanity","academics_finance","safety_crime","housing"]
        pretty = {
            "race_ethnicity":"Race/Ethnicity",
            "countries":"Countries",
            "gender_sexuality":"Gender & Sexuality",
            "profanity":"Profanity",
            "academics_finance":"Academics & Finance",
            "safety_crime":"Safety/Crime",
            "housing":"Housing"
        }
        
        tabs = st.tabs([pretty[c] for c in categories])
        
        # Global controls
        metric = st.radio("Metric", ["Count", "Per 1k items"], horizontal=True, key="topic_metric")
        ycol = "count" if metric=="Count" else "rate_per_1k"
        
        # Date filter
        min_day, max_day = mcat["day"].min().date(), mcat["day"].max().date()
        dr = st.date_input("Date Range (topics)", (min_day, max_day), min_value=min_day, max_value=max_day, key="topics_date2")
        if isinstance(dr, tuple) and len(dr)==2:
            start, end = dr
            mcat = mcat[(mcat["day"].dt.date >= start) & (mcat["day"].dt.date <= end)]
        
        for cat, tab in zip(categories, tabs):
            with tab:
                dfc = mcat[mcat["category"]==cat]
                terms = sorted(dfc["term"].unique().tolist())
                selected = st.multiselect(f"Terms in {pretty[cat]}", terms, default=terms[:min(5,len(terms))], key=f"sel_{cat}")
                if selected:
                    plot_df = dfc[dfc["term"].isin(selected)]
                    fig = px.line(plot_df, x="day", y=ycol, color="term", markers=True)
                    fig.update_layout(hovermode="x unified", height=360, title=f"{pretty[cat]} ({ycol})")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Select at least one term.")
    
    # Flagged content section
    st.markdown("---")
    st.subheader("🚨 Flagged Content Review")
    
    if len(df_filtered) > 0:
        st.write(f"Showing {len(df_filtered)} flagged items (hate speech ≥ 0.20)")
        
        # Sort by highest hate speech scores
        flagged_df = df_filtered.sort_values('hate_speech', ascending=False)
        
        # Display flagged items
        for idx, row in flagged_df.iterrows():
            # Extract original content
            title, body, author = extract_original_content(row)
            
            with st.expander(f"🚩 {row['item_type'].title()} by {author} - {row['flag_reasons']}"):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.write("**Original Content:**")
                    if row['item_type'] == 'post' and title:
                        st.write(f"**Title:** {title}")
                    st.write(f"**Body:** {body}")
                    
                    st.write("**Cleaned Text:**")
                    st.write(row['text_cleaned'])
                
                with col2:
                    st.write("**Toxicity Scores:**")
                    for label in toxicity_labels:
                        score = row[label]
                        threshold = thresholds[label]["high"]
                        if score >= threshold:
                            st.write(f"🔴 **{label}:** {score:.3f}")
                        elif score >= thresholds[label]["medium"]:
                            st.write(f"🟡 **{label}:** {score:.3f}")
                        else:
                            st.write(f"🟢 **{label}:** {score:.3f}")
                    
                    st.write("**Metadata:**")
                    st.write(f"**Score:** {row['score']}")
                    st.write(f"**Date:** {row['created_date'].strftime('%Y-%m-%d %H:%M')}")
                    st.write(f"**Author:** {author}")
    else:
        st.info("No flagged items found in the selected filters.")
    
    # Summary statistics
    st.markdown("---")
    st.subheader("📋 Summary Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Content Type Breakdown:**")
        type_counts = df_filtered['item_type'].value_counts()
        st.write(type_counts)
    
    with col2:
        st.write("**Toxicity Label Statistics:**")
        toxicity_stats = df_filtered[toxicity_labels].describe()
        st.write(toxicity_stats.round(3))

if __name__ == "__main__":
    main()
