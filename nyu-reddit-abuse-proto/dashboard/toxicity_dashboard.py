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
    ORDER BY tc.classification_timestamp DESC
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
    """Get toxicity thresholds for flagging"""
    return {
        "toxic": {"high": 0.70, "medium": 0.50},
        "insult": {"high": 0.78, "medium": 0.55},          # tiny nudge
        "identity_attack": {"high": 0.58, "medium": 0.38}, # lower to catch more
        "threat": {"high": 0.68, "medium": 0.55},
        "sexual_explicit": {"high": 0.75, "medium": 0.55},
        "severe_toxic": {"high": 0.80, "medium": 0.60},
        "severe_toxicity": {"high": 0.80, "medium": 0.60}
    }

def is_flagged(row, thresholds):
    """Check if an item is flagged based on thresholds"""
    for label, threshold_info in thresholds.items():
        if row.get(label, 0) >= threshold_info["high"]:
            return True
    return False

def get_flag_reasons(row, thresholds):
    """Get reasons why an item was flagged"""
    reasons = []
    for label, threshold_info in thresholds.items():
        if row.get(label, 0) >= threshold_info["high"]:
            reasons.append(f"{label} ({row.get(label, 0):.2f})")
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
    st.title("🔍 NYU Reddit Toxicity Analysis Dashboard")
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
    
    # Flagged only filter
    show_flagged_only = st.sidebar.checkbox("Show flagged items only", value=False)
    if show_flagged_only:
        df_filtered = df_filtered[df_filtered['is_flagged'] == True]
    
    # Main dashboard
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Items", len(df_filtered))
    
    with col2:
        flagged_count = len(df_filtered[df_filtered['is_flagged'] == True])
        st.metric("Flagged Items", flagged_count)
    
    with col3:
        if len(df_filtered) > 0:
            flagged_rate = (flagged_count / len(df_filtered)) * 100
            st.metric("Flagged Rate", f"{flagged_rate:.1f}%")
        else:
            st.metric("Flagged Rate", "0%")
    
    with col4:
        deleted_count = len(df_filtered[df_filtered['is_deleted'] == True])
        removed_count = len(df_filtered[df_filtered['is_removed'] == True])
        st.metric("Deleted/Removed", deleted_count + removed_count)
    
    st.markdown("---")
    
    # Toxicity distribution charts
    st.subheader("📊 Toxicity Score Distribution")
    
    toxicity_labels = ['toxic', 'insult', 'identity_attack', 'threat', 'sexual_explicit', 'severe_toxic']
    
    # Create subplots for toxicity scores
    fig = make_subplots(
        rows=2, cols=3,
        subplot_titles=toxicity_labels,
        specs=[[{"secondary_y": False}] * 3] * 2
    )
    
    for i, label in enumerate(toxicity_labels):
        row = (i // 3) + 1
        col = (i % 3) + 1
        
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
    
    fig.update_layout(height=600, showlegend=False)
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
    
    # Flagged content section
    st.markdown("---")
    st.subheader("🚨 Flagged Content Review")
    
    flagged_df = df_filtered[df_filtered['is_flagged'] == True].copy()
    
    if len(flagged_df) > 0:
        st.write(f"Found {len(flagged_df)} flagged items")
        
        # Sort by highest toxicity scores
        flagged_df['max_toxicity'] = flagged_df[toxicity_labels].max(axis=1)
        flagged_df = flagged_df.sort_values('max_toxicity', ascending=False)
        
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
