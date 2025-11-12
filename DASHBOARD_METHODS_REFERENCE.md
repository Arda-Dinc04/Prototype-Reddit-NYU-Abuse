# 🔍 Dashboard Methods Reference

Quick reference for the exact methods and functions used to output data to the Streamlit dashboard.

## 📊 Core Data Loading Methods

### 1. `load_data()` - Main Toxicity Data Loader

**Location:** `dashboard/toxicity_dashboard.py` (line 40-81)

**Purpose:** Loads all flagged items (hate_speech ≥ 0.20) with their original content

**Method Signature:**
```python
@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_data():
    """Load toxicity classification data from SQLite"""
    conn = sqlite3.connect("nyu_reddit_full.sqlite")
    
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
```

**Returns:** DataFrame with columns:
- `id`, `item_type`, `text_cleaned`
- `non_hate`, `hate_speech` (scores)
- `is_deleted`, `is_removed`, `is_empty` (flags)
- `raw_json` (original Reddit data)
- `created_utc`, `score` (metadata)
- `created_date`, `classification_date` (parsed dates)
- `is_flagged`, `flag_reasons` (computed fields)

**Usage in Dashboard:**
```python
df = load_data()  # Called in main() function
```

---

### 2. `load_topic_mentions()` - Topic Mentions Loader

**Location:** `dashboard/toxicity_dashboard.py` (line 16-22)

**Purpose:** Loads daily topic mention counts

**Method Signature:**
```python
@st.cache_data
def load_topic_mentions(db_path: str):
    conn = sqlite3.connect(db_path)
    dfm = pd.read_sql_query("SELECT * FROM topic_mentions_daily ORDER BY day", conn)
    conn.close()
    dfm["day"] = pd.to_datetime(dfm["day"])
    return dfm
```

**Returns:** DataFrame with columns:
- `term` - Topic keyword (e.g., "black", "asian")
- `day` - Date (as datetime)
- `count` - Number of mentions
- `item_type` - 'post' or 'comment'

**Usage in Dashboard:**
```python
mentions_df = load_topic_mentions("nyu_reddit_full.sqlite")
```

---

### 3. `load_topic_mentions_cat()` - Category Mentions Loader

**Location:** `dashboard/toxicity_dashboard.py` (line 24-30)

**Purpose:** Loads daily category mention counts

**Method Signature:**
```python
@st.cache_data
def load_topic_mentions_cat(db_path: str):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM topic_mentions_cat_daily ORDER BY day", conn)
    conn.close()
    df["day"] = pd.to_datetime(df["day"])
    return df
```

**Returns:** DataFrame with columns:
- `category` - Category name (e.g., "race_ethnicity")
- `term` - Specific term within category
- `day` - Date (as datetime)
- `count` - Number of mentions

**Usage in Dashboard:**
```python
mcat = load_topic_mentions_cat("nyu_reddit_full.sqlite")
```

---

## 🎨 Visualization Methods

### 4. `get_toxicity_thresholds()` - Threshold Configuration

**Location:** `dashboard/toxicity_dashboard.py` (line 83-88)

**Purpose:** Returns toxicity thresholds for flagging

**Method Signature:**
```python
def get_toxicity_thresholds():
    """Get toxicity thresholds for flagging - optimized for dehatebert"""
    return {
        "non_hate": {"high": 0.20, "medium": 0.20},
        "hate_speech": {"high": 0.20, "medium": 0.20}
    }
```

**Returns:** Dictionary with threshold values

---

### 5. `is_flagged()` - Flagging Logic

**Location:** `dashboard/toxicity_dashboard.py` (line 90-95)

**Purpose:** Determines if an item should be flagged

**Method Signature:**
```python
def is_flagged(row, thresholds):
    """Check if an item is flagged based on thresholds - only flag hate_speech"""
    hate_score = row.get('hate_speech', 0)
    hate_threshold = thresholds.get('hate_speech', {}).get('high', 0.50)
    return hate_score >= hate_threshold
```

**Returns:** Boolean (True if flagged)

---

### 6. `get_flag_reasons()` - Flag Reason Generator

**Location:** `dashboard/toxicity_dashboard.py` (line 97-104)

**Purpose:** Generates human-readable flag reasons

**Method Signature:**
```python
def get_flag_reasons(row, thresholds):
    """Get reasons why an item was flagged - only hate_speech"""
    reasons = []
    hate_score = row.get('hate_speech', 0)
    hate_threshold = thresholds.get('hate_speech', {}).get('high', 0.50)
    if hate_score >= hate_threshold:
        reasons.append(f"hate_speech ({hate_score:.2f})")
    return ", ".join(reasons)
```

**Returns:** String with flag reasons

---

### 7. `extract_original_content()` - Content Extractor

**Location:** `dashboard/toxicity_dashboard.py` (line 106-118)

**Purpose:** Extracts title, body, and author from raw JSON

**Method Signature:**
```python
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
```

**Returns:** Tuple of (title, body, author)

---

## 📈 Dashboard Display Sections

### Section 1: Metrics Cards
**Location:** Lines 175-190

**Method:**
```python
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Flagged Items", len(df_filtered))

with col2:
    high_hate_count = len(df_filtered[df_filtered['hate_speech'] >= 0.50])
    st.metric("High Hate (≥0.50)", high_hate_count)

with col3:
    medium_hate_count = len(df_filtered[(df_filtered['hate_speech'] >= 0.30) & 
                                       (df_filtered['hate_speech'] < 0.50)])
    st.metric("Medium Hate (0.30-0.50)", medium_hate_count)

with col4:
    low_hate_count = len(df_filtered[(df_filtered['hate_speech'] >= 0.20) & 
                                     (df_filtered['hate_speech'] < 0.30)])
    st.metric("Low Hate (0.20-0.30)", low_hate_count)
```

---

### Section 2: Toxicity Score Distribution
**Location:** Lines 194-237

**Method:**
```python
fig = make_subplots(
    rows=1, cols=2,
    subplot_titles=toxicity_labels,
    specs=[[{"secondary_y": False}] * 2]
)

for i, label in enumerate(toxicity_labels):
    valid_scores = df_filtered[
        (df_filtered['is_deleted'] == False) & 
        (df_filtered['is_removed'] == False) & 
        (df_filtered['is_empty'] == False)
    ][label]
    
    fig.add_trace(
        go.Histogram(x=valid_scores, nbinsx=20, name=label),
        row=1, col=i+1
    )
    
    threshold = thresholds[label]["high"]
    fig.add_vline(x=threshold, line_dash="dash", line_color="red")

st.plotly_chart(fig, use_container_width=True)
```

---

### Section 3: Daily Toxicity Trends
**Location:** Lines 239-267

**Method:**
```python
daily_stats = df_filtered.groupby(df_filtered['created_date'].dt.date).agg({
    'is_flagged': 'sum',
    'id': 'count',
    'is_deleted': 'sum',
    'is_removed': 'sum'
}).reset_index()

fig_trends = go.Figure()
fig_trends.add_trace(
    go.Scatter(
        x=daily_stats['created_date'],
        y=daily_stats['is_flagged'],
        mode='lines+markers',
        name='Flagged Items',
        line=dict(color='red')
    )
)
st.plotly_chart(fig_trends, use_container_width=True)
```

---

### Section 4: Topic Mentions Over Time
**Location:** Lines 269-304

**Method:**
```python
mentions_df = load_topic_mentions("nyu_reddit_full.sqlite")

# Filter by date range
mdf = mentions_df[(mentions_df["day"].dt.date >= start_day) & 
                  (mentions_df["day"].dt.date <= end_day)]

# Filter by selected terms
plot_df = mdf[mdf["term"].isin(terms)]

fig = px.line(plot_df, x="day", y="count", color="term", markers=True,
              title="Topic mentions (Count)")
st.plotly_chart(fig, use_container_width=True)
```

---

### Section 5: Topic Categories
**Location:** Lines 306-350

**Method:**
```python
mcat = load_topic_mentions_cat("nyu_reddit_full.sqlite")

tabs = st.tabs([pretty[c] for c in categories])

for cat, tab in zip(categories, tabs):
    with tab:
        dfc = mcat[mcat["category"]==cat]
        terms = sorted(dfc["term"].unique().tolist())
        selected = st.multiselect(f"Terms in {pretty[cat]}", terms, 
                                 default=terms[:min(5,len(terms))])
        
        if selected:
            plot_df = dfc[dfc["term"].isin(selected)]
            fig = px.line(plot_df, x="day", y="count", color="term", markers=True)
            st.plotly_chart(fig, use_container_width=True)
```

---

### Section 6: Flagged Content Review
**Location:** Lines 352-396

**Method:**
```python
flagged_df = df_filtered.sort_values('hate_speech', ascending=False)

for idx, row in flagged_df.iterrows():
    title, body, author = extract_original_content(row)
    
    with st.expander(f"🚩 {row['item_type'].title()} by {author} - {row['flag_reasons']}"):
        # Display original content, cleaned text, scores, metadata
        st.write(f"**Title:** {title}")
        st.write(f"**Body:** {body}")
        st.write(f"**Cleaned Text:** {row['text_cleaned']}")
        
        # Display scores with color coding
        for label in toxicity_labels:
            score = row[label]
            if score >= threshold:
                st.write(f"🔴 **{label}:** {score:.3f}")
```

---

## 🔄 Refresh Mechanism

### Manual Refresh Button
**Location:** Lines 124-129

```python
col1, col2 = st.columns([1, 4])
with col1:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()  # Clears all cached data
        st.rerun()  # Reloads the entire app
```

### Automatic Cache Expiration
**Location:** Line 40

```python
@st.cache_data(ttl=60)  # Time-to-live: 60 seconds
def load_data():
    # After 60 seconds, cache expires and data reloads
```

---

## 🗄️ Database Connection Pattern

All methods follow this pattern:

```python
conn = sqlite3.connect("nyu_reddit_full.sqlite")
df = pd.read_sql_query(query, conn)
conn.close()
```

**Key Points:**
- Always close connections after use
- Use pandas for SQL queries (returns DataFrames)
- Database path is hardcoded: `"nyu_reddit_full.sqlite"`

---

## 📊 Data Flow Summary

```
SQLite Database
    ↓
load_data() / load_topic_mentions() / load_topic_mentions_cat()
    ↓
Pandas DataFrame
    ↓
Streamlit Filters (date_range, content_types, terms)
    ↓
Filtered DataFrame
    ↓
Plotly Charts / Streamlit Components
    ↓
Interactive Dashboard
```

---

## 🎯 Key Takeaways

1. **Three main data loaders:**
   - `load_data()` - Flagged toxicity items
   - `load_topic_mentions()` - Topic mentions
   - `load_topic_mentions_cat()` - Category mentions

2. **Caching strategy:**
   - Main data: 60-second TTL
   - Topic data: Session-based (no expiration)
   - Manual refresh: Button clears cache

3. **Data joins:**
   - `toxicity_classifications` JOIN `posts`/`comments`
   - Uses LEFT JOIN to preserve all classifications
   - Filters for `hate_speech >= 0.20`

4. **Visualization libraries:**
   - Plotly Express (`px.line`, `px.histogram`)
   - Plotly Graph Objects (`go.Scatter`, `go.Histogram`)
   - Streamlit components (`st.metric`, `st.plotly_chart`)

5. **Filtering happens client-side:**
   - Load all data once
   - Apply filters in Streamlit
   - No database re-queries needed

