# 📊 Complete Data Pipeline: From Reddit to Streamlit Dashboard

This guide explains how data flows from Reddit collection through classification to visualization in your Streamlit dashboard.

## 🔄 Complete Data Flow

```
Reddit API / Pushshift Dumps
    ↓
[1] Data Collection (pull_reddit_to_db.py)
    ↓
SQLite Database (nyu_reddit_full.sqlite)
    ├── posts table
    └── comments table
    ↓
[2] Toxicity Classification (classify_toxicity_hatebert.py)
    ↓
toxicity_classifications table
    ↓
[3] Topic Analysis (compute_topic_mentions.py)
    ↓
topic_mentions_daily table
topic_mentions_cat_daily table
    ↓
[4] Streamlit Dashboard (toxicity_dashboard.py)
    ↓
Interactive Visualizations & Analysis
```

---

## 📋 Step-by-Step Methods

### **Step 1: Data Collection** → SQLite Database

**Script:** `src/pull_reddit_to_db.py`

**Method:**

```bash
python src/pull_reddit_to_db.py --limit 100 --db nyu_reddit_full.sqlite
```

**What it does:**

- Pulls recent Reddit posts and comments from r/nyu
- Stores raw JSON data in `posts` and `comments` tables
- Prevents duplicates using existing ID checks
- Handles rate limiting automatically

**Database Tables Created:**

- `posts` - Contains post data with `id`, `raw_json`, `created_utc`, `score`, `author`
- `comments` - Contains comment data with `id`, `raw_json`, `created_utc`, `score`, `author`

**Key Functions:**

- `pull_reddit_to_db()` - Main function that orchestrates data collection
- `store_post()` - Stores individual posts
- `store_comment()` - Stores individual comments
- `get_existing_ids()` - Prevents duplicates

---

### **Step 2: Toxicity Classification** → Classification Scores

**Script:** `src/classify_toxicity_hatebert.py`

**Method:**

```bash
python src/classify_toxicity_hatebert.py --db nyu_reddit_full.sqlite
```

**What it does:**

- Loads DehateBERT model (`Hate-speech-CNERG/dehatebert-mono-english`)
- Processes all posts and comments from the database
- Classifies each item for hate speech
- Stores scores in `toxicity_classifications` table

**Database Table Created:**

- `toxicity_classifications` - Contains:
  - `id` - Post/comment ID
  - `item_type` - 'post' or 'comment'
  - `text_cleaned` - Cleaned text used for classification
  - `non_hate` - Probability score (0.0-1.0)
  - `hate_speech` - Probability score (0.0-1.0)
  - `is_deleted`, `is_removed`, `is_empty` - Flags
  - `classification_timestamp` - When classification ran

**Key Functions:**

- `process_all_items()` - Main processing function
- `classify_toxicity_batch()` - Batch classification for efficiency
- `clean_text()` - Text preprocessing
- `get_parent_text()` - Gets parent context for comments

**Classification Threshold:**

- Items with `hate_speech >= 0.20` are flagged

---

### **Step 3: Topic Analysis** → Topic Mentions

**Script:** `src/compute_topic_mentions.py`

**Method:**

```bash
python src/compute_topic_mentions.py --db nyu_reddit_full.sqlite
```

**What it does:**

- Scans all posts and comments for topic keywords
- Counts mentions per day
- Categorizes topics (race/ethnicity, countries, gender/sexuality, etc.)
- Stores daily aggregated counts

**Database Tables Created:**

- `topic_mentions_daily` - Daily counts per term:

  - `term` - Topic keyword (e.g., "black", "asian", "racism")
  - `day` - Date
  - `count` - Number of items mentioning the term
  - `item_type` - 'post' or 'comment'

- `topic_mentions_cat_daily` - Daily counts per category:
  - `category` - Category name (e.g., "race_ethnicity")
  - `term` - Specific term within category
  - `day` - Date
  - `count` - Number of mentions

**Key Functions:**

- `compute_topic_mentions()` - Main computation function
- `extract_text()` - Extracts text from raw JSON
- `match_terms()` - Matches keywords using regex

---

### **Step 4: Dashboard Display** → Streamlit Visualization

**Script:** `dashboard/toxicity_dashboard.py`

**Method:**

```bash
streamlit run dashboard/toxicity_dashboard.py
```

**What it does:**

- Loads data from `nyu_reddit_full.sqlite`
- Displays flagged content (hate_speech ≥ 0.20)
- Shows toxicity trends over time
- Visualizes topic mentions
- Provides interactive filters

**Key Functions:**

#### `load_data()` - Main Data Loader

```python
@st.cache_data(ttl=60)  # Caches for 60 seconds
def load_data():
    # Joins toxicity_classifications with posts/comments
    # Filters for hate_speech >= 0.20
    # Returns DataFrame with all flagged items
```

**SQL Query Used:**

```sql
SELECT
    tc.*,
    CASE WHEN tc.item_type = 'post' THEN p.raw_json ELSE c.raw_json END as raw_json,
    CASE WHEN tc.item_type = 'post' THEN p.created_utc ELSE c.created_utc END as created_utc,
    CASE WHEN tc.item_type = 'post' THEN p.score ELSE c.score END as score
FROM toxicity_classifications tc
LEFT JOIN posts p ON tc.id = p.id AND tc.item_type = 'post'
LEFT JOIN comments c ON tc.id = c.id AND tc.item_type = 'comment'
WHERE tc.hate_speech >= 0.20
ORDER BY tc.hate_speech DESC
```

#### `load_topic_mentions()` - Topic Data Loader

```python
@st.cache_data
def load_topic_mentions(db_path: str):
    # Loads topic_mentions_daily table
    # Returns DataFrame with daily topic counts
```

#### `load_topic_mentions_cat()` - Category Data Loader

```python
@st.cache_data
def load_topic_mentions_cat(db_path: str):
    # Loads topic_mentions_cat_daily table
    # Returns DataFrame with daily category counts
```

**Dashboard Sections:**

1. **Metrics Cards:**

   - Total Flagged Items
   - High Hate (≥0.50)
   - Medium Hate (0.30-0.50)
   - Low Hate (0.20-0.30)

2. **Toxicity Score Distribution:**

   - Histograms for `non_hate` and `hate_speech` scores
   - Threshold lines at 0.20

3. **Daily Toxicity Trends:**

   - Line chart showing flagged items over time

4. **Topic Mentions Over Time:**

   - Interactive line charts for selected terms
   - Date range filtering

5. **Topic Categories:**

   - Tabbed interface for different categories
   - Race/Ethnicity, Countries, Gender/Sexuality, etc.

6. **Flagged Content Review:**

   - Expandable cards for each flagged item
   - Shows original content, cleaned text, scores, metadata

7. **Summary Statistics:**
   - Content type breakdown
   - Toxicity label statistics

---

## 🔄 Complete Workflow Example

### **Scenario: Adding New Data to Dashboard**

```bash
# 1. Pull new Reddit data
python src/pull_reddit_to_db.py --limit 100 --db nyu_reddit_full.sqlite

# 2. Classify toxicity
python src/classify_toxicity_hatebert.py --db nyu_reddit_full.sqlite

# 3. Compute topic mentions
python src/compute_topic_mentions.py --db nyu_reddit_full.sqlite

# 4. Launch dashboard (or refresh if already running)
streamlit run dashboard/toxicity_dashboard.py
```

**In Dashboard:**

- Click "🔄 Refresh Data" button to clear cache
- Dashboard automatically reloads with new data
- Cache expires after 60 seconds automatically

---

## 🗄️ Database Schema Overview

### **Core Tables:**

```sql
-- Posts from Reddit
CREATE TABLE posts (
    id TEXT PRIMARY KEY,
    raw_json TEXT,
    created_utc INTEGER,
    score INTEGER,
    author TEXT
);

-- Comments from Reddit
CREATE TABLE comments (
    id TEXT PRIMARY KEY,
    raw_json TEXT,
    created_utc INTEGER,
    score INTEGER,
    author TEXT,
    parent_id TEXT,
    link_id TEXT
);

-- Toxicity classifications
CREATE TABLE toxicity_classifications (
    id TEXT PRIMARY KEY,
    item_type TEXT CHECK(item_type IN ('post', 'comment')),
    text_cleaned TEXT,
    non_hate REAL DEFAULT 0.0,
    hate_speech REAL DEFAULT 0.0,
    is_deleted INTEGER DEFAULT 0,
    is_removed INTEGER DEFAULT 0,
    is_empty INTEGER DEFAULT 0,
    classification_timestamp TEXT
);

-- Daily topic mentions
CREATE TABLE topic_mentions_daily (
    term TEXT,
    day DATE,
    count INTEGER,
    item_type TEXT,
    PRIMARY KEY (term, day, item_type)
);

-- Daily category mentions
CREATE TABLE topic_mentions_cat_daily (
    category TEXT,
    term TEXT,
    day DATE,
    count INTEGER,
    PRIMARY KEY (category, term, day)
);
```

---

## 🔧 Dashboard Configuration

### **Database Path:**

- **Current:** `nyu_reddit_full.sqlite` (line 44)
- **To change:** Edit `dashboard/toxicity_dashboard.py` line 44

### **Caching:**

- **Data cache:** 60 seconds (`ttl=60`)
- **Topic cache:** No expiration (loads once per session)
- **Manual refresh:** Click "🔄 Refresh Data" button

### **Filters:**

- **Date Range:** Sidebar date picker
- **Content Type:** Posts, Comments, or Both
- **Topic Terms:** Multi-select dropdown
- **Topic Categories:** Tabbed interface

---

## 🚀 Quick Start Commands

### **Full Pipeline (First Time):**

```bash
# 1. Collect data
python src/pull_reddit_to_db.py --limit 100 --db nyu_reddit_full.sqlite

# 2. Classify
python src/classify_toxicity_hatebert.py --db nyu_reddit_full.sqlite

# 3. Topics
python src/compute_topic_mentions.py --db nyu_reddit_full.sqlite

# 4. Dashboard
streamlit run dashboard/toxicity_dashboard.py
```

### **Update Existing Data:**

```bash
# Pull more recent posts
python src/pull_reddit_to_db.py --limit 200 --db nyu_reddit_full.sqlite

# Re-classify everything (includes new items)
python src/classify_toxicity_hatebert.py --db nyu_reddit_full.sqlite

# Re-compute topics (includes new items)
python src/compute_topic_mentions.py --db nyu_reddit_full.sqlite

# Refresh dashboard (click button or wait 60 seconds)
```

---

## 📊 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    REDDIT DATA SOURCES                      │
│  • PRAW API (recent posts/comments)                        │
│  • Pushshift Dumps (historical data)                       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│         STEP 1: pull_reddit_to_db.py                         │
│  • Fetches posts/comments                                    │
│  • Prevents duplicates                                       │
│  • Stores raw JSON in SQLite                                 │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              nyu_reddit_full.sqlite                         │
│  ┌──────────────┐  ┌──────────────┐                        │
│  │    posts     │  │   comments    │                        │
│  │  (raw_json)  │  │  (raw_json)   │                        │
│  └──────────────┘  └──────────────┘                        │
└────────────────────┬────────────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         │                       │
         ▼                       ▼
┌──────────────────┐   ┌──────────────────────┐
│ STEP 2:          │   │ STEP 3:              │
│ classify_toxicity │   │ compute_topic_       │
│ _hatebert.py      │   │ mentions.py          │
│                   │   │                      │
│ • DehateBERT      │   │ • Keyword matching   │
│ • Batch process   │   │ • Daily aggregation  │
│ • Store scores    │   │ • Category grouping  │
└────────┬──────────┘   └──────────┬───────────┘
         │                         │
         ▼                         ▼
┌──────────────────┐   ┌──────────────────────┐
│ toxicity_        │   │ topic_mentions_      │
│ classifications  │   │ daily               │
│                  │   │ topic_mentions_     │
│ • hate_speech    │   │ cat_daily           │
│ • non_hate       │   │                      │
│ • text_cleaned   │   │ • term counts        │
└────────┬──────────┘   │ • category counts   │
         │              └──────────┬───────────┘
         │                         │
         └───────────┬─────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│         STEP 4: toxicity_dashboard.py                       │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  load_data()                                         │  │
│  │  • JOINs toxicity_classifications + posts/comments  │  │
│  │  • Filters hate_speech >= 0.20                      │  │
│  │  • Returns DataFrame                                 │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Visualizations:                                    │  │
│  │  • Metrics cards                                    │  │
│  │  • Score distributions                              │  │
│  │  • Daily trends                                    │  │
│  │  • Topic mentions                                  │  │
│  │  • Flagged content review                          │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## ✅ Verification Checklist

After running each step, verify:

1. **Data Collection:**

   ```bash
   sqlite3 nyu_reddit_full.sqlite "SELECT COUNT(*) FROM posts;"
   sqlite3 nyu_reddit_full.sqlite "SELECT COUNT(*) FROM comments;"
   ```

2. **Classification:**

   ```bash
   sqlite3 nyu_reddit_full.sqlite "SELECT COUNT(*) FROM toxicity_classifications;"
   sqlite3 nyu_reddit_full.sqlite "SELECT COUNT(*) FROM toxicity_classifications WHERE hate_speech >= 0.20;"
   ```

3. **Topics:**

   ```bash
   sqlite3 nyu_reddit_full.sqlite "SELECT COUNT(*) FROM topic_mentions_daily;"
   sqlite3 nyu_reddit_full.sqlite "SELECT COUNT(*) FROM topic_mentions_cat_daily;"
   ```

4. **Dashboard:**
   - Open http://localhost:8501
   - Check metrics cards show correct counts
   - Verify charts render properly
   - Test filters work correctly

---

## 🐛 Troubleshooting

### **Dashboard shows "No data found":**

- Check database path matches in dashboard (line 44)
- Verify `toxicity_classifications` table exists
- Run classification script first

### **Dashboard not updating:**

- Click "🔄 Refresh Data" button
- Wait 60 seconds for cache expiration
- Restart Streamlit: `Ctrl+C` then `streamlit run dashboard/toxicity_dashboard.py`

### **Missing topic data:**

- Run `python src/compute_topic_mentions.py --db nyu_reddit_full.sqlite`
- Check `topic_mentions_daily` table exists

### **Database locked errors:**

- Close dashboard before running scripts
- Or use separate database files for different processes

---

## 📝 Summary

**The complete pipeline:**

1. **Collect** → `pull_reddit_to_db.py` → `posts` & `comments` tables
2. **Classify** → `classify_toxicity_hatebert.py` → `toxicity_classifications` table
3. **Analyze** → `compute_topic_mentions.py` → `topic_mentions_*` tables
4. **Visualize** → `toxicity_dashboard.py` → Streamlit dashboard

**Key Methods:**

- `load_data()` - Main dashboard data loader (cached 60s)
- `load_topic_mentions()` - Topic data loader
- `load_topic_mentions_cat()` - Category data loader
- SQL JOINs connect classifications with original content
- Filters applied client-side in Streamlit

**Refresh Strategy:**

- Automatic: Cache expires after 60 seconds
- Manual: Click refresh button
- Full refresh: Restart Streamlit after running scripts
