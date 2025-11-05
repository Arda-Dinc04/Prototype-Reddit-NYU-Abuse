# Complete Workflow: Classification + Dashboard

## Step 1: Run Toxicity Classification Model

This will process all posts and comments in the database and classify them for toxicity:

```bash
python src/classify_toxicity_hatebert.py --db nyu_reddit_full.sqlite
```

**What it does:**
- Loads HateBERT model (GroNLP/hate-bert-base-uncased)
- Processes all posts and comments from `nyu_reddit_full.sqlite`
- Stores results in `toxicity_classifications` table
- Prints progress every 50 items

**Expected time:** ~10-15 minutes for ~6,000 items

**Monitor progress:**
```bash
python -c "import sqlite3; conn = sqlite3.connect('nyu_reddit_full.sqlite'); c = conn.cursor(); classified = c.execute('SELECT COUNT(*) FROM toxicity_classifications').fetchone()[0]; flagged = c.execute('SELECT COUNT(*) FROM toxicity_classifications WHERE hate_speech >= 0.20').fetchone()[0]; print(f'Progress: {classified} items classified, {flagged} flagged'); conn.close()"
```

## Step 2: Run Topic Mentions Analysis (Optional)

If you want topic analysis in the dashboard:

```bash
python src/compute_topic_mentions.py --db nyu_reddit_full.sqlite
```

## Step 3: Launch Streamlit Dashboard

The dashboard will automatically load the classified data:

```bash
streamlit run dashboard/toxicity_dashboard.py
```

**Dashboard features:**
- View all flagged items (hate_speech ≥ 0.20)
- Filter by date range, content type
- View toxicity scores and reasons
- See trends over time
- Topic mentions analysis

## Quick One-Liner (All Steps)

```bash
# Run classification
python src/classify_toxicity_hatebert.py --db nyu_reddit_full.sqlite && \
# Run topic analysis
python src/compute_topic_mentions.py --db nyu_reddit_full.sqlite && \
# Launch dashboard
streamlit run dashboard/toxicity_dashboard.py
```

## Database Location

- **Main database:** `nyu_reddit_full.sqlite`
- **Classification results:** Stored in `toxicity_classifications` table
- **Dashboard:** Automatically reads from `nyu_reddit_full.sqlite`

