#!/usr/bin/env python3
import sqlite3, json, re
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, Pattern
import argparse

DB_PATH = "nyu_reddit_local.sqlite"

# Legacy terms for backward compatibility
RAW_TERMS = {
    "black":        r"\bblack(s)?\b",
    "asian":        r"\basian(s)?\b",
    "white":        r"\bwhite(s)?\b",
    "racism":       r"\bracism\b|\bracist(s)?\b",
    "financial aid":r"\bfinancial[\s\-]?aid\b",
}

# Category-based terms configuration
CATEGORY_TERMS = {
    "race_ethnicity": {
        "black":  r"\bblack(s)?\b(?!\s*board)",
        "white":  r"\bwhite(s)?\b(?!\s*board)",
        "asian":  r"\basian(s)?\b",
        "latino": r"\blatino(s)?\b|\blatinx\b",
        "hispanic": r"\bhispanic(s)?\b",
        "arab":   r"\barab(s)?\b",
        "african": r"\bafrican(s)?\b",
    },
    "countries": {
        "china":  r"\bchina\b|\bchinese\b",
        "india":  r"\bindia\b|\bindian(s)?\b",
        "united states": r"\b(united states|usa|u\.s\.a\.|america|american(s)?)\b",
        "korea":  r"\b(south\s+)?korea(n|ns)?\b|\bnorth\s+korea\b",
        "mexico": r"\bmexico\b|\bmexican(s)?\b",
        "turkey": r"\bturkey\b|\bturk(s)?\b",
        "russia": r"\brussia\b|\brussian(s)?\b",
    },
    "gender_sexuality": {
        "women":  r"\bwom[ae]n\b|\bwomen\b",
        "men":    r"\bmen\b|\bman\b",
        "female": r"\bfemale(s)?\b",
        "male":   r"\bmale(s)?\b",
        "trans":  r"\btrans(gender|sexual)?\b",
        "lgbtq":  r"\blgbt(q|\+)?\b|\bgay\b|\blesbian(s)?\b|\bqueer\b",
    },
    "profanity": {
        "fuck":   r"\bfuck(ing|er|s)?\b",
        "shit":   r"\bshit(ty|s)?\b",
        "bitch":  r"\bbitch(es)?\b",
        "asshole":r"\basshole(s)?\b",
        "bastard":r"\bbastard(s)?\b",
        "dumbass":r"\bdumbass(es)?\b",
    },
    "academics_finance": {
        "financial aid": r"\bfinancial[\s\-]?aid\b",
        "scholarship":   r"\bscholarship(s)?\b",
        "tuition":       r"\btuition\b",
        "fafsa":         r"\bfafsa\b",
        "loan":          r"\b(student[\s\-]?)?loan(s)?\b",
    },
    "safety_crime": {
        "assault": r"\bassault(ed|s|ing)?\b",
        "robbery": r"\brobber(y|ies)\b",
        "police":  r"\bpolice\b|\bnypd\b",
        "crime":   r"\bcrime(s)?\b",
    },
    "housing": {
        "housing": r"\bhousing\b",
        "dorm":    r"\bdorm(s)?\b",
        "rent":    r"\brent(ed|ing|s)?\b|\brental\b",
        "lease":   r"\blease(d|s|ing)?\b",
        "landlord":r"\blandlord(s)?\b",
    },
}

# Compile all patterns
TERMS: Dict[str, Pattern] = {k: re.compile(v, re.IGNORECASE) for k, v in RAW_TERMS.items()}
CATEGORY_PATTERNS = {
    category: {term: re.compile(pattern, re.IGNORECASE) for term, pattern in terms.items()}
    for category, terms in CATEGORY_TERMS.items()
}

def _norm_text(s: str) -> str:
    if not s: return ""
    s = re.sub(r"http\S+|www\.\S+", " ", s)             # remove URLs
    s = re.sub(r"u/[A-Za-z0-9_-]+|@[A-Za-z0-9_-]+", " ", s)  # usernames
    s = re.sub(r"\[.*?\]\(.*?\)", " ", s)               # markdown links
    s = re.sub(r"&[a-z]+;", " ", s)                     # html entities
    s = re.sub(r"\s+", " ", s).strip()
    return s.lower()

def _is_deleted_or_removed(text: str) -> bool:
    t = (text or "").strip().lower()
    return t in ("[deleted]", "[removed]")

def ensure_table(conn: sqlite3.Connection):
    # Legacy table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS topic_mentions_daily (
          day TEXT,
          term TEXT,
          count INTEGER,
          total_items INTEGER,
          rate_per_1k REAL,
          PRIMARY KEY(day, term)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_day ON topic_mentions_daily(day)")
    
    # New category table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS topic_mentions_cat_daily (
          day TEXT,
          category TEXT,
          term TEXT,
          count INTEGER,
          total_items INTEGER,
          rate_per_1k REAL,
          PRIMARY KEY(day, category, term)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_cat_day ON topic_mentions_cat_daily(day)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_cat_category ON topic_mentions_cat_daily(category)")
    conn.commit()

def iter_posts(conn):
    for (id_, raw_json, created_utc) in conn.execute("SELECT id, raw_json, created_utc FROM posts"):
        try:
            r = json.loads(raw_json)
            rd = (r.get("raw_data") or {})
            title = rd.get("title", "") or ""
            body  = rd.get("body", "") or ""
            if _is_deleted_or_removed(body): 
                text = title  # keep title if body removed
            else:
                text = (title + " " + body).strip()
        except: 
            text = ""
        yield created_utc, text

def iter_comments(conn):
    for (id_, raw_json, created_utc) in conn.execute("SELECT id, raw_json, created_utc FROM comments"):
        try:
            r = json.loads(raw_json)
            body = (r.get("raw_data") or {}).get("body", "") or ""
            if _is_deleted_or_removed(body):
                text = ""
            else:
                text = body
        except:
            text = ""
        yield created_utc, text

def main():
    parser = argparse.ArgumentParser(description="Compute topic mentions from Reddit data")
    parser.add_argument("--rebuild", action="store_true", help="Truncate tables before inserting")
    args = parser.parse_args()
    
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)
    
    if args.rebuild:
        conn.execute("DELETE FROM topic_mentions_daily")
        conn.execute("DELETE FROM topic_mentions_cat_daily")
        conn.commit()
        print("🗑️  Cleared existing topic mention tables")

    # day → total items seen (posts+comments with non-empty text)
    totals = defaultdict(int)
    # (day, term) → count of items mentioning term (binary per item) - legacy
    counts = defaultdict(int)
    # (day, category, term) → count of items mentioning term (binary per item) - categories
    cat_counts = defaultdict(int)

    def handle(created_utc, raw_text):
        if not raw_text: return
        text = _norm_text(raw_text)
        if not text: return
        day = datetime.fromtimestamp(int(created_utc), tz=timezone.utc).date().isoformat()
        totals[day] += 1
        
        # Legacy counting
        for term, pat in TERMS.items():
            if pat.search(text):
                counts[(day, term)] += 1
        
        # Category-based counting
        for category, patterns in CATEGORY_PATTERNS.items():
            for term, pat in patterns.items():
                if pat.search(text):
                    cat_counts[(day, category, term)] += 1

    # Stream through DB
    print("📊 Processing posts and comments...")
    for created_utc, text in iter_posts(conn):    
        handle(created_utc, text)
    for created_utc, text in iter_comments(conn): 
        handle(created_utc, text)

    # Upsert legacy results
    legacy_rows = []
    for (day, term), cnt in counts.items():
        total = totals.get(day, 0) or 1
        rate = 1000.0 * cnt / total
        legacy_rows.append((day, term, cnt, total, rate))

    conn.executemany("""
        INSERT INTO topic_mentions_daily(day, term, count, total_items, rate_per_1k)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(day, term) DO UPDATE SET
          count=excluded.count,
          total_items=excluded.total_items,
          rate_per_1k=excluded.rate_per_1k
    """, legacy_rows)
    
    # Upsert category results
    cat_rows = []
    for (day, category, term), cnt in cat_counts.items():
        total = totals.get(day, 0) or 1
        rate = 1000.0 * cnt / total
        cat_rows.append((day, category, term, cnt, total, rate))

    conn.executemany("""
        INSERT INTO topic_mentions_cat_daily(day, category, term, count, total_items, rate_per_1k)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(day, category, term) DO UPDATE SET
          count=excluded.count,
          total_items=excluded.total_items,
          rate_per_1k=excluded.rate_per_1k
    """, cat_rows)
    
    conn.commit()
    print(f"✅ topic_mentions_daily upserted: {len(legacy_rows)} rows")
    print(f"✅ topic_mentions_cat_daily upserted: {len(cat_rows)} rows")
    
    # Print summary
    if cat_rows:
        print("\n📈 Category Summary:")
        cat_summary = defaultdict(int)
        for (day, category, term), cnt in cat_counts.items():
            cat_summary[category] += cnt
        
        for category, total in sorted(cat_summary.items()):
            print(f"  {category}: {total} mentions")
    
    conn.close()

if __name__ == "__main__":
    main()
