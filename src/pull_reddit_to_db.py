#!/usr/bin/env python3
"""
Pull recent Reddit posts and comments directly into local SQLite database
Prevents duplicates by checking existing IDs before insertion
"""

import sqlite3
import json
import argparse
import time
from datetime import datetime, timedelta, timezone
import praw
from config import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT,
    SUBREDDIT
)

DB_PATH = "nyu_reddit_local.sqlite"
# New database for full dataset
DB_PATH_FULL = "nyu_reddit_full.sqlite"

def ensure_db_schema(db_path: str):
    """Ensure database schema exists"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create posts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            author TEXT,
            created_utc INTEGER,
            title TEXT,
            body TEXT,
            score INTEGER,
            num_comments INTEGER,
            url TEXT,
            permalink TEXT,
            subreddit TEXT,
            raw_json TEXT,
            timestamp TEXT
        )
    """)
    
    # Create comments table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            parent_id TEXT,
            link_id TEXT,
            author TEXT,
            created_utc INTEGER,
            body TEXT,
            score INTEGER,
            subreddit TEXT,
            raw_json TEXT,
            timestamp TEXT
        )
    """)
    
    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_utc)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_created ON comments(created_utc)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_link ON comments(link_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_id)")
    
    conn.commit()
    conn.close()

def get_existing_ids(db_path: str):
    """Get set of existing post and comment IDs to avoid duplicates"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get existing post IDs
    cursor.execute("SELECT id FROM posts")
    post_ids = {row[0] for row in cursor.fetchall()}
    
    # Get existing comment IDs
    cursor.execute("SELECT id FROM comments")
    comment_ids = {row[0] for row in cursor.fetchall()}
    
    conn.close()
    
    print(f"📊 Found {len(post_ids)} existing posts and {len(comment_ids)} existing comments in database")
    return post_ids, comment_ids

def store_post(conn, post, existing_ids, db_path=None):
    """Store a Reddit post in the database if it doesn't exist"""
    if post.id in existing_ids:
        return False  # Already exists
    
    try:
        # Use the provided connection (no need for separate connection)
        cursor = conn.cursor()
        timestamp = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
        
        # Prepare raw JSON data
        raw_data = {
            'id': post.id,
            'type': 'post',
            'subreddit': str(post.subreddit),
            'author': str(post.author) if post.author else None,
            'created_utc': post.created_utc,
            'timestamp': timestamp.isoformat(),
            'score': post.score,
            'raw_data': {
                'title': post.title if hasattr(post, 'title') else None,
                'body': post.selftext if hasattr(post, 'selftext') else None,
                'url': post.url if hasattr(post, 'url') else None,
                'permalink': post.permalink if hasattr(post, 'permalink') else None,
                'num_comments': post.num_comments if hasattr(post, 'num_comments') else None,
                'upvote_ratio': post.upvote_ratio if hasattr(post, 'upvote_ratio') else None,
                'is_self': post.is_self if hasattr(post, 'is_self') else None,
                'over_18': post.over_18 if hasattr(post, 'over_18') else None,
                'spoiler': post.spoiler if hasattr(post, 'spoiler') else None,
                'locked': post.locked if hasattr(post, 'locked') else None,
                'stickied': post.stickied if hasattr(post, 'stickied') else None,
            }
        }
        
        raw_json = json.dumps(raw_data, default=str)
        
        # Insert post (INSERT OR IGNORE prevents duplicate key errors)
        cursor.execute("""
            INSERT OR IGNORE INTO posts 
            (id, author, created_utc, title, body, score, num_comments, 
             url, permalink, subreddit, raw_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            post.id,
            str(post.author) if post.author else None,
            int(post.created_utc),
            post.title if hasattr(post, 'title') else None,
            post.selftext if hasattr(post, 'selftext') else None,
            post.score,
            post.num_comments if hasattr(post, 'num_comments') else None,
            post.url if hasattr(post, 'url') else None,
            post.permalink if hasattr(post, 'permalink') else None,
            str(post.subreddit),
            raw_json,
            timestamp.isoformat()
        ))
        
        return cursor.rowcount > 0
        
    except Exception as e:
        print(f"⚠️  Error storing post {post.id}: {e}")
        return False

def store_comment(conn, comment, existing_ids, db_path=None):
    """Store a Reddit comment in the database if it doesn't exist"""
    if comment.id in existing_ids:
        return False  # Already exists
    
    try:
        # Use the provided connection
        cursor = conn.cursor()
        timestamp = datetime.fromtimestamp(comment.created_utc, tz=timezone.utc)
        
        # Extract parent_id and link_id
        parent_id = None
        link_id = None
        if hasattr(comment, 'parent_id'):
            # parent_id format: t1_xxx (comment) or t3_xxx (post)
            parent_id = comment.parent_id.split('_')[-1] if comment.parent_id else None
        if hasattr(comment, 'link_id'):
            # link_id format: t3_xxx (post)
            link_id = comment.link_id.split('_')[-1] if comment.link_id else None
        
        # Prepare raw JSON data
        raw_data = {
            'id': comment.id,
            'type': 'comment',
            'subreddit': str(comment.subreddit),
            'author': str(comment.author) if comment.author else None,
            'created_utc': comment.created_utc,
            'timestamp': timestamp.isoformat(),
            'score': comment.score,
            'raw_data': {
                'body': comment.body if hasattr(comment, 'body') else None,
                'parent_id': comment.parent_id if hasattr(comment, 'parent_id') else None,
                'link_id': comment.link_id if hasattr(comment, 'link_id') else None,
            }
        }
        
        raw_json = json.dumps(raw_data, default=str)
        
        # Insert comment (INSERT OR IGNORE prevents duplicate key errors)
        cursor.execute("""
            INSERT OR IGNORE INTO comments 
            (id, parent_id, link_id, author, created_utc, body, score, 
             subreddit, raw_json, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            comment.id,
            parent_id,
            link_id,
            str(comment.author) if comment.author else None,
            int(comment.created_utc),
            comment.body if hasattr(comment, 'body') else None,
            comment.score,
            str(comment.subreddit),
            raw_json,
            timestamp.isoformat()
        ))
        
        return cursor.rowcount > 0
        
    except Exception as e:
        print(f"⚠️  Error storing comment {comment.id}: {e}")
        return False

def pull_reddit_to_db(days: int = None, limit: int = None, db_path: str = None, skip_comments: bool = False):
    """Pull recent Reddit posts and comments into local database"""
    
    # Use specified database or default to full dataset
    target_db = db_path if db_path else DB_PATH_FULL
    print(f"🚀 Starting Reddit data pull for r/{SUBREDDIT}")
    print(f"📁 Target database: {target_db}")
    if skip_comments:
        print("⚠️  Skipping comments (posts only mode)")
    
    # Initialize Reddit client
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )
    
    # Ensure database schema exists
    ensure_db_schema(target_db)
    
    # Connect to database with timeout and WAL mode for better concurrency
    conn = sqlite3.connect(target_db, timeout=30.0)
    # Enable WAL mode for better concurrent access
    conn.execute("PRAGMA journal_mode=WAL")
    
    # Get existing IDs to avoid duplicates
    existing_post_ids, existing_comment_ids = get_existing_ids(target_db)
    
    # Use max limit if not specified (Reddit API max is 1000)
    post_limit = limit if limit else 1000
    print(f"📊 Using maximum Reddit API limit: {post_limit} posts")
    
    # Calculate time threshold if days specified
    since_epoch = None
    if days:
        since = datetime.now(timezone.utc) - timedelta(days=days)
        since_epoch = since.timestamp()
        print(f"📅 Collecting data since: {since.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    else:
        print("📅 Collecting all available recent posts (max limit)")
    
    # Get subreddit
    subreddit = reddit.subreddit(SUBREDDIT)
    
    posts_added = 0
    posts_skipped = 0
    comments_added = 0
    comments_skipped = 0
    
    # Process posts
    print("\n📝 Processing posts...")
    for post in subreddit.new(limit=post_limit):
        # Skip if older than specified days
        if since_epoch and post.created_utc < since_epoch:
            break  # Posts are sorted by new, so we can stop here
        
        if post.id in existing_post_ids:
            posts_skipped += 1
            continue
        
        if store_post(conn, post, existing_post_ids):
            posts_added += 1
            existing_post_ids.add(post.id)  # Update set to avoid duplicate checks
            # Commit periodically to release locks
            if posts_added % 10 == 0:
                conn.commit()
                print(f"   Added {posts_added} posts...")
            
            # Process comments for this post (only if post was successfully stored and not skipped)
            if not skip_comments:
                try:
                    # Small delay before fetching comments to avoid rate limits
                    time.sleep(0.1)
                    
                    # Try to get comments
                    post.comments.replace_more(limit=0)  # Remove "more comments" placeholders
                    comment_list = post.comments.list()
                    
                    for comment in comment_list:
                        # Skip if older than specified days (handle None case)
                        if since_epoch and comment.created_utc and comment.created_utc < since_epoch:
                            continue
                        
                        if comment.id in existing_comment_ids:
                            comments_skipped += 1
                            continue
                        
                        if store_comment(conn, comment, existing_comment_ids):
                            comments_added += 1
                            existing_comment_ids.add(comment.id)  # Update set
                            # Commit periodically to release locks
                            if comments_added % 50 == 0:
                                conn.commit()
                                print(f"   Added {comments_added} comments...")
                except Exception as e:
                    # Don't fail the whole script if comments fail - just log and continue
                    error_msg = str(e).lower()
                    if "rate limit" in error_msg or "429" in error_msg or "ratelimit" in error_msg:
                        print(f"⚠️  Rate limited - skipping comments for post {post.id}, waiting...")
                        time.sleep(2)  # Wait a bit before continuing
                    else:
                        print(f"⚠️  Error processing comments for post {post.id}: {e}")
        
        # Small delay to avoid rate limiting (more frequent for first 100 posts)
        if posts_added % 10 == 0:
            time.sleep(0.3)
    
    # Final commit
    conn.commit()
    conn.close()
    
    print(f"\n📊 Data Pull Complete!")
    print(f"   ✅ New posts added: {posts_added}")
    print(f"   ⏭️  Posts skipped (duplicates): {posts_skipped}")
    print(f"   ✅ New comments added: {comments_added}")
    print(f"   ⏭️  Comments skipped (duplicates): {comments_skipped}")
    print(f"   📁 Database: {target_db}")

def main():
    parser = argparse.ArgumentParser(description='Pull recent Reddit posts/comments to local database')
    parser.add_argument('--days', type=int, default=None, help='Number of days to look back (default: all available, max 1000 posts)')
    parser.add_argument('--limit', type=int, default=100, help='Maximum number of posts to check (default: 100, can increase to 1000)')
    parser.add_argument('--db', type=str, default=DB_PATH_FULL, help=f'Database file path (default: {DB_PATH_FULL})')
    parser.add_argument('--posts-only', action='store_true', help='Only pull posts, skip comments (faster, avoids rate limits)')
    parser.add_argument('--skip-existing', action='store_true', default=True, help='Skip posts/comments that already exist (default: True)')
    args = parser.parse_args()
    
    try:
        pull_reddit_to_db(args.days, args.limit, args.db, skip_comments=args.posts_only)
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()

