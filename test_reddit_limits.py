#!/usr/bin/env python3
"""Test Reddit API limits for r/nyu"""
import sys
import os
sys.path.append('src')
import praw
from datetime import datetime, timezone
from config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT, SUBREDDIT

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

sr = reddit.subreddit(SUBREDDIT)

print(f"🔍 Testing Reddit API limits for r/{SUBREDDIT}")
print("=" * 60)

# Test different limits
for limit in [100, 500, 1000]:
    try:
        posts = list(sr.new(limit=limit))
        if posts:
            oldest = datetime.fromtimestamp(posts[-1].created_utc, tz=timezone.utc)
            newest = datetime.fromtimestamp(posts[0].created_utc, tz=timezone.utc)
            days_span = (newest - oldest).days
            print(f"\n✅ Limit {limit}:")
            print(f"   Retrieved: {len(posts)} posts")
            print(f"   Date range: {oldest.date()} to {newest.date()} ({days_span} days)")
        else:
            print(f"\n❌ Limit {limit}: No posts retrieved")
    except Exception as e:
        print(f"\n⚠️  Limit {limit}: Error - {e}")

# Check subreddit stats
print(f"\n📊 Subreddit Statistics:")
print(f"   Subscribers: {sr.subscribers:,}")
print(f"   Active Users: {sr.active_user_count if hasattr(sr, 'active_user_count') else 'N/A'}")

