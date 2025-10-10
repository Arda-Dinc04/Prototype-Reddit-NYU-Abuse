import os
from dotenv import load_dotenv

load_dotenv()

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "nyu-abuse-proto/0.1")
SUBREDDIT = os.getenv("SUBREDDIT", "nyu")
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "7"))

DB_PATH = os.getenv("DB_PATH", "nyu_reddit.sqlite")
