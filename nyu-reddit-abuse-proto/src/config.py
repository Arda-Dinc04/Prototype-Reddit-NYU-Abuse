import os
from dotenv import load_dotenv

# Load .env file from the project root directory
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "nyu-abuse-proto/0.1")
SUBREDDIT = os.getenv("SUBREDDIT", "nyu")
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "7"))

DB_PATH = os.getenv("DB_PATH", "nyu_reddit.sqlite")

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "nyu-reddit-raw-data")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "nyu-reddit-metadata")
