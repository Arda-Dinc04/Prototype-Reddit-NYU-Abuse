import os
from dotenv import load_dotenv

print("Before load_dotenv():")
print(f"REDDIT_CLIENT_ID: {os.getenv('REDDIT_CLIENT_ID')}")

load_dotenv()

print("After load_dotenv():")
print(f"REDDIT_CLIENT_ID: {os.getenv('REDDIT_CLIENT_ID')}")
print(f"REDDIT_CLIENT_SECRET: {os.getenv('REDDIT_CLIENT_SECRET')}")
print(f"REDDIT_USER_AGENT: {os.getenv('REDDIT_USER_AGENT')}")

# Test the config import
from src.config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
print("\nFrom config module:")
print(f"REDDIT_CLIENT_ID: {REDDIT_CLIENT_ID}")
print(f"REDDIT_CLIENT_SECRET: {REDDIT_CLIENT_SECRET}")
print(f"REDDIT_USER_AGENT: {REDDIT_USER_AGENT}")
