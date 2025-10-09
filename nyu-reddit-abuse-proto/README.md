# NYU Reddit Data Collection — AWS-Powered Pipeline

A scalable, cloud-based system for collecting and storing Reddit data from university subreddits using AWS S3 and DynamoDB.

## 🏗️ Architecture

- **Data Collection**: Reddit API (PRAW) → AWS S3 (JSON files)
- **Metadata Storage**: DynamoDB for searchable indexing
- **Storage**: Individual JSON files per post/comment
- **Scalability**: Cloud-based, unlimited storage

## 📁 Clean Codebase Structure

```
nyu-reddit-abuse-proto/
├── .env.example          # Configuration template
├── README.md            # This file
├── requirements.txt     # Python dependencies
├── src/
│   ├── config.py        # Environment configuration
│   ├── aws_utils.py     # AWS S3/DynamoDB utilities
│   └── ingest_s3.py     # Main data collection script
├── dashboard/
│   └── app.py          # Streamlit dashboard (legacy)
├── browse_data.py      # Quick data overview
└── view_db.py         # Interactive database viewer
```

## 🚀 Quick Start

### 1. Setup

```bash
# Clone and setup
git clone <repo>
cd nyu-reddit-abuse-proto

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure AWS & Reddit

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials:
# - Reddit API credentials
# - AWS access keys
# - S3 bucket name
```

### 3. Collect Data

```bash
# Collect last 7 days of r/nyu data
python src/ingest_s3.py --days 7

# Collect specific number of days
python src/ingest_s3.py --days 14
```

### 4. View Data

```bash
# Quick overview
python browse_data.py

# Interactive viewer
python view_db.py
```

### 5. Local Database (For Collaborators)

```bash
# Export AWS data to local SQLite (requires AWS access)
python src/export_to_local_db.py

# This creates nyu_reddit_local.sqlite with all collected data
# Collaborators can work with this database without AWS credentials
```

## 📊 Current Dataset

- **Posts**: 55 posts from r/nyu
- **Comments**: 491 comments
- **Authors**: 270 unique users
- **Time Range**: 8 days (Oct 2-9, 2025)
- **Storage**: 546 JSON files in S3

## 🔧 Core Components

### `src/ingest_s3.py`

Main data collection script that:

- Fetches posts/comments from Reddit
- Stores individual JSON files in S3
- Maintains metadata in DynamoDB
- Handles AWS resource creation

### `src/aws_utils.py`

AWS utilities for:

- S3 bucket management
- DynamoDB table operations
- Data upload/storage
- Connection testing

### `src/config.py`

Centralized configuration:

- Environment variable loading
- AWS and Reddit settings
- Default values

### `src/export_to_local_db.py`

Local database export utility:

- Exports AWS data to SQLite for collaboration
- Converts DynamoDB data types to SQLite format
- Includes raw JSON data from S3
- Enables offline development

## 📈 Data Structure

### S3 Storage

```
s3://your-bucket/raw-data/
├── 2025/10/09/post/1o28m8k.json
├── 2025/10/09/comment/nimbupe.json
└── ...
```

### JSON Format

```json
{
  "id": "1o28m8k",
  "type": "post",
  "subreddit": "nyu",
  "author": "nyunews",
  "score": 6,
  "timestamp": "2025-10-09T15:14:23+00:00",
  "raw_data": {
    "title": "NSA withdraws from NYU Tandon career fair",
    "body": "...",
    "url": "...",
    "permalink": "..."
  }
}
```

## 🎯 Use Cases

- **Research**: University subreddit analysis
- **Monitoring**: Community sentiment tracking
- **Analytics**: Engagement pattern analysis
- **Data Science**: Large-scale social media datasets

## 🔄 Scaling Options

- **Multi-Subreddit**: Add more university subreddits
- **Real-time**: Stream processing with Kinesis
- **Analytics**: Athena/Redshift for big data queries
- **ML Pipeline**: SageMaker for content analysis

## 📝 Notes

- **Reddit API**: Respects rate limits and terms of service
- **AWS Costs**: Pay-per-use S3 and DynamoDB
- **Data Privacy**: Stores public Reddit data only
- **Scalability**: Designed for production workloads
