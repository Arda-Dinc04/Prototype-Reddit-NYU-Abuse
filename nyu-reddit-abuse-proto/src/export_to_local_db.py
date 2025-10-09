#!/usr/bin/env python3
"""
Export AWS data to local SQLite database for collaboration
"""

import sqlite3
import json
import sys
import os
from datetime import datetime
from typing import List, Dict
from decimal import Decimal

# Add src to path for imports
sys.path.append(os.path.dirname(__file__))

import boto3
from config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, 
    S3_BUCKET_NAME, DYNAMODB_TABLE_NAME
)

def convert_dynamodb_value(value):
    """Convert DynamoDB types to SQLite-compatible types"""
    if isinstance(value, Decimal):
        return int(value)
    elif isinstance(value, dict):
        return json.dumps(value)
    elif value is None:
        return None
    else:
        return value

def create_local_db_schema(db_path: str):
    """Create SQLite database with proper schema"""
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
            timestamp TEXT,
            FOREIGN KEY (parent_id) REFERENCES comments(id),
            FOREIGN KEY (link_id) REFERENCES posts(id)
        )
    """)
    
    # Create indexes for better performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_utc)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_created ON comments(created_utc)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_link ON comments(link_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_id)")
    
    conn.commit()
    conn.close()
    print(f"✅ Created local database schema at {db_path}")

def export_dynamodb_to_local(db_path: str):
    """Export all data from DynamoDB to local SQLite"""
    print("📊 Exporting data from DynamoDB to local SQLite...")
    
    # Connect to DynamoDB
    dynamodb = boto3.resource(
        'dynamodb', 
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    # Get all items
    response = table.scan()
    items = response['Items']
    
    # Handle pagination
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    
    print(f"📦 Found {len(items)} items in DynamoDB")
    
    # Connect to local SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    posts_count = 0
    comments_count = 0
    
    for item in items:
        item_type = item.get('type', 'unknown')
        
        if item_type == 'post':
            cursor.execute("""
                INSERT OR REPLACE INTO posts 
                (id, author, created_utc, title, body, score, num_comments, 
                 url, permalink, subreddit, raw_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                convert_dynamodb_value(item.get('id')),
                convert_dynamodb_value(item.get('author')),
                convert_dynamodb_value(item.get('created_utc')),
                convert_dynamodb_value(item.get('title')),
                convert_dynamodb_value(item.get('body')),
                convert_dynamodb_value(item.get('score')),
                convert_dynamodb_value(item.get('num_comments')),
                convert_dynamodb_value(item.get('url')),
                convert_dynamodb_value(item.get('permalink')),
                convert_dynamodb_value(item.get('subreddit')),
                convert_dynamodb_value(item.get('raw_json')),
                convert_dynamodb_value(item.get('timestamp'))
            ))
            posts_count += 1
            
        elif item_type == 'comment':
            cursor.execute("""
                INSERT OR REPLACE INTO comments 
                (id, parent_id, link_id, author, created_utc, body, score, 
                 subreddit, raw_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                convert_dynamodb_value(item.get('id')),
                convert_dynamodb_value(item.get('parent_id')),
                convert_dynamodb_value(item.get('link_id')),
                convert_dynamodb_value(item.get('author')),
                convert_dynamodb_value(item.get('created_utc')),
                convert_dynamodb_value(item.get('body')),
                convert_dynamodb_value(item.get('score')),
                convert_dynamodb_value(item.get('subreddit')),
                convert_dynamodb_value(item.get('raw_json')),
                convert_dynamodb_value(item.get('timestamp'))
            ))
            comments_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"✅ Exported {posts_count} posts and {comments_count} comments")
    return posts_count, comments_count

def export_s3_raw_data(db_path: str):
    """Export raw JSON data from S3 to local database"""
    print("📦 Exporting raw JSON data from S3...")
    
    # Connect to S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    # Connect to local SQLite
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # List all objects in S3
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix='raw-data/')
    
    posts_updated = 0
    comments_updated = 0
    
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            
            # Get the raw JSON data
            try:
                response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                raw_json = response['Body'].read().decode('utf-8')
                
                # Parse the JSON to get the ID
                data = json.loads(raw_json)
                item_id = data.get('id')
                item_type = data.get('type')
                
                if item_type == 'post':
                    cursor.execute("""
                        UPDATE posts SET raw_json = ? WHERE id = ?
                    """, (raw_json, item_id))
                    posts_updated += 1
                    
                elif item_type == 'comment':
                    cursor.execute("""
                        UPDATE comments SET raw_json = ? WHERE id = ?
                    """, (raw_json, item_id))
                    comments_updated += 1
                    
            except Exception as e:
                print(f"⚠️  Error processing {key}: {e}")
                continue
    
    conn.commit()
    conn.close()
    
    print(f"✅ Updated raw JSON for {posts_updated} posts and {comments_updated} comments")

def main():
    """Main export function"""
    db_path = "nyu_reddit_local.sqlite"
    
    print("🚀 Starting AWS to Local Database Export")
    print("=" * 50)
    
    # Create database schema
    create_local_db_schema(db_path)
    
    # Export DynamoDB data
    posts_count, comments_count = export_dynamodb_to_local(db_path)
    
    # Export S3 raw data
    export_s3_raw_data(db_path)
    
    # Show final statistics
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM posts")
    final_posts = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM comments")
    final_comments = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(created_utc), MAX(created_utc) FROM posts")
    date_range = cursor.fetchone()
    
    conn.close()
    
    print("\n🎉 Export Complete!")
    print("=" * 30)
    print(f"📊 Final Database Statistics:")
    print(f"   Posts: {final_posts}")
    print(f"   Comments: {final_comments}")
    print(f"   Total: {final_posts + final_comments}")
    
    if date_range[0] and date_range[1]:
        start_date = datetime.fromtimestamp(date_range[0]).strftime('%Y-%m-%d')
        end_date = datetime.fromtimestamp(date_range[1]).strftime('%Y-%m-%d')
        print(f"   Date Range: {start_date} to {end_date}")
    
    print(f"\n📁 Local database created: {db_path}")
    print("✅ Your collaborators can now use this database without AWS access!")

if __name__ == "__main__":
    main()
