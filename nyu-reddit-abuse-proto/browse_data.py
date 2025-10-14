#!/usr/bin/env python3
import sys
sys.path.append('src')

import boto3
import json
from src.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME, DYNAMODB_TABLE_NAME

def quick_browse():
    """Quick browse of the database"""
    print("�� Quick Database Browse")
    print("=" * 40)
    
    # Setup clients
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
    dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    # Get all items
    response = table.scan()
    items = response['Items']
    
    print(f"📊 Total Records: {len(items)}")
    
    # Show posts
    posts = [item for item in items if item['type'] == 'post']
    print(f"📝 Posts: {len(posts)}")
    
    print("\n📝 Recent Posts:")
    for post in sorted(posts, key=lambda x: x['timestamp'], reverse=True)[:5]:
        print(f"  • {post['id']} - {post.get('title', 'No title')[:50]}...")
        print(f"    Author: {post.get('author', 'Unknown')} | Score: {post.get('score', 0)} | {post['timestamp'][:19]}")
    
    # Show comments
    comments = [item for item in items if item['type'] == 'comment']
    print(f"\n💬 Comments: {len(comments)}")
    
    print("\n💬 Recent Comments:")
    for comment in sorted(comments, key=lambda x: x['timestamp'], reverse=True)[:5]:
        print(f"  • {comment['id']} - {comment.get('author', 'Unknown')}")
        print(f"    Score: {comment.get('score', 0)} | Body: {comment.get('body_length', 0)} chars | {comment['timestamp'][:19]}")
    
    # Show authors
    authors = set([item.get('author', 'Unknown') for item in items if item.get('author')])
    print(f"\n👥 Unique Authors: {len(authors)}")
    print("Top Authors:")
    author_counts = {}
    for item in items:
        author = item.get('author', 'Unknown')
        if author != 'Unknown':
            author_counts[author] = author_counts.get(author, 0) + 1
    
    for author, count in sorted(author_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  • {author}: {count} posts/comments")
    
    # Show date distribution
    dates = {}
    for item in items:
        date = item['timestamp'][:10]
        dates[date] = dates.get(date, 0) + 1
    
    print(f"\n📅 Activity by Date:")
    for date, count in sorted(dates.items()):
        print(f"  • {date}: {count} items")

if __name__ == "__main__":
    quick_browse()
