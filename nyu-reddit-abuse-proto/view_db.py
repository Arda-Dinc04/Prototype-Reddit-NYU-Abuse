#!/usr/bin/env python3
import sys
sys.path.append('src')

import boto3
import json
from datetime import datetime
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME, DYNAMODB_TABLE_NAME

class DatabaseViewer:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        self.table = self.dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    def show_summary(self):
        """Show database summary"""
        print("📊 Database Summary")
        print("=" * 50)
        
        # DynamoDB summary
        response = self.table.scan()
        items = response['Items']
        
        posts = [item for item in items if item['type'] == 'post']
        comments = [item for item in items if item['type'] == 'comment']
        
        print(f"📝 Posts: {len(posts)}")
        print(f"💬 Comments: {len(comments)}")
        print(f"📊 Total Records: {len(items)}")
        
        # Date range
        dates = sorted(set([item['timestamp'][:10] for item in items]))
        print(f"📅 Date Range: {dates[0]} to {dates[-1]}")
        print(f"📅 Days Covered: {len(dates)} days")
        
        # S3 summary
        response = self.s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix='raw-data/')
        print(f"📦 S3 Files: {len(response.get('Contents', []))} JSON files")
    
    def list_posts(self, limit=10):
        """List recent posts"""
        print(f"\n📝 Recent Posts (showing {limit})")
        print("-" * 50)
        
        response = self.table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('type').eq('post')
        )
        
        posts = sorted(response['Items'], key=lambda x: x['timestamp'], reverse=True)
        
        for i, post in enumerate(posts[:limit]):
            print(f"{i+1}. {post['id']}")
            print(f"   Title: {post.get('title', 'No title')[:60]}...")
            print(f"   Author: {post.get('author', 'Unknown')}")
            print(f"   Score: {post.get('score', 0)}")
            print(f"   Date: {post['timestamp'][:19]}")
            print()
    
    def list_comments(self, limit=10):
        """List recent comments"""
        print(f"\n💬 Recent Comments (showing {limit})")
        print("-" * 50)
        
        response = self.table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('type').eq('comment')
        )
        
        comments = sorted(response['Items'], key=lambda x: x['timestamp'], reverse=True)
        
        for i, comment in enumerate(comments[:limit]):
            print(f"{i+1}. {comment['id']}")
            print(f"   Author: {comment.get('author', 'Unknown')}")
            print(f"   Score: {comment.get('score', 0)}")
            print(f"   Body Length: {comment.get('body_length', 0)} chars")
            print(f"   Date: {comment['timestamp'][:19]}")
            print()
    
    def view_post(self, post_id):
        """View full post content"""
        print(f"\n📄 Post: {post_id}")
        print("=" * 50)
        
        # Get metadata
        response = self.table.get_item(Key={'id': post_id, 'timestamp': '2025-10-09T15:14:23+00:00'})
        if 'Item' not in response:
            print("❌ Post not found")
            return
        
        metadata = response['Item']
        
        # Get full content from S3
        try:
            s3_key = metadata['s3_key']
            response = self.s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            data = json.loads(response['Body'].read())
            
            print(f"Title: {data['raw_data']['title']}")
            print(f"Author: {data['author']}")
            print(f"Score: {data['score']}")
            print(f"Created: {data['timestamp']}")
            print(f"Subreddit: {data['subreddit']}")
            print()
            print("Content:")
            print("-" * 30)
            print(data['raw_data']['body'])
            print()
            print(f"S3 Location: s3://{S3_BUCKET_NAME}/{s3_key}")
            
        except Exception as e:
            print(f"❌ Error loading post content: {e}")
    
    def search_by_author(self, author_name):
        """Search posts/comments by author"""
        print(f"\n🔍 Posts/Comments by: {author_name}")
        print("-" * 50)
        
        response = self.table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('author').eq(author_name)
        )
        
        items = response['Items']
        if not items:
            print("❌ No posts/comments found by this author")
            return
        
        for item in items:
            print(f"• {item['id']} ({item['type']}) - Score: {item.get('score', 0)} - {item['timestamp'][:19]}")
    
    def search_by_date(self, date):
        """Search posts/comments by date"""
        print(f"\n📅 Posts/Comments on: {date}")
        print("-" * 50)
        
        response = self.table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('timestamp').begins_with(date)
        )
        
        items = response['Items']
        if not items:
            print("❌ No posts/comments found on this date")
            return
        
        posts = [item for item in items if item['type'] == 'post']
        comments = [item for item in items if item['type'] == 'comment']
        
        print(f"📝 Posts: {len(posts)}")
        print(f"💬 Comments: {len(comments)}")
        
        for item in items[:10]:  # Show first 10
            print(f"• {item['id']} ({item['type']}) - {item.get('author', 'Unknown')} - Score: {item.get('score', 0)}")

def main():
    viewer = DatabaseViewer()
    
    while True:
        print("\n🔍 Database Viewer")
        print("=" * 30)
        print("1. Show summary")
        print("2. List recent posts")
        print("3. List recent comments")
        print("4. View specific post")
        print("5. Search by author")
        print("6. Search by date")
        print("0. Exit")
        
        choice = input("\nEnter your choice (0-6): ").strip()
        
        if choice == "0":
            break
        elif choice == "1":
            viewer.show_summary()
        elif choice == "2":
            limit = input("How many posts to show? (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            viewer.list_posts(limit)
        elif choice == "3":
            limit = input("How many comments to show? (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            viewer.list_comments(limit)
        elif choice == "4":
            post_id = input("Enter post ID: ").strip()
            viewer.view_post(post_id)
        elif choice == "5":
            author = input("Enter author name: ").strip()
            viewer.search_by_author(author)
        elif choice == "6":
            date = input("Enter date (YYYY-MM-DD): ").strip()
            viewer.search_by_date(date)
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
