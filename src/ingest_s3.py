import json, time, argparse
from datetime import datetime, timedelta, timezone
import praw
import boto3
from botocore.exceptions import ClientError
from config import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT,
    SUBREDDIT, BACKFILL_DAYS, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    AWS_REGION, S3_BUCKET_NAME, DYNAMODB_TABLE_NAME
)

class RedditS3Ingester:
    def __init__(self):
        """Initialize Reddit client and AWS services"""
        self.reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        
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
        
    def ensure_bucket_exists(self):
        """Ensure S3 bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            print(f"✅ S3 bucket '{S3_BUCKET_NAME}' exists")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"📦 Creating S3 bucket '{S3_BUCKET_NAME}'...")
                if AWS_REGION == 'us-east-1':
                    self.s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
                else:
                    self.s3_client.create_bucket(
                        Bucket=S3_BUCKET_NAME,
                        CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                    )
                print(f"✅ Created S3 bucket '{S3_BUCKET_NAME}'")
            else:
                raise
    
    def ensure_dynamodb_table_exists(self):
        """Ensure DynamoDB table exists"""
        try:
            self.table.load()
            print(f"✅ DynamoDB table '{DYNAMODB_TABLE_NAME}' exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(f"📦 Creating DynamoDB table '{DYNAMODB_TABLE_NAME}'...")
                table = self.dynamodb.create_table(
                    TableName=DYNAMODB_TABLE_NAME,
                    KeySchema=[
                        {'AttributeName': 'id', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'id', 'AttributeType': 'S'},
                        {'AttributeName': 'timestamp', 'AttributeType': 'S'}
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                table.wait_until_exists()
                print(f"✅ Created DynamoDB table '{DYNAMODB_TABLE_NAME}'")
            else:
                raise
    
    def get_s3_key(self, item_id, item_type, timestamp):
        """Generate S3 key for storing data"""
        date_str = timestamp.strftime("%Y/%m/%d")
        return f"raw-data/{date_str}/{item_type}/{item_id}.json"
    
    def store_item_in_s3(self, item_data, item_id, item_type, timestamp):
        """Store individual item as JSON in S3"""
        try:
            s3_key = self.get_s3_key(item_id, item_type, timestamp)
            
            # Convert to JSON
            json_data = json.dumps(item_data, default=str, indent=2)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            return s3_key
        except Exception as e:
            print(f"❌ Failed to store {item_id} in S3: {e}")
            return None
    
    def store_metadata_in_dynamodb(self, item_id, item_type, timestamp, s3_key, reddit_data):
        """Store metadata in DynamoDB"""
        try:
            metadata = {
                'id': item_id,
                'timestamp': timestamp.isoformat(),
                'type': item_type,
                's3_key': s3_key,
                'subreddit': SUBREDDIT,
                'author': str(reddit_data.author) if reddit_data.author else None,
                'score': reddit_data.score,
                'created_utc': int(reddit_data.created_utc),
                'num_comments': getattr(reddit_data, 'num_comments', None),
                'title': getattr(reddit_data, 'title', None),
                'body_length': len(getattr(reddit_data, 'body', '') or getattr(reddit_data, 'selftext', '') or ''),
                'processed_at': datetime.now(timezone.utc).isoformat()
            }
            
            self.table.put_item(Item=metadata)
            return True
        except Exception as e:
            print(f"❌ Failed to store metadata for {item_id}: {e}")
            return False
    
    def process_reddit_item(self, item, item_type):
        """Process a single Reddit item (post or comment)"""
        try:
            item_id = item.id
            timestamp = datetime.fromtimestamp(item.created_utc, tz=timezone.utc)
            
            # Prepare data for storage
            item_data = {
                'id': item_id,
                'type': item_type,
                'subreddit': SUBREDDIT,
                'author': str(item.author) if item.author else None,
                'created_utc': item.created_utc,
                'timestamp': timestamp.isoformat(),
                'score': item.score,
                'raw_data': {
                    'title': getattr(item, 'title', None),
                    'body': getattr(item, 'body', None) or getattr(item, 'selftext', None),
                    'url': getattr(item, 'url', None),
                    'permalink': getattr(item, 'permalink', None),
                    'num_comments': getattr(item, 'num_comments', None),
                    'upvote_ratio': getattr(item, 'upvote_ratio', None),
                    'is_self': getattr(item, 'is_self', None),
                    'over_18': getattr(item, 'over_18', None),
                    'spoiler': getattr(item, 'spoiler', None),
                    'locked': getattr(item, 'locked', None),
                    'stickied': getattr(item, 'stickied', None),
                    'parent_id': getattr(item, 'parent_id', None),
                    'link_id': getattr(item, 'link_id', None),
                }
            }
            
            # Store in S3
            s3_key = self.store_item_in_s3(item_data, item_id, item_type, timestamp)
            if not s3_key:
                return False
            
            # Store metadata in DynamoDB
            success = self.store_metadata_in_dynamodb(item_id, item_type, timestamp, s3_key, item)
            
            if success:
                print(f"✅ Stored {item_type}: {item_id}")
                return True
            else:
                return False
                
        except Exception as e:
            print(f"❌ Error processing {item_type} {item.id}: {e}")
            return False
    
    
    def get_existing_ids(self):
        """Get all existing item IDs from DynamoDB to avoid duplicates"""
        print("🔍 Checking for existing items in DynamoDB...")
        existing_ids = set()
        try:
            # Scan table to get all IDs (may be slow for large tables, but ensures no duplicates)
            response = self.table.scan(
                ProjectionExpression='id',
                Select='SPECIFIC_ATTRIBUTES'
            )
            for item in response.get('Items', []):
                existing_ids.add(item['id'])
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    ProjectionExpression='id',
                    Select='SPECIFIC_ATTRIBUTES',
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response.get('Items', []):
                    existing_ids.add(item['id'])
            
            print(f"📊 Found {len(existing_ids)} existing items in DynamoDB")
            return existing_ids
        except Exception as e:
            print(f"⚠️  Error scanning DynamoDB: {e}")
            print("   Continuing without duplicate check...")
            return set()
    
    def ingest_posts_and_comments(self, days=None, max_posts=None):
        """Main ingestion function - pulls all available recent posts"""
        print(f"🚀 Starting Reddit ingestion for r/{SUBREDDIT}")
        
        # Ensure AWS resources exist
        self.ensure_bucket_exists()
        self.ensure_dynamodb_table_exists()
        
        # Get existing IDs to avoid duplicates
        existing_ids = self.get_existing_ids()
        
        # Use max limit if not specified (Reddit API max is 1000)
        max_posts = max_posts if max_posts else 1000
        print(f"📊 Using maximum Reddit API limit: {max_posts} posts")
        
        # Calculate time range if days specified
        since_epoch = None
        if days:
            since = datetime.now(timezone.utc) - timedelta(days=days)
            since_epoch = since.timestamp()
            print(f"📅 Collecting data since: {since.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        else:
            print("📅 Collecting all available recent posts (max limit)")
        
        # Get subreddit
        sr = self.reddit.subreddit(SUBREDDIT)
        
        posts_processed = 0
        posts_skipped = 0
        comments_processed = 0
        comments_skipped = 0
        
        # Process posts
        print("\n📝 Processing posts...")
        for post in sr.new(limit=max_posts):
            # Skip if older than specified days
            if since_epoch and post.created_utc < since_epoch:
                break  # Posts are sorted by new, so we can stop here
            
            # Skip if already exists
            if post.id in existing_ids:
                posts_skipped += 1
                continue
                
            if self.process_reddit_item(post, 'post'):
                posts_processed += 1
                existing_ids.add(post.id)  # Track to avoid re-processing
                if posts_processed % 50 == 0:
                    print(f"   Processed {posts_processed} new posts...")
            
            # Process comments for this post
            try:
                post.comments.replace_more(limit=0)
                for comment in post.comments.list():
                    # Skip if older than specified days
                    if since_epoch and comment.created_utc < since_epoch:
                        continue
                    
                    # Skip if already exists
                    if comment.id in existing_ids:
                        comments_skipped += 1
                        continue
                    
                    if self.process_reddit_item(comment, 'comment'):
                        comments_processed += 1
                        existing_ids.add(comment.id)  # Track to avoid re-processing
            except Exception as e:
                print(f"⚠️ Error processing comments for post {post.id}: {e}")
        
        print(f"\n📊 Ingestion Complete!")
        print(f"   ✅ New posts added: {posts_processed}")
        print(f"   ⏭️  Posts skipped (duplicates): {posts_skipped}")
        print(f"   ✅ New comments added: {comments_processed}")
        print(f"   ⏭️  Comments skipped (duplicates): {comments_skipped}")
        print(f"   📦 Total new items: {posts_processed + comments_processed}")
        print(f"   📁 Data stored in: s3://{S3_BUCKET_NAME}/raw-data/")
        print(f"   📊 Metadata in: DynamoDB table '{DYNAMODB_TABLE_NAME}'")

def main():
    parser = argparse.ArgumentParser(description='Ingest Reddit data to S3 and DynamoDB')
    parser.add_argument('--days', type=int, default=None, help='Number of days to backfill (default: all available, max 1000 posts)')
    parser.add_argument('--max-posts', type=int, default=1000, help='Maximum number of posts to retrieve (default: 1000, Reddit API max)')
    args = parser.parse_args()
    
    ingester = RedditS3Ingester()
    ingester.ingest_posts_and_comments(args.days, args.max_posts)

if __name__ == "__main__":
    main()
