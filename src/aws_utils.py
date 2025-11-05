import boto3
import json
import os
from datetime import datetime
from typing import Dict, Any, List
from config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    S3_BUCKET_NAME, DYNAMODB_TABLE_NAME
)

class AWSManager:
    def __init__(self):
        """Initialize AWS clients"""
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
    
    def create_s3_bucket(self) -> bool:
        """Create S3 bucket if it doesn't exist"""
        try:
            self.s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            print(f"✅ S3 bucket '{S3_BUCKET_NAME}' already exists")
            return True
        except self.s3_client.exceptions.NoSuchBucket:
            try:
                self.s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
                print(f"✅ Created S3 bucket '{S3_BUCKET_NAME}'")
                return True
            except Exception as e:
                print(f"❌ Failed to create S3 bucket: {e}")
                return False
        except Exception as e:
            print(f"❌ Error checking S3 bucket: {e}")
            return False
    
    def create_dynamodb_table(self) -> bool:
        """Create DynamoDB table if it doesn't exist"""
        try:
            self.table.load()
            print(f"✅ DynamoDB table '{DYNAMODB_TABLE_NAME}' already exists")
            return True
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
            try:
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
                return True
            except Exception as e:
                print(f"❌ Failed to create DynamoDB table: {e}")
                return False
        except Exception as e:
            print(f"❌ Error checking DynamoDB table: {e}")
            return False
    
    def upload_raw_data(self, data: Dict[str, Any], s3_key: str) -> bool:
        """Upload raw Reddit data to S3"""
        try:
            json_data = json.dumps(data, default=str)
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json'
            )
            return True
        except Exception as e:
            print(f"❌ Failed to upload to S3: {e}")
            return False
    
    def store_metadata(self, metadata: Dict[str, Any]) -> bool:
        """Store metadata in DynamoDB"""
        try:
            self.table.put_item(Item=metadata)
            return True
        except Exception as e:
            print(f"❌ Failed to store metadata in DynamoDB: {e}")
            return False
    
    def get_s3_key(self, item_id: str, item_type: str, timestamp: datetime) -> str:
        """Generate S3 key for storing data"""
        date_str = timestamp.strftime("%Y/%m/%d")
        return f"raw-data/{date_str}/{item_type}/{item_id}.json"
    
    def test_connection(self) -> bool:
        """Test AWS connection"""
        try:
            # Test S3
            self.s3_client.list_buckets()
            print("✅ S3 connection successful")
            
            # Test DynamoDB
            self.dynamodb.meta.client.list_tables()
            print("✅ DynamoDB connection successful")
            
            return True
        except Exception as e:
            print(f"❌ AWS connection failed: {e}")
            return False

def upload_sqlite_to_s3(db_path: str) -> bool:
    """Upload current SQLite database to S3 for testing"""
    try:
        aws_manager = AWSManager()
        
        # Test connection first
        if not aws_manager.test_connection():
            return False
        
        # Create bucket if needed
        if not aws_manager.create_s3_bucket():
            return False
        
        # Upload SQLite file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"backup/sqlite_backup_{timestamp}.db"
        
        aws_manager.s3_client.upload_file(db_path, S3_BUCKET_NAME, s3_key)
        print(f"✅ Uploaded SQLite database to s3://{S3_BUCKET_NAME}/{s3_key}")
        
        return True
    except Exception as e:
        print(f"❌ Failed to upload SQLite to S3: {e}")
        return False
