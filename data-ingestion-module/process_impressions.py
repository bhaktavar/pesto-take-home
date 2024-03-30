import json
import boto3
import base64
from datetime import datetime
import re

s3 = boto3.client('s3')

def validate_and_transform(record):
    try:
        record['timestamp'] = datetime.strptime(record['timestamp'], "%Y-%m-%dT%H:%M:%SZ").isoformat()
    except ValueError:
        return False

    if not all(isinstance(record.get(field), str) and record.get(field) for field in ['userId', 'adCreativeId']):
        return False
    
    if not isinstance(record.get('website'), str) or not re.match(r'^https?://', record['website']):
        return False
    
    return True

def lambda_handler():
    valid_data = []
    invalid_data = []
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"])
        data = json.loads(payload)
        
        if validate_and_transform(data):
            valid_data.append(data)
        else:
            invalid_data.append(data)

    s3.put_object(Bucket='valid-data-bucket', Key=key, Body=json.dumps(valid_data))
    s3.put_object(Bucket='invalid-data-bucket', Key=key, Body=json.dumps(invalid_data))    

    return {
        'statusCode': 200,
        'body': json.dumps('Data processed successfully!')
    }
