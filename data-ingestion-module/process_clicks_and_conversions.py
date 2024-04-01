import csv
import boto3
from datetime import datetime

def validate_row(row):
    try:
        datetime.strptime(row['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        if not row['user_id'] or not row['campaign_id']:
            return False
        if row['action_type'] not in ['click', 'conversion']:
            return False
    except Exception:
        return False
    return True

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    
    valid_data = []
    invalid_data = []
    
    obj = s3.get_object(Bucket=source_bucket, Key=source_key)
    lines = obj['Body'].read().decode('utf-8').splitlines()
    for row in csv.DictReader(lines):
        if validate_row(row):
            valid_data.append(row)
        else:
            invalid_data.append(row)

    key = "clicks_and_conversions" + datetime.today().strftime('%Y-%m-%d')

    s3.put_object(Bucket='valid-data-bucket', Key=key, Body=json.dumps(valid_data))
    s3.put_object(Bucket='invalid-data-bucket', Key=key, Body=json.dumps(invalid_data))    

    return {
        'statusCode': 200,
        'body': 'Process completed.'
    }
