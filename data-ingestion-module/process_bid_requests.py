import json
import boto3
import base64
from datetime import datetime
from fastavro import reader, json_writer

s3 = boto3.client('s3')
MAX_BID_PRICE = 100

def validate_and_transform(record):
    try:
        if not (0 < float(record.get('bidPrice', 0)) < MAX_BID_PRICE):
            return False
        if not len(record.get('userId', '')) > 0:
            return False
        if not len(record.get('auctionId', '')) > 0:
            return False
        datetime.strptime(record['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True

def lambda_handler(event, context):
    valid_data = []
    invalid_data = []
    
    for kinesis_record in event['Records']:
        payload = base64.b64decode(kinesis_record['kinesis']['data']).decode('utf-8')
        avro_reader = reader(payload)
        record = json.loads(avro_reader)
        
        if validate_and_transform(record):
            valid_data.append(record)
        else:
            invalid_data.append(record)
    
    s3.put_object(Bucket='valid-data-bucket', Key=key, Body=json.dumps(valid_data))
    s3.put_object(Bucket='invalid-data-bucket', Key=key, Body=json.dumps(invalid_data))    

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed records.')
    }
