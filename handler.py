import json
import boto3
import os
import uuid
import requests


s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
textract = boto3.client('textract')

FILES_TABLE = os.environ['FILES_TABLE']


def create_file(event, context):
    body = json.loads(event['body'])
    callback_url = body.get('callback_url')
    file_id = str(uuid.uuid4())

    upload_url = s3_client.generate_presigned_url(
        'put_object',
        Params={'Bucket': 'file-bucket', 'Key': file_id},
        ExpiresIn=3600
    )

    table = dynamodb.Table(FILES_TABLE)
    table.put_item(
        Item={
            'file_id': file_id,
            'callback_url': callback_url,
            'status': 'PENDING'
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps({'upload_url': upload_url})
    }


def process_file(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        response = textract.detect_document_text(
            Document={'S3Object': {'Bucket': bucket, 'Name': key}}
        )

        text = ''
        for item in response['Blocks']:
            if item['BlockType'] == 'LINE':
                text += item['Text'] + '\n'

        table = dynamodb.Table(FILES_TABLE)
        table.update_item(
            Key={'file_id': key},
            UpdateExpression='SET #status = :status, text = :text',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': 'COMPLETED', ':text': text}
        )


def make_callback(event, context):
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            file_id = record['dynamodb']['NewImage']['file_id']['S']
            callback_url = record['dynamodb']['NewImage']['callback_url']['S']
            text = record['dynamodb']['NewImage']['text']['S']

            response = requests.post(callback_url, json={'file_id': file_id, 'text': text})
            print(f'Callback response: {response.status_code}')


def get_file(event, context):
    file_id = event['pathParameters']['file_id']

    table = dynamodb.Table(FILES_TABLE)
    response = table.get_item(Key={'file_id': file_id})

    if 'Item' in response:
        return {
            'statusCode': 200,
            'body': json.dumps(response['Item'])
        }
    else:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'File not found'})
        }
