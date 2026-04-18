import json
import boto3
import urllib.parse
import requests
from requests_aws4auth import AWS4Auth

rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')

region = 'us-east-1'
service = 'es'

# open searchg
host = 'https://search-photos-4nxovnoosm2qtihvnjxnax7b4i.us-east-1.es.amazonaws.com'

def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    # Get bucket + object key from S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key']
    )

    # Detect labels using Rekognition
    response = rekognition.detect_labels(
        Image={'S3Object': {'Bucket': bucket, 'Name': key}},
        MaxLabels=10
    )

    labels = [label['Name'].lower() for label in response['Labels']]

    # Get custom metadata (if any)
    head = s3.head_object(Bucket=bucket, Key=key)

    metadata = head.get('Metadata', {})
    custom_labels = metadata.get('customlabels')

    if custom_labels:
        labels.extend(custom_labels.split(','))

    # Create document
    document = {
        "objectKey": key,
        "bucket": bucket,
        "createdTimestamp": head['LastModified'].isoformat(),
        "labels": labels
    }

    # Index into OpenSearch
    url = f"{host}/photos/_doc/{key}"

    headers = {"Content-Type": "application/json"}

    session = boto3.Session()
    credentials = session.get_credentials()

    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token
    )

    response = requests.put(
        url,
        auth=awsauth,
        json=document,
        headers={"Content-Type": "application/json"}
    )

    print("OpenSearch response:", response.text)

    return {
        'statusCode': 200,
        'body': json.dumps('Indexed successfully')
    }
