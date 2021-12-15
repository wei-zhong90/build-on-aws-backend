import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__))+'/lib')

import json
import pymongo
import boto3
from bson import json_util
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# import requests
USERNAME = os.environ['USERNAME']
PASSWORD = boto3.client('secretsmanager').get_secret_value(SecretId=os.environ['SECRET'])['SecretString']
CLUSTER_URL = os.environ['CLUSTER_URL']

host = 'vpc-builder-creed-mzkhhddt7zqw3sni7g3e3qfuvq.ap-northeast-2.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
region = 'ap-northeast-2' # e.g. us-west-1

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

search = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

def lambda_handler(event, context): 
    client = pymongo.MongoClient(f'mongodb://{USERNAME}:{PASSWORD}@{CLUSTER_URL}:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
    db = client.builder
    col = db.blogs
    data = col.find({})
    posts = []
    response_body = []
    for i, post in enumerate(data, start=1):
        post = json.loads(json_util.dumps(post))
        posts.append(post)
        if i % 50 == 0:
            response_body.append(posts)
            posts = []

    if posts:
        response_body.append(posts)
    ##Close the connection
    client.close()
    
    
    return {
        "statusCode": 200,
        "body": json.dumps(response_body),
    }

def search_handler(event, context):
    print(event)
    query = {
        "from": 0,
        'size': 50,
        'query': {
            "query_string": {
                "query": event['queryStringParameters']['search_string'],
            }
        },
        "stored_fields": []
    }

    response = search.search(
        body = query,
        index = "blogs"
    )
    
    print(response)
    
    return {
        "statusCode": 200,
        "body": json.dumps(response),
    }