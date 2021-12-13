import sys
import os

sys.path.append(os.path.dirname(os.path.realpath(__file__))+'/lib')

import json
import pymongo
import boto3

# import requests
USERNAME = os.environ['USERNAME']
PASSWORD = boto3.client('secretsmanager').get_secret_value(SecretId=os.environ['SECRET'])['SecretString']
CLUSTER_URL = os.environ['CLUSTER_URL']


def lambda_handler(event, context): 
    client = pymongo.MongoClient(f'mongodb://{USERNAME}:{PASSWORD}@{CLUSTER_URL}:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false')
    db = client.builder
    col = db.blogs
    data = col.find({}, {'author': 1, 'title': 1, 'timestamp': 1})
    page_count = 0
    posts = []
    response_body = []
    for i, post in enumerate(data):
        post.pop('_id')
        posts.append(post)
        if i % 50 == 0 and i != 0:
            page_count = page_count + 1
            response_body.append({
                'page': page_count,
                'posts': posts 
            })
            posts = []

    ##Close the connection
    client.close()
    
    
    return {
        "statusCode": 200,
        "body": json.dumps(response_body),
    }
