import json
import boto3
import os
import csv
import pandas as pd
from io import StringIO

from functions.extract_data import extraction
from functions.load_data import load

dynamodb = boto3.client('dynamodb')
DynamoDBTable = os.environ['PROCESS_STATUS_TABLE']

def handler(event, context):
    # Initializes Process State if it not already exists
    try:
        dynamodb.put_item(
            TableName=DynamoDBTable,
            Item={
                'Status': {'S': 'Failed'},
                'Result': {'S': 'False'},
            },
            ConditionExpression='attribute_not_exists(#Status)',
            ExpressionAttributeNames={
                "#Status": "Status"
            }
        )
    except Exception as e:
        print(str(e))

    reponse = dynamodb.get_item(
        TableName=DynamoDBTable,
        Key={
            'Status': {'S': 'Failed'}
        })

    failedProcess = reponse['Item']['Result']['S']
    print(f'Result of Previous Data Process Function: {failedProcess}')

    # If previous ETL was Processed then perform the ETL tasks
    if failedProcess == "False":
        print("New Data is being downloaded...")
        processNewData = extraction()
        if processNewData == "DONE":
            print("Running load() function...")
            load()
        else:
            return "Failed"
    # If previous ETL was not Processed then Process the old data first, and then rerun the regular ETL tasks
    else:
        print("Last Data was not consumed! Processing Old Data")
        processOldData = load()
        print("Old Data Processed...")
        if processOldData == "DONE":
            extraction()
            print("Extracting New Data...")
        else:
            return "Failed"


if __name__ == "__main__":
    handler(None, None)