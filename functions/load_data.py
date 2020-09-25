import json
import csv
import boto3
import os
import re
import pandas as pd
from io import StringIO

s3_client = boto3.client('s3')

def SNSmessage(message):
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=os.environ['SNSTOPIC_NAME'],
        Message=message,
        Subject='COVID ETL Job Status'
    )

def s3Save(bucket, key):
    oldBucket = bucket
    oldKey = key
    newBucket = bucket
    newKey = 'data/processedData.csv'

    csv_file = s3_client.get_object(Bucket=oldBucket, Key=oldKey)
    body = csv_file['Body']
    csv_body = body.read().decode('utf-8')

    updated_headers = pd.read_csv(StringIO(csv_body), names=['Date', 'Cases', 'Deaths', 'Recovered'])

    csv_buffer = StringIO()
    updated_headers.to_csv(csv_buffer, header=True, index=False)
    s3_client.put_object(Bucket = newBucket, Key = newKey, Body = csv_buffer.getvalue())
    return "DONE"

def ProcessStatus(status):
    processStatusDb = boto3.resource('dynamodb')
    processTableName = os.environ['PROCESS_STATUS_TABLE']

    table = processStatusDb.Table(processTableName)

    table.update_item(
        Key={
            "Status": "Failed"
        },
        UpdateExpression="set #result=:s",
        ExpressionAttributeValues={
            ':s': str(status)
        },
        ExpressionAttributeNames={
            "#result": "Result"
        },
        ReturnValues="UPDATED_NEW"
    )

def load():
    record_list = []

    s3 = boto3.client('s3')
    dynamodb = boto3.client('dynamodb')

    bucket = os.environ['BUCKET_NAME']
    key = 'TransformedData/TransformedData.csv'

    DynamoDBTable = os.environ['TABLE_NAME']

    print('Bucket :', bucket, ' Key :', key)

    csv_file = s3.get_object(Bucket=bucket, Key=key)

    record_list = csv_file['Body'].read().decode('utf-8').split('\n')

    csv_reader = csv.reader(record_list, delimiter=',', quotechar='"')
    validation_reader = csv.reader(record_list, delimiter=',', quotechar='"')

    linereader = csv.reader(record_list)
    lines = len(list(linereader))

    failed = False

    counter = 0

    try:
        for row in validation_reader:
            try:
                datetime = row[0]
                Cases = int(float(row[1]))
                Deaths = int(float(row[2]))
                Recovered = int(float(row[3]))

                r = re.compile('.{4}-.{2}-.{2}')
                if len(datetime) == 10 and r.match(datetime):
                    counter = counter + 1
                else:
                    message = f"Date Field failed to match the format \n{type(datetime)} {datetime}"
                    print(message)
                    SNSmessage(message)
                    failed = True
                    ProcessStatus(True)
                    break

                if Cases >= 0:
                    counter = counter + 1
                else:
                    message = f"Cases Field failed to match the format \n{type(Cases)} {Cases}"
                    print(message)
                    SNSmessage(message)
                    failed = True
                    ProcessStatus(True)
                    break

                if Deaths >= 0:
                    counter = counter + 1
                else:
                    message = f"Deaths Field failed to match the format \n{type(Deaths)} {Deaths}"
                    print(message)
                    SNSmessage(message)
                    failed = True
                    ProcessStatus(True)
                    break

                if Recovered >= 0:
                    counter = counter + 1
                else:
                    message = f"Recovered Field failed to match the format \n{type(Recovered)} {Recovered}"
                    print(message)
                    SNSmessage(message)
                    failed = True
                    ProcessStatus(True)
                    break
            except Exception as e:
                print(str(e))

        if failed == False:
            existing = 0

            for row in csv_reader:
                try:
                    datetime = row[0]
                    Cases = row[1]
                    Deaths = row[2]
                    Recovered = row[3]

                    dynamodb.put_item(
                        TableName=DynamoDBTable,
                        Item={
                            'Date': {'S': str(datetime)},
                            'Cases': {'N': str(Cases)},
                            'Deaths': {'N': str(Deaths)},
                            'Recovered': {'N': str(Recovered)},
                        },
                        ConditionExpression='attribute_not_exists(#Date)',
                        ExpressionAttributeNames={
                            "#Date": "Date"
                        }
                    )
                except Exception as e:
                    existing = existing + 1
                    print(str(e))

            updated_rows = lines - existing

            ProcessStatus(False)
            fileSaved = s3Save(bucket, key)
            if fileSaved == "DONE":
                print("File was saved...")
                s3_client.delete_object(Bucket = bucket, Key = key)
            else:
                print("File did not save!")

            if updated_rows > 0:
                message = f"Message: Number of Lines Updated: {updated_rows}"
                print(message)
                SNSmessage(message)
                return ("DONE")

            else:
                message = "Message: There weren't any rows updated"
                print(message)
                SNSmessage(message)
                return ("DONE")

    except Exception as e:
        print(str(e))
