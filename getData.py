import json
import boto3
import os
import csv
import pandas as pd
from io import StringIO

from functions.transform_data import transformNewYorkTimesData
from functions.transform_data import transformJohnHopkinsData
from functions.transform_data import transformJoinData
from functions.load_data import load

s3 = boto3.resource('s3')

nytDataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
jhpDataURL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1600111930493r0.051025759046968044'

def getData(URL):
    df = pd.read_csv(URL)
    return df

def handler(event, context):
    def extraction():
        # Download File and Sort Data
        newYorkDataExtraction = getData(nytDataURL)
        johnHopkinsDataExtraction = getData(jhpDataURL)
            
        NYTData = transformNewYorkTimesData(newYorkDataExtraction) 
        JHPData = transformJohnHopkinsData(johnHopkinsDataExtraction) 
        
        transformeData = transformJoinData(NYTData, JHPData)
        
        bucket = os.environ['BUCKET_NAME']
        csv_buffer = StringIO()
        transformeData.to_csv(csv_buffer, header=False, index=False)
        s3.Object(bucket, 'TransformedData/TransformedData.csv').put(Body=csv_buffer.getvalue())

        return "DONE"
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['PROCESS_STATUS_TABLE'])
    reponse = table.get_item(
        Key={
            'Status': 'Failed'
        })
    failedProcess = reponse['Item']['Result']
    
    print(failedProcess)
    
    if failedProcess == "False" or failedProcess == None:
        processNewData = extraction()
        if processNewData == "DONE":
            load()
        else:
            return "Failed"
    else:
        processOldData = load()
        
        if processOldData == "DONE":
            extraction()
        else:
            return "Failed"