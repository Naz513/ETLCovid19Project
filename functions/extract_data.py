import pandas as pd
import csv
from io import StringIO
import boto3
import os

from .transform_data import transformJohnHopkinsData
from .transform_data import transformNewYorkTimesData
from .transform_data import transformJoinData

s3 = boto3.resource('s3')

nytDataURL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
jhpDataURL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv?opt_id=oeu1600111930493r0.051025759046968044'

# Downloads Data and Stores it in memory
def getData(URL):
    df = pd.read_csv(URL)
    return df

# Transform data and combines them together to form one dataset and saves it to S3 Bucket
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
