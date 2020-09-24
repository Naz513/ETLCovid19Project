import json
import boto3
import os
import csv
import pandas as pd
from io import StringIO

s3 = boto3.resource('s3')

def transformNewYorkTimesData(data):
    # Download File and Sort Data
    df = data
    df = df.fillna(0)
    sortedData = df.sort_index(ascending=False)
    sortedData['datetime'] = pd.to_datetime(sortedData['date'], format='%Y-%m-%d')
    sortedData['cases'] = pd.to_numeric(sortedData['cases'], downcast='integer')
    sortedData['deaths'] = pd.to_numeric(sortedData['deaths'], downcast='integer')
    nyt_data = sortedData[['datetime', 'cases', 'deaths']]
    return nyt_data

def transformJohnHopkinsData(data):
    # Download File and Sort Data
    df = data
    df = df.fillna(0)
    us_data = df.loc[df['Country/Region'] == 'US']
    new_data = us_data[['Date', 'Country/Region', 'Recovered']]
    sortedData = new_data.sort_index(ascending=False)
    sortedData['datetime'] = pd.to_datetime(sortedData['Date'], format='%Y-%m-%d')
    sortedData['Recovered'] = pd.to_numeric(sortedData['Recovered'], downcast='integer')
    new_final_data = sortedData[['datetime', 'Recovered']]
    return new_final_data

def transformJoinData(NYTData, JHPData):
    nyt_data = NYTData
    jhp_data = JHPData
    frames = [nyt_data, jhp_data]
    result = pd.merge(nyt_data, jhp_data, on='datetime', how='inner')
    return result