import unittest
import pandas as pd
import os.path


from functions.extract_data import getData
from functions.transform_data import transformNewYorkTimesData
from functions.transform_data import transformJohnHopkinsData
from functions.transform_data import transformJoinData


my_path = os.path.abspath(os.path.dirname(__file__))
nytTestDataPath = os.path.join(my_path, "test_files/NYT-test-data.csv")
jhpTestDataPath = os.path.join(my_path, "test_files/JPH-test-data.csv")

class TestCalc(unittest.TestCase):
    
    def setUp(self):
        self.nytdata = nytTestDataPath
        self.jphdata = jhpTestDataPath        
    
    def tearDown(self):
        pass
    
    ################## TEST GETDATA ##################  
    
    ## New York Times Data ##
        
    # NYT Data Test: Check if headers match
    def test_csv_read_data_headers(self):        
        result = getData(self.nytdata)
        result_headers = list(result.columns)
        result_df = ['date', 'cases', 'deaths']
        self.assertEqual(result_headers, result_df)
    
    # NYT Data Test: Check if Date can be read properly    
    def test_csv_nyt_date_data(self):
        result = getData(self.nytdata)
        result_date = result.date.iloc[0]
        self.assertEqual(result_date, '2020-12-11')
        
    ## John Hopkins Data ##
        
    # JHP Data Test: Check if headers match
    def test_jhp_csv_read_data_headers(self):        
        result = getData(self.jphdata)
        result_headers = list(result.columns)
        result_df = ['Date', 'Country/Region', 'Province/State', 'Lat', 'Long', 'Confirmed', 'Recovered', 'Deaths']
        self.assertEqual(result_headers, result_df)
    
    # JHP Data Test: Check if Date can be read properly    
    def test_csv_jph_date_data(self):
        result = getData(self.jphdata)
        result_date = result.Date.iloc[0]
        self.assertEqual(result_date, '2020-07-10')
    
    ################## TEST TRANSFORM MODULE DATA ##################  
    
    ## New York Times Data ##
      
    # TRANSFORM NYT DATA TEST: check if headers match
    def test_nytdata_headers(self):
        result = transformNewYorkTimesData(getData(self.nytdata))
        result_headers = list(result.columns)
        result_df = ['datetime', 'cases', 'deaths']
        self.assertEqual(result_headers, result_df)
    
    # TRANSFORM NYT DATA TEST: Check if date has been converted to datetime   
    def test_nytdata(self):
        result = transformNewYorkTimesData(getData(self.nytdata))
        result_date = str(result.datetime.iloc[1])
        self.assertEqual(result_date, '2020-12-12 00:00:00')
    
    # TRANSFORM NYT DATA TEST: Check if Cases and Deaths Field are Int and greater or equal to 0       
    def test_nytdata_int(self):
        result = transformNewYorkTimesData(getData(self.nytdata))
        result_cases_int = result.cases.iloc[0]
        result_deaths_int = result.deaths.iloc[1]
        self.assertGreaterEqual(result_cases_int, 0)
        self.assertGreaterEqual(result_deaths_int, 0)
        
    ## John Hopkins Data ##
    
    # TRANSFORM JPH DATA TEST: check if headers match
    def test_jphdata_headers(self):
        result = transformJohnHopkinsData(getData(self.jphdata))
        result_headers = list(result.columns)
        result_df = ['datetime', 'Recovered']
        self.assertEqual(result_headers, result_df)
    
    # TRANSFORM NYT DATA TEST: Check if date has been converted to datetime   
    def test_jphdata(self):
        result = transformJohnHopkinsData(getData(self.jphdata))
        result_date = str(result.datetime.iloc[1])
        self.assertEqual(result_date, '2020-07-10 00:00:00')
    
    # TRANSFORM NYT DATA TEST: Check if Cases and Deaths Field are Int and greater or equal to 0       
    def test_jphdata_int(self):
        result = transformJohnHopkinsData(getData(self.jphdata))
        result_recovered_int = result.Recovered.iloc[0]
        self.assertGreaterEqual(result_recovered_int, 0)
        
    ## JOINED DATA OF BOTH NEW YORK TIME AND JOHN HOPKINS ##
    
    # JOINED DATA TEST: check if headers match    
    def test_joindataset_headers(self):
        newYorkDataExtraction = getData(self.nytdata)
        johnHopkinsDataExtraction = getData(self.jphdata)
        NYTData = transformNewYorkTimesData(newYorkDataExtraction) 
        JPHData = transformJohnHopkinsData(johnHopkinsDataExtraction) 
        
        result = transformJoinData(NYTData, JPHData)
        
        result_headers = list(result.columns)
        expected_headers = ['datetime', 'cases', 'deaths', 'Recovered']
        
        self.assertEqual(result_headers, expected_headers) 

    # JOINED DATA TEST: Check if date has been converted to datetime         
    def test_joindataset_date(self):
        newYorkDataExtraction = getData(self.nytdata)
        johnHopkinsDataExtraction = getData(self.jphdata)
        NYTData = transformNewYorkTimesData(newYorkDataExtraction) 
        JPHData = transformJohnHopkinsData(johnHopkinsDataExtraction) 
        
        result = transformJoinData(NYTData, JPHData)  
        result_date = str(result.datetime.iloc[0])
        
        self.assertEqual(result_date, '2020-08-12 00:00:00')      
        
    # JOINED DATA TEST: Check if Cases and Deaths Field are Int and greater or equal to 0       
    def test_joindataset_int(self):
        newYorkDataExtraction = getData(self.nytdata)
        johnHopkinsDataExtraction = getData(self.jphdata)
        NYTData = transformNewYorkTimesData(newYorkDataExtraction) 
        JPHData = transformJohnHopkinsData(johnHopkinsDataExtraction) 
        
        result = transformJoinData(NYTData, JPHData)  
        
        result_cases_int = result.cases.iloc[0]
        result_deaths_int = result.deaths.iloc[0]
        result_Recocered_int = result.Recovered.iloc[0]
        
        self.assertGreaterEqual(result_cases_int, 0)
        self.assertGreaterEqual(result_deaths_int, 0)    
        self.assertGreaterEqual(result_Recocered_int, 0)   
        

if __name__ == '__main__':
    unittest.main()