import yfinance as yf
import time
import pandas as pd
import joblib
from joblib import Memory
import os
import numpy as np
import fastparquet
import datetime
class Data:

    def __init__(self , tickers_path:str , date:tuple, log_path:str):
        '''
        tickers_path: The path to the tickers
        date: a tuple that should be strictly of length 2 and contains values for the start and end date like so (start date , end date). 
        Takes strings in %Y-%M-%D format. 
        log_path: The path where logs should be noted

        '''
        if date == None:
            raise ValueError('tuple is None')
        elif len(date) > 2:
            raise ValueError('Unexpected amount of dates')
        
        self.tickers_path = tickers_path
        self.tickers_list = []
        self.memory = Memory(os.path.join('.' , 'Cache'))
        self.get_ticker_data_caching = self.memory.cache(self._get_ticker_data)
        self.clean = self.memory.cache(self._clean_ticker_data)
        self.date = date
        self.log_path = log_path
        
    #method for retrieving data
    def get_tickers(self) -> None:
        '''
        Sets the tickers we want to analyze. Tickers are accessed from symbols.txt. 
        '''
        with open(self.tickers_path, "r") as r:
            all_lines = r.readlines()
            for line in all_lines:
                line = line.replace(' ' ,'')
                ticks = line.split(sep=',')
                self.tickers_list += ticks
        
        r.close()
    
    #methods for retrieving and updating ticker data
    def _get_ticker_data(self , ticker:str) -> pd.DataFrame:
        '''
        inner function for retrieving ticker data
        '''
        return yf.download(ticker , start=self.date[0] , end=self.date[1])
    
    def get_ticker_data(self , ticker:str) -> pd.DataFrame:
        '''
        Retrieves the DataFrame for a given ticker
        '''
        return self.get_ticker_data_caching(ticker)
    
    def update_ticker_data(self , ticker):
        '''
        updating ticker data
        '''
        self.get_ticker_data_caching.clear(ticker)
        return self.get_ticker_data(ticker)


    #method for logging data
    def log_data(self, ticker:str , information:list) -> None:
        '''
        Writes added days to the log.txt file
        '''
        
        with open(self.log_path , 'w') as tx:
            tx.write('Logs for ' + ticker + '\n')
            for date in information:
                tx.write(str(date) + "\n")
            tx.close()
                

    def clear_cache(self):
        '''
        clears the cache
        '''
        for ticker in self.tickers_list:
            self.get_ticker_data_caching.clear(ticker)
            self.clean.clear(ticker)


    

     #methods for cleaning data and storing back in cache   
    def clean_ticker_data(self , ticker):
        '''
        cleans the data
        '''
        data = self.clean(ticker)
        data.reset_index(inplace=True)
        return data

    def _clean_ticker_data(self , ticker:str):
        '''
        Inner function that manages the data cleaning
        '''
        data = self.get_ticker_data(ticker)
        data.dropna(axis=0)
        #Forward fill data and
        new_data = data.resample('D').ffill()#will fill in missing days using fast forward fill
        
        new_dates = set(new_data.index.difference(data.index)) #finding data that was logged in the new_data
        old_dates = set(data.index)

        date_diff = new_dates - old_dates #getting dates that are not common between the two
        self.log_data(ticker , date_diff) # logging the dates that were added
        
        return new_data
        

    #converting data to parquete file
    def to_parquete(self , ticker:str) -> None:
        '''
        converts the data for a given ticker to a parquet file
        '''
        data = self.clean_ticker_data(ticker)
        data_path = os.path.join('Data' , 'Clean' , f'{ticker}' + '.parq' )
        if os.path.exists(data_path):
            os.remove(data_path)
        
        data.to_parquet(data_path, engine='fastparquet')


    def get_parquet(self , ticker):
        '''
        Retrieves the data of a given ticker from the respective parque file
        '''
        path = os.path.join('Data' , 'Clean' , f'{ticker}' + '.parq' )
        return pd.read_parquet(path , engine='fastparquet')
    

    #calculations
    def calculate_dividend_error(self , ticker , date:str):
        '''
        Calculates the dividend error with respect to the average of the period analyzed.
        '''
        date_format = '%y-%m-%d'
        
        dt_object = datetime.strptime(date , date_format)

        data = self.get_parquet(ticker)

        close_values = data[('Close' , ticker)]
        close_avg = close_values.sum()
        

        close_on_day = data[data[('Date' , '') == dt_object]][('Close' , ticker)]

        error = abs(close_on_day - close_avg) / close_avg * 100

    
    




        








