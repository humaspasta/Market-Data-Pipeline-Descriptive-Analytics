import yfinance as yf
import time
import pandas as pd
import joblib
from joblib import Memory
import os
import numpy as np
import fastparquet
class Data:

    def __init__(self , tickers_path):
        self.tickers_path = tickers_path
        self.tickers_list = []
        self.memory = Memory(os.path.join('.' , 'cache'))
        self.get_ticker_data_caching = self.memory.cache(self._get_ticker_data)
        self.clean = self.memory.cache(self._clean_ticker_data)
        

    #method for retrieving data
    def get_tickers(self) -> None:
        '''
        Sets the tickers we want to analyze. Tickers are accessed from symbols.txt. 
        '''
        with open("symbols.txt" , "r") as r:
            all_lines = r.readlines()
            for line in all_lines:
                line = line.replace(' ' ,'')
                ticks = line.split(sep=',')
                self.tickers_list += ticks
    

    #methods for retrieving and updating ticker data
    def _get_ticker_data(self , ticker:str) -> pd.DataFrame:
        return yf.download(ticker)
    
    def get_ticker_data(self , ticker:str) -> pd.DataFrame:
        '''
        Retrieves the DataFrame for a given ticker
        '''
        return self.get_ticker_data_caching(ticker)
    
    def update_ticker_data(self , ticker):
        self.get_ticker_data_caching.clear(ticker)
        return self.get_ticker_data(ticker)


    #method for logging data
    def log_data(self):
        pass


     #methods for cleaning data and storing back in cache   
    def clean_ticker_data(self , ticker):
        return self.clean(ticker)

    def _clean_ticker_data(self , ticker):
        data = self.get_ticker_data(ticker)
        data.dropna(axis=0)
        #Forward fill data and
        new_data = data.resample('D').ffill()#will fill in missing days 
      
        self.log_data()
        return new_data

    #converting data to parquete file
    def to_parquete(self , ticker:str) -> None:
        data = self.get_ticker_data(ticker)
        fastparquet.write(os.path.join('Data' , 'Clean' , f'{ticker}' + '.parq' ) , data)

    






        








