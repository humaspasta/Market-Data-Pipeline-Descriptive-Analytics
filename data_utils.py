import yfinance as yf
import time
import pandas as pd
import joblib
from joblib import Memory
import os
import numpy as np
import fastparquet
class Data:

    def __init__(self , tickers_path:str , date:tuple, log_path:str):
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
        return yf.download(ticker , start=self.date[0] , end=self.date[1])
    
    def get_ticker_data(self , ticker:str) -> pd.DataFrame:
        '''
        Retrieves the DataFrame for a given ticker
        '''
        return self.get_ticker_data_caching(ticker)
    
    def update_ticker_data(self , ticker):
        self.get_ticker_data_caching.clear(ticker)
        return self.get_ticker_data(ticker)


    #method for logging data
    def log_data(self, information:pd.DataFrame) -> None:
        '''
        Writes input dataframe to a txt file that stores the logs.
        '''
        with open(self.log_path , 'w') as tx:
            tx.write('Logs')
        information.to_csv(self.log_path, sep='\t', index=False)
                
    
    def clear_cache(self):
        for ticker in self.tickers_list:
            self.get_ticker_data_caching.clear(ticker)
            self.clean.clear(ticker)


    

     #methods for cleaning data and storing back in cache   
    def clean_ticker_data(self , ticker):
        return self.clean(ticker)

    def _clean_ticker_data(self , ticker):
        data = self.get_ticker_data(ticker)
        data.dropna(axis=0)
        #Forward fill data and
        new_data = data.resample('D').ffill()#will fill in missing days 

        diff_data = data[data[not data.isin(new_data)]] #finding data that was logged in the new_data

        self.log_data(diff_data) #logging changed data in new_data
        return new_data

    #converting data to parquete file
    def to_parquete(self , ticker:str) -> None:
        data = self.get_ticker_data(ticker)
        fastparquet.write(os.path.join('Data' , 'Clean' , f'{ticker}' + '.parq' ) , data)

    






        








