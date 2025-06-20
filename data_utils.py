import yfinance as yf
import time
import pandas as pd
from joblib import Memory
import os
import numpy as np
class Data:

    def __init__(self , tickers_path):
        self.tickers_path = tickers_path
        self.tickers_list = []
        self.ticker_data = []
        self.memory = Memory(os.path.join('.' , 'cache'))
        self.get_ticker_data_caching = self.memory1.cache(self._get_ticker_data)
        

    def get_tickers(self) -> None:
        '''
        Sets the tickers we want to analyze. Tickers are accessed from symbols.txt. 
        '''
        with open("symbols.txt" , "r") as r:
            all_lines = r.readlines()
            for line in all_lines:
                line = line.replace(' ' ,'')
                ticks = line.split(sep=',')
                tickers_list += ticks
    

    def _get_ticker_data(self , ticker:str) -> pd.DataFrame:
        return yf.download(ticker)
    
    def get_ticker_data(self , ticker:str) -> None:
        '''
        Retrieves the DataFrame for a given ticker
        '''
        return self.get_ticker_data_caching(ticker)
    
    def set_ticker_data(self , new_data):
        pass

    def log_data(self):
        pass
        

    def clean_ticker_data(self , ticker):
        data = self.get_ticker_data(ticker)
        #Forward fill data and
        new_data = data.resample('D').ffill()#will fill in missing days 

        diff = new_data[new_data != data]
        




        








