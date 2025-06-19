import yfinance as yf
import time
import pandas as pd
from joblib import Memory
import os

class Data:

    def __init__(self , tickers_path):
        self.tickers_path = tickers_path
        self.tickers_list = []
        self.ticker_data = []
        self.memory = Memory(os.path.join('.' , 'cache'))
        self.get_ticker_data = self.memory.cache(self._get_ticker_data)
        

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
    
    def get_ticker(self , ticker) -> None:
        '''
        Appends all the ticker dataframe references to an array
        '''
        return self.get_ticker_data(ticker)







