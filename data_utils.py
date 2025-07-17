import shutil
import yfinance as yf
import time
import pandas as pd
import joblib
from joblib import Memory
import os
import numpy as np
import fastparquet
from datetime import datetime
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

        return self.tickers_list
    

    def create_all_cleaned_parqs(self):
        '''
        Creates all clean parquet files
        '''
        for ticker in self.tickers_list:
            data = self.clean_ticker_data(ticker)
            self.to_parquete(ticker , data)
            

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
        
        with open(self.log_path , 'a') as tx:
            tx.write('\nLogs for ' + ticker + '\n')
            for date in information:
                tx.write(str(date) + "\n")
        
            if self.calculate_dividend_error(ticker , self.date[1]) >= 0.2:
                tx.write("\nSplit dividend anomaly")
            tx.write('-' * 10)
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
        
        return data

    def _clean_ticker_data(self , ticker:str):
        '''
        Inner function that manages the data cleaning
        '''
        data = self.get_ticker_data(ticker)
        data.dropna(axis=0)
        #Forward fill data and
        new_data = data.resample('B').ffill(limit=2)#will fill in missing days using fast forward fill

         #converting new data to a parquet
        new_data[('Cummalitive Max' , ticker)] = new_data[('Close' , ticker)].cummax()
        new_data[('Drawdown' , ticker)] = new_data[('Close' , ticker)] - new_data[('Cummalitive Max' , ticker)]

        new_dates = set(new_data.index.difference(data.index)) #finding data that was logged in the new_data
        old_dates = set(data.index)

        date_diff = new_dates - old_dates #getting dates that are not common between the two
         # logging the dates that were added

        new_data = new_data.reset_index()

        new_data.index = range(len(new_data))
        self.to_parquete(ticker , new_data)
        

        self.log_data(ticker , date_diff)
        return new_data
        

    #converting data to parquete file
    def to_parquete(self , ticker:str , data:pd.DataFrame) -> None:
        '''
        converts the data for a given ticker to a parquet file
        '''
        data_path = os.path.join('Data' , 'Clean' , f'{ticker}.parquet' )
        if os.path.exists(data_path):
            os.remove(data_path)
        
        data.to_parquet(data_path, engine='fastparquet')


    def get_parquet(self , file_name):
        '''
        Retrieves the data of a given ticker from the respective parque file. If the parquet file doesn't exist. It will 
        create the file.
        file_name should only contain the name of the file without the .parquet at the end. 
        '''
        path = os.path.join('Data' , 'Clean' , f'{file_name}' + '.parquet' )
        return pd.read_parquet(path , engine='fastparquet')
    

    #calculations
    def calculate_dividend_error(self , ticker , date:str):
        '''
        Calculates the dividend error with respect to the average of the period analyzed.
        '''
        date_format = '%Y-%m-%d'
        
        dt_object = datetime.strptime(date , date_format)
        if(not os.path.exists(os.path.join('Data' , 'Clean' , f'{ticker}.parquet'))):
            raise FileNotFoundError('The parquet file does not exist')
        
        data = self.get_parquet(ticker)
        

        close_values = data[('Close' , ticker)]
       
        close_avg = close_values.mean()
   
        
       
        data = data.reset_index()

        data.index = range(len(data))
        print(data.head())
        close_on_day = data.iloc[-1][('Close' , ticker)]
    
        error = abs(close_avg - close_on_day) / close_avg

        return error #this can be an issue if we are to forward many days in the dataframe as it can skew the actual average.

    def create_merge_prices_table(self) -> pd.DataFrame:
        
        path = os.path.join('Data' , 'Clean' , 'Prices.parquet')
        if os.path.exists(path):
            os.remove(path)
        
        dataframes_list = []
       

        for ticker in self.tickers_list:
            data = self.get_parquet(ticker)
            data = data.set_index('Date')
            dataframes_list.append(data)

        if len(dataframes_list) == 0:
            raise ValueError("No Pandas df's to concat")
        

        prices_table = pd.concat(dataframes_list, axis=1)
        self.to_parquete('Price' , prices_table)
        return prices_table

    def get_daily_return(self , ticker):
        '''
        Returns the daily return for a single ticker over the given period of time
        '''

        data = self.get_parquet(ticker)

        last_data = data.iloc[-1][('Close' , ticker)]
        first_data = data.iloc[0][('Close' , ticker)]
        change =  last_data - first_data
        return change

    
    
    
    def clear_directory(self, path):
        '''
        A method for clearing any directory
        '''
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # remove file or symlink
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # remove subdirectory
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')



        








