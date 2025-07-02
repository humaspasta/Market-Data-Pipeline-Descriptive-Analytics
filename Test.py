import os 
from data_utils import Data
import pandas as pd
data = Data(os.path.join('Data' , 'symbols.txt') , ('2023-03-12' , '2023-04-12'), os.path.join('Data' , 'logs.txt'))
data.clear_cache()
tickers = data.get_tickers()

data_app = data.clean_ticker_data('AAPL')

print(data_app)