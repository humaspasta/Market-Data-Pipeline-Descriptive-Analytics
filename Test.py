import os 
from data_utils import Data


data = Data(os.path.join('Data' , 'symbols.txt') , ('2024-06-16' , '2025-06-16') , os.path.join('Data' , 'logs.txt'))
data.clear_cache()
data.get_tickers()

present_data = data.clean_ticker_data(data.tickers_list[0])

data.to_parquete('AAPL')
data_parqed = data.get_parquet('AAPL')
print(data_parqed)


