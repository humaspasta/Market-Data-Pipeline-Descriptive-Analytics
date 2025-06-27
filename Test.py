import os 
from data_utils import Data


data = Data(os.path.join('Data' , 'symbols.txt') , ('2024-06-16' , '2025-06-16') , os.path.join('Data' , 'logs.txt'))
#data.clear_directory(os.path.join("Data" , "Clean"))
data.clear_cache()
data.get_tickers()

print(data.calculate_dividend_error('SPY' , '2025-01-13'))







