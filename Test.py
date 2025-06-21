from data_utils import Data

data = Data('symbols.txt')
data.get_tickers()
retireved_data = data.get_ticker_data(data.tickers_list[0])
print(retireved_data.head())

cleaned_data = data.clean_ticker_data(data.tickers_list[0])

print(data.get_ticker_data(data.tickers_list[0]).head())
