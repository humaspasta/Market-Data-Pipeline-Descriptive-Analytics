# Market-Data-Pipeline-Descriptive-Analytics

Create a cached, quality‑checked data store for 6–10 symbols plus a notebook
that visualises price history, return distribution, correlations, and drawdowns.

# How to use it:

1) Clone this repository
2) In symbols.txt, type all tickers you wish to analyze. Ensure that they are comma seperated and the ticker is a valid ticker.
3) In the  project_one_data_pipeline notebook, change start_date and end_date variables accordingly. Dates should be a string that follows the '%Y-%m-%d' format. All of this information will be stored inside the variable 'data' which is a Data object from the data_utils.py file. 'data' will be used to manipulate and access data pertaining to the start and end date that you have inserted when creating a Data object. 
4) To create a merged data table, run data.create_merged_prices_table(). This will return a large data frame with combined data from all tickers. This will also create a price.parquet file.
5) All parquet files can be accessed with data.get_parquet(filename). Filename must only contain the name itself and not the .parqet. 
6) Run all code within the notebook to get descriptive graphs, correlation heatmap, log returned histogram, and drawdown curve.
7) Adjust paramter values or use methods inside of data_utils.py for further custom analysis.

# NOTE
1) Parquet files are made for each ticker analyzed after data.clean_ticker_data(ticker) is run. All other code relies on these parquet files. To retrieve fresh data. Run data.clear_directory('PATH OF Data/Clean ON YOUR SYSTEM')
2) Since calls to data.clean_ticker_data(ticker) and data.get_ticker_data(ticker) to reduce calls to yfinance, run data_utils.clear_cache() to clear cached data and then run data.clean_ticker_data(ticker) or data.get_ticker_data(ticker)
3) All na values are dropped by default when creating parquet files.
4) drawdown information and any data pertaining to it is not included in the log returned histogram. 
