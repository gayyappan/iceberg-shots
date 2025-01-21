import datetime
import os
import pandas as pd

import yfinance as yf
start_date = datetime.datetime(2023, 11, 1)
end_date = datetime.datetime(2023, 12, 31)
download_dir = os.path.join(os.getcwd(), 'marketdata')

##magnificient_seven = ['AAPL', 'AMZN', 'GOOGL', 'META', 'MSFT', 'NVDA', 'TSLA']
magnificient_seven = ['AAPL', 'AMZN']
for ticker_string in magnificient_seven:
   ticker = yf.Ticker(ticker_string)
   ### historical_data is a dataframe
   historical_data = ticker.history(start=start_date, end=end_date)
   raw_dt_mod=pd.to_datetime(historical_data.index, unit='ms')
   historical_data["Date_new"]=raw_dt_mod.tz_localize(None)
   df3 = historical_data.copy(deep=True)
   df3.reset_index(drop=True, inplace=True)
###df3.index
   file_path = os.path.join(download_dir, f'{ticker_string}.parquet')
   ###historical_data.to_parquet(file_path, engine='pyarrow', coerce_timestamps='ms')
   df3.to_parquet(file_path, engine='pyarrow', coerce_timestamps='ms')
   print(f'{ticker_string} downloaded.')
