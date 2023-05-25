import pandas as pd
import glob
import os
from quantrocket.master import get_securities
from quantrocket import get_prices

current_file_path = os.path.dirname(os.path.abspath(__file__))

def get_metadata():
    nasdaq_metadata = pd.read_csv(os.path.join(current_file_path, "metadata", "nasdaq_screener_nasdaq_20230421.csv"), index_col='Symbol')
    nasdaq_metadata["Exchange"] = "Nasdaq"
    nyse_metadata = pd.read_csv(os.path.join(current_file_path, "metadata", "nasdaq_screener_nyse_20230421.csv"), index_col='Symbol')
    nyse_metadata["Exchange"] = "Nyse"
    amex_metadata = pd.read_csv(os.path.join(current_file_path, "metadata", "nasdaq_screener_amex_20230421.csv"), index_col='Symbol')
    amex_metadata["Exchange"] = "Amex"
    metadata = pd.concat([nasdaq_metadata, nyse_metadata, amex_metadata]).sort_index()
    metadata.dropna(inplace=True)
    metadata.index = metadata.index.str.rstrip()
    metadata.index.name = 'ticker'
    metadata.columns = ['name', 'last_sale', 'net_change', 'perc_change', 'market_cap', 'country', 'ipo_year', 'volume', 'sector', 'industry', 'exchange']

    metadata.reset_index(inplace=True)
    metadata.dropna(inplace=True)
    
    return metadata

def get_data_daily_data_to_df(path, prefix):

    # Set the file path pattern to match
    file_pattern = f'{path}/{prefix}/*.csv'

    # Use glob to find all files matching the pattern
    file_list = glob.glob(file_pattern)

    # Create an empty list to hold the data frames for each file
    df_list = []

    # Loop through each file in the list and load it into a data frame
    for file_name in file_list:
        # Load the file into a data frame
        df = pd.read_csv(file_name)
        
        # Add a new column to the data frame with the file name
        df['ticker'] = os.path.splitext(os.path.basename(file_name))[0]
        
        # Append the data frame to the list
        df_list.append(df)

    # Concatenate all the data frames in the list into a single data frame
    df_all = pd.concat(df_list, ignore_index=True)
    df_all['timestamp'] = pd.to_datetime(df_all['timestamp']).dt.date    

    # Print the resulting data frame
    return df_all.dropna()


def get_qr_prices(start_date, end_date):
    securities = get_securities(exchanges=["NASDAQ", "NYSE", "AMEX"], sec_types="STK", fields="*")

    filtered_securities = securities[securities.Delisted==False]
    filtered_securities = filtered_securities[["Symbol", "Exchange", "Name", "Delisted", "alpaca_Status", "usstock_FirstPriceDate"]].dropna()

    metadata = get_metadata()

    metadata = pd.merge(metadata, filtered_securities.reset_index(), left_on="ticker", right_on="Symbol", how="left").dropna()
    metadata.set_index('Sid', inplace=True)
    # prices = get_prices("usstock-free-1d", universes="usstock-free", start_date="2017-01-01", fields=["Close"])
    prices = get_prices("usstock-1d", universes="usstock-active", start_date=start_date, end_date=end_date, fields=["Close"])

    shared_metadata = filtered_securities.index.intersection(metadata.index)
    metadata = metadata.loc[shared_metadata, :]
    # metadata.info()

    prices = prices.reset_index().drop(columns=['Field']).set_index("Date")

    shared_prices = prices.columns.intersection(metadata.index)

    prices = prices.loc[:, shared_prices]
    # prices.info()
    return metadata, prices

# get_qr_prices(start_date="2000-01-01", end_date='2023-03-31')