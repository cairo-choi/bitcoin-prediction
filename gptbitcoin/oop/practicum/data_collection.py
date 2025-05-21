
import yfinance as yf
import pandas as pd
import numpy as np
import requests
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os 
from dotenv import load_dotenv

# load .env
load_dotenv()
snow_flake_user = os.getenv('SNOW_FLAKE_USER')
snow_flake_pwd = os.getenv('SNOW_FLAKE_PASSWORD')
snow_flake_db = os.getenv('SNOW_FLAKE_DATABASE')
snow_flake_schema = os.getenv('SNOW_FLAKE_SCHEMA')
snow_flake_account = os.getenv('SNOW_FLAKE_ACCOUNT')


class DataCollection:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def fetch_asset_df(self, asset_name):
        """
        Fetches historical data for a given asset name from Yahoo Finance.
        Args:
        asset_name (str): The ticker symbol of the asset.
        e.g. BTC ticker: 'BTC-USD', SPDR S&P 500 ETF: 'SPY'
        Returns:
        pandas.DataFrame: A DataFrame containing the historical data.
        """
        df = yf.download(asset_name, start=self.start, end=self.end)
        # Drop the 'Ticker' level from the column index
        if isinstance(df.columns, pd.MultiIndex) and 'Ticker' in df.columns.names:
            df = df.droplevel('Ticker', axis=1)       

        # Prefix asset name to column names (e.g., 'BTC-USD_Close', 'BTC-USD_Open', etc.)
        df.columns = [f"{asset_name}_{col}" for col in df.columns]
        return df
    
    def fetch_hash_rate(self, sampled = False, timespan='all'):
        """
        Fetches historical Bitcoin network hash rate data from the Blockchain.com API.

        Args:
        sampled (bool): Whether to fetch sampled data. If False, the data is as detailed as possible.
        timespan (str): The time range to fetch (e.g., 'all', '30days', '1year'). Defaults to 'all'.
        Returns:
        pandas.DataFrame: A DataFrame with timestamp-indexed hash rate data (column: 'HashRate').
                          The data is filtered by `self.start` and `self.end` if they are set.  
        Raises:
        Exception: If the API request fails (non-200 status code).
        """
        url = "https://api.blockchain.info/charts/hash-rate"
        params = {
            'timespan': timespan,
            'format': 'json',
            'sampled': str(sampled).lower()
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception(f'API requested failed: {response.status_code}')
        data = response.json()
        df = pd.DataFrame(data['values'])
        df['Date'] = pd.to_datetime(df['x'], unit='s')
        df['HashRate'] = df['y']
        df.set_index('Date', inplace=True)
        # Filtering df from 'start' to 'end'
        if self.start:
            df = df[df.index >= pd.to_datetime(self.start)]
        else:
            print('No start date provided.')

        if self.end:
            df = df[df.index < pd.to_datetime(self.end)]
        else:
            print('No end date provided.')
        
        return df


    def bitcoin_log_returns(self, df, price_col='BTC-USD_Close'):
        """
        Adds a 'log_returns' column to the given DataFrame based on the specified price column.
    
        Args:
            df (pandas.DataFrame): The bitcoin DataFrame with historical data.
            price_col (str): The name of the column to use for calculating returns. Defaults to 'Close'.
        
        Returns:
            pandas.DataFrame: The DataFrame with the 'log_returns' column added.
            pandas.Series: The log returns series with NaN values dropped.
        """
        if price_col in df.columns:
            df['BTC_LogReturns'] = np.log(df[price_col] / df[price_col].shift(1))
            log_returns_clean = df['BTC_LogReturns'].dropna()
        else:
            print(f"Warning: Column '{price_col}' not found for calculating returns.")
            df['BTC_LogReturns'] = np.nan
            log_returns_clean = pd.Series(dtype=float)

        return df, log_returns_clean
    
    def merge_df(self):
        btc_df = self.fetch_asset_df('BTC-USD')
        btc_df, log_returns = self.bitcoin_log_returns(btc_df)
        sp500_df = self.fetch_asset_df('SPY')
        hash_rate = self.fetch_hash_rate()
        
        merged_df = pd.concat([btc_df, sp500_df, hash_rate], axis=1)
        merged_df = merged_df.drop(['x', 'y'], axis=1)
        merged_df = merged_df.reset_index()
        merged_df['Date'] = merged_df['Date'].dt.date

        return merged_df
    
    def upload_to_snowflake(self):
        merged_df = self.merge_df()
        conn = connect(
            user = snow_flake_user,
            password = snow_flake_pwd,
            database = snow_flake_db,
            schema = snow_flake_schema,
            account = snow_flake_account
        )

        success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=merged_df,
        table_name='merged_data',
        auto_create_table=True  
        )

        print(f"Upload status: {success}, Rows inserted: {nrows}")


data = DataCollection(start='2014-12-31', end='2025-05-20')
data.upload_to_snowflake()