import os
import time
import yfinance as yf
import pandas as pd
import duckdb
import warnings
from tqdm.auto import tqdm
warnings.filterwarnings('ignore')
start_time = time.time()


dir_composites   = 'Composites'
dir_constituents = 'Constituents'

script_dir        = os.path.dirname(os.path.abspath(__file__))
dir_composites    = os.path.join(script_dir, dir_composites)
dir_constituents  = os.path.join(script_dir, dir_constituents)

os.makedirs(dir_composites,   exist_ok=True)
os.makedirs(dir_constituents, exist_ok=True)

retries = 3
retry_pause = 5

etfs = [
    'XLC', 'XLY', 'XLP', 'XLE', 'XLF', 'XLV', 'XLI', 'XLB', 'XLRE', 'XAR',
    'KBE', 'XBI', 'KCE', 'XHE', 'XHS', 'XHB', 'KIE', 'XME', 'XES', 'XOP', 
    'XPH', 'KRE', 'XRT', 'XSD', 'XSW', 'XTL', 'XTN', 'XLK', 'XLU', 'SPY'
]

def download_price(ticker, path):

    for attempt in range(retries):
        try:
            price = yf.download(ticker, period='5y', progress=False).reset_index()
            break
        except Exception as e:
            if attempt == 2:
                print(f"{ticker} failed: {e}")
            time.sleep(retry_pause)
                
    price.columns = price.columns.droplevel(1).str.lower()
    price.insert(0, 'symbol', ticker)
    price.to_parquet(f'{path}/{ticker}_price.parquet')

def download_metadata(ticker, path):

    for attempt in range(retries):
        try:
            metadata = pd.DataFrame(pd.Series(yf.Ticker(ticker).info))
        except Exception as e:
            if attempt == 2:
                print(f"{ticker} failed: {e}")
            time.sleep(retry_pause)
            
    metadata = metadata.T.set_index('symbol').reset_index()
    metadata.to_parquet(f'{path}/{ticker}_meta.parquet')

def download_holdings(ticker, path):

    for attempt in range(retries):
        try:
            holdings = yf.Ticker(ticker).funds_data.top_holdings.reset_index()
        except Exception as e:
            if attempt == 2:
                print(f"{ticker} failed: {e}")
            time.sleep(retry_pause)
    
    holdings.columns = ['symbol', 'name', 'weight']
    holdings.insert(0, 'etf', ticker)
    holdings.to_parquet(f'{path}/{ticker}_holdings.parquet')

def download_weightings(ticker, path):

    for attempt in range(retries):
        try:
            sectors = pd.DataFrame(pd.Series(yf.Ticker(ticker).funds_data.sector_weightings)).reset_index()
        except Exception as e:
            if attempt == 2:
                print(f"{ticker} failed: {e}")
            time.sleep(retry_pause)
            
    sectors.columns = ['sector', 'weight']
    sectors.insert(0, 'etf', ticker)
    sectors.reset_index()
    sectors.to_parquet(f'{path}/{ticker}_sectors.parquet')


print('SPDR PIPELINE\n')

print('Composites:\n')
path = dir_composites

print('     Downloading price...')
for ticker in tqdm(etfs):
    download_price(ticker, path)
print('     Downloading metadata...')
for ticker in tqdm(etfs):
    download_metadata(ticker, path)
print('     Downloading holdings...')
for ticker in tqdm(etfs):
    download_holdings(ticker, path)
print('     Downloading weightings...')
for ticker in tqdm(etfs):
    download_weightings(ticker, path)


print('\nConstituents:\n')
path = dir_constituents

fund_holdings = duckdb.sql(f"""
    select distinct symbol
    from read_parquet(
        '{dir_composites}/*holdings.parquet', 
        union_by_name=True)
""").fetchdf()
positions = fund_holdings['symbol'].tolist()

print('     Downloading price...')
for ticker in tqdm(positions):
    download_price(ticker, path)
print('     Downloading metadata...')
for ticker in tqdm(positions):
    download_metadata(ticker, path)


print(f"\nDone in {time.time()-start_time:.2f}s")
input("Press any key to exit.")