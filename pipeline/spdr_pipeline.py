import os
import time
import glob
import yfinance as yf
import pandas as pd
import duckdb
import warnings
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm
from datetime import date

start_time = time.time()
warnings.filterwarnings('ignore')
today = date.today().strftime('%Y%m%d')

# # # # # # # # # # # # # # # # # # # #

dir_holdings       = 'holdings'
dir_composites     = 'composites'
dir_constituents   = 'constituents'
dir_options_comp   = 'options_composites'
dir_options_consti = 'options_constituents'
# script_dir         = os.path.dirname(os.path.abspath(__file__))
# dir_holdings       = os.path.join(script_dir, dir_holdings)
# dir_composites     = os.path.join(script_dir, dir_composites)
# dir_constituents   = os.path.join(script_dir, dir_constituents)
# dir_options_comp   = os.path.join(script_dir, dir_options_comp)
# dir_options_consti = os.path.join(script_dir, dir_options_consti)
os.makedirs(dir_holdings,       exist_ok=True)
os.makedirs(dir_composites,     exist_ok=True)
os.makedirs(dir_constituents,   exist_ok=True)
os.makedirs(dir_options_comp,   exist_ok=True)
os.makedirs(dir_options_consti, exist_ok=True)

bucket_name = 'rlp_jym_spdr_pipeline'

workers = 10
retries = 3
retry_pause = 5

etfs = [
    # state street etfs
     'XLC', 'XLY', 'XLP'#, 'XLE', 'XLF', 'XLV', 'XLI', 'XLB', 'XLRE', 'XAR',
    # 'KBE', 'XBI', 'KCE', 'XHE', 'XHS', 'XHB', 'KIE', 'XME', 'XES', 'XOP', 
    # 'XPH', 'KRE', 'XRT', 'XSD', 'XSW', 'XTL', 'XTN', 'XLK', 'XLU', 'SPY'
]

# # # # # # # # # # # # # # # # # # # #

def download_holdings(ticker, path):
    for attempt in range(retries):
        try:    
            ssga = f'https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker.lower()}.xlsx'
            duckdb.sql(f"""
                copy (
                    select *
                    from read_xlsx('{ssga}', range = 'A5:H')
                    where Ticker is not null
                ) to '{path}/ssga_{ticker.lower()}_holdings.parquet'
            """)
            break
        except Exception as e:
            if attempt == 2:
                print(f"     {ticker} failed: {e}")
                break
            time.sleep(retry_pause)

def download_price(ticker, path, interval, period):
    for attempt in range(retries):
        try:
            price = yf.download(ticker, interval=interval, period=period, multi_level_index=False, progress=False).reset_index()
            price.columns = price.columns.str.lower()
            price.insert(0, 'symbol', ticker)
            price.to_parquet(f'{path}/{ticker.lower()}_{interval}_price.parquet')
            break
        except Exception as e:
            if attempt == 2:
                print(f"     {ticker} failed: {e}")
                break
            time.sleep(retry_pause)

def download_metadata(ticker, path):
    for attempt in range(retries):
        try:
            metadata  = pd.DataFrame(pd.Series(yf.Ticker(ticker).info))
            metadata  = metadata.T.set_index('symbol').reset_index()
            metadata.to_parquet(f'{path}/{ticker.lower()}_metadata.parquet')
            break
        except Exception as e:
            if attempt == 2:
                print(f"     {ticker} failed: {e}")
                break
            time.sleep(retry_pause)

def download_financials(ticker, path):
    for attempt in range(retries):
        try:
            yf.Ticker(ticker).ttm_income_stmt.T.reset_index().to_parquet(f'{path}/{ticker.lower()}_ttm_income_financial.parquet')
            yf.Ticker(ticker).ttm_cashflow.T.reset_index().to_parquet(f'{path}/{ticker.lower()}_ttm_cashflow_financial.parquet')
            yf.Ticker(ticker).quarterly_income_stmt.T.reset_index().to_parquet(f'{path}/{ticker.lower()}_qtr_income_financial.parquet')
            yf.Ticker(ticker).quarterly_cashflow.T.reset_index().to_parquet(f'{path}/{ticker.lower()}_qtr_cashflow_financial.parquet')
            yf.Ticker(ticker).quarterly_balance_sheet.T.reset_index().to_parquet(f'{path}/{ticker.lower()}_qtr_assets_financial.parquet')
            yf.Ticker(ticker).earnings_dates.reset_index().to_parquet(f'{path}/{ticker.lower()}_release_dates_financial.parquet')
            break
        except Exception as e:
            if attempt == 2:
                print(f"     {ticker} failed: {e}")
                break
            time.sleep(retry_pause)

def download_options(ticker, path):
    for attempt in range(retries):
        try:
            chain = yf.Ticker(ticker).option_chain()
            chain.calls.to_parquet(f'{path}/{ticker.lower()}_calls_{today}_options.parquet')
            chain.puts.to_parquet(f'{path}/{ticker.lower()}_puts_{today}_options.parquet')
            break
        except Exception as e:
            if attempt == 2:
                print(f"     {ticker} failed: {e}")
                break
            time.sleep(retry_pause)

def upload_to_gcs(local_path, bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

def upload_many_to_gcs(files, bucket_name):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for local_path, blob_name in files:
            executor.submit(upload_to_gcs, local_path, bucket_name, blob_name)

# # # # # # # # # # # # # # # # # # # #

print('SPDR PIPELINE\n')

print('Composites:\n')

# # # # #

print('     Downloading holdings')
path = dir_holdings
for ticker in tqdm(etfs):
    download_holdings(ticker, path)

pattern = f'{path}/*holdings.parquet'
actual_files = glob.glob(pattern)
files_to_upload = [
    (local_file, f"{path}/{os.path.basename(local_file)}")
    for local_file in actual_files
]
upload_many_to_gcs(files_to_upload, bucket_name)

# # # # #

print('     Downloading price')
path = dir_composites
for ticker in tqdm(etfs):
    download_price(ticker, path, '1h', '2y')  
    download_price(ticker, path, '1d', '5y')  
    download_price(ticker, path, '1wk', 'max')

pattern = f'{path}/*price.parquet'
actual_files = glob.glob(pattern)
files_to_upload = [
    (local_file, f"{path}/{os.path.basename(local_file)}")
    for local_file in actual_files
]
upload_many_to_gcs(files_to_upload, bucket_name)

# # # # #

print('     Downloading metadata')
path = dir_composites
for ticker in tqdm(etfs):
    download_metadata(ticker, path)

pattern = f'{path}/*metadata.parquet'
actual_files = glob.glob(pattern)
files_to_upload = [
    (local_file, f"{path}/{os.path.basename(local_file)}")
    for local_file in actual_files
]
upload_many_to_gcs(files_to_upload, bucket_name)

# # # # #

print('     Downloading option chain')
path = dir_options_comp
for ticker in tqdm(etfs):
    download_options(ticker, path)

pattern = f'{path}/*options.parquet'
actual_files = glob.glob(pattern)
files_to_upload = [
    (local_file, f"{path}/{os.path.basename(local_file)}")
    for local_file in actual_files
]
upload_many_to_gcs(files_to_upload, bucket_name)

# # # # # # # # # # # # # # # # # # # #

print('\nConstituents:\n')

#
get_uniques_ssga = duckdb.sql(f"""
    with 
    all_holdings as (
        select
            split_part(
                split_part(filename, '_', 2), '.', 1) as etf,
            Ticker as ticker, 
            Name as name, 
            SEDOL as sedol, 
            Weight as weight, 
            "Local Currency" as currency
        from read_parquet('{dir_holdings}/ssga*', union_by_name=True)
        where
            sedol != '-' and
            weight > 0
    )
    select
        distinct ticker
    from all_holdings
""").fetchdf()
uniques = get_uniques_ssga['ticker'].tolist()

# # # # #

print('     Downloading price')
path = dir_constituents
for ticker in tqdm(uniques):
    download_price(ticker, path, '1h', '2y')  
    download_price(ticker, path, '1d', '5y')  
    download_price(ticker, path, '1wk', 'max')

pattern = f'{path}/*price.parquet'
actual_files = glob.glob(pattern)
files_to_upload = [
    (local_file, f"{path}/{os.path.basename(local_file)}")
    for local_file in actual_files
]
upload_many_to_gcs(files_to_upload, bucket_name)

# # # # #

print('     Downloading metadata')
path = dir_constituents
for ticker in tqdm(uniques):
    download_metadata(ticker, path)

pattern = f'{path}/*metadata.parquet'
actual_files = glob.glob(pattern)
files_to_upload = [
    (local_file, f"{path}/{os.path.basename(local_file)}")
    for local_file in actual_files
]
upload_many_to_gcs(files_to_upload, bucket_name)

# # # # #

print('     Downloading financials')
path = dir_constituents
for ticker in tqdm(uniques):
    download_financials(ticker, path)

pattern = f'{path}/*financials.parquet'
actual_files = glob.glob(pattern)
files_to_upload = [
    (local_file, f"{path}/{os.path.basename(local_file)}")
    for local_file in actual_files
]
upload_many_to_gcs(files_to_upload, bucket_name)

# # # # #

print('     Downloading option chain')
path = dir_options_consti
for ticker in tqdm(uniques):
    download_options(ticker, path)

pattern = f'{path}/*options.parquet'
actual_files = glob.glob(pattern)
files_to_upload = [
    (local_file, f"{path}/{os.path.basename(local_file)}")
    for local_file in actual_files
]
upload_many_to_gcs(files_to_upload, bucket_name)

print(f"\nDone in {time.time()-start_time:.2f}s")
input("\nPress any key to exit.")