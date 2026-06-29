import os
import time
import glob
import yfinance as yf
import pandas as pd
import duckdb
import warnings
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm
from datetime import date

start_time = time.time()
warnings.filterwarnings('ignore')
today = date.today().strftime('%Y%m%d')

# # # # # # # # # # # # # # # # # # # #

dir_uploads        = 'uploads'
dir_holdings       = 'holdings'
dir_composites     = 'composites'
dir_constituents   = 'constituents'
dir_options_comp   = 'options_composites'
dir_options_consti = 'options_constituents'
# script_dir         = os.path.dirname(os.path.abspath(__file__))
# dir_uploads        = os.path.join(script_dir, dir_uploads)
# dir_holdings       = os.path.join(script_dir, dir_holdings)
# dir_composites     = os.path.join(script_dir, dir_composites)
# dir_constituents   = os.path.join(script_dir, dir_constituents)
# dir_options_comp   = os.path.join(script_dir, dir_options_comp)
# dir_options_consti = os.path.join(script_dir, dir_options_consti)
os.makedirs(dir_uploads,        exist_ok=True)
os.makedirs(dir_holdings,       exist_ok=True)
os.makedirs(dir_composites,     exist_ok=True)
os.makedirs(dir_constituents,   exist_ok=True)
os.makedirs(dir_options_comp,   exist_ok=True)
os.makedirs(dir_options_consti, exist_ok=True)

bucket_name = 'rlp_jym_spdr_pipeline'

workers = 2
retries = 3
retry_pause = 5

etfs = [
    # state street etfs
    'XLC', 'XLY', 'XLP', 'XLE', 'XLF', 'XLV', 'XLI', 'XLB', 'XLRE', 'XAR',
    'KBE', 'XBI', 'KCE', 'XHE', 'XHS', 'XHB', 'KIE', 'XME', 'XES', 'XOP', 
    'XPH', 'KRE', 'XRT', 'XSD', 'XSW', 'XTL', 'XTN', 'XLK', 'XLU', 'SPY'
]

# # # # # # # # # # # # # # # # # # # #

def download_holdings(ticker, path):
    for attempt in range(retries):
        try:    
            ssga = f'https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker.lower()}.xlsx'
            duckdb.sql(f"""
                copy (
                    select '{ticker}' as etf, *
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

def download_price_fast(tickers, path, interval, period):
    os.makedirs(path, exist_ok=True)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_ticker = {
            executor.submit(download_price, ticker, path, interval, period): ticker 
            for ticker in tickers
        }
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                future.result()
            except Exception as exc:
                print(f"     {ticker} generated an unexpected exception: {exc}")

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
            ttm_income = yf.Ticker(ticker).ttm_income_stmt.T
            ttm_income.insert(0, 'symbol', ticker)
            ttm_income.reset_index()
            ttm_income.to_parquet(f'{path}/{ticker.lower()}_ttm_income_financial.parquet')
            ttm_cashflow = yf.Ticker(ticker).ttm_cashflow.T
            ttm_cashflow.insert(0, 'symbol', ticker)
            ttm_cashflow.reset_index()
            ttm_cashflow.to_parquet(f'{path}/{ticker.lower()}_ttm_cashflow_financial.parquet')
            qtr_income = yf.Ticker(ticker).quarterly_income_stmt.T
            qtr_income.insert(0, 'symbol', ticker)
            qtr_income.reset_index()
            qtr_income.to_parquet(f'{path}/{ticker.lower()}_qtr_income_financial.parquet')
            qtr_cashflow = yf.Ticker(ticker).quarterly_cashflow.T
            qtr_cashflow.insert(0, 'symbol', ticker)
            qtr_cashflow.reset_index()
            qtr_cashflow.to_parquet(f'{path}/{ticker.lower()}_qtr_cashflow_financial.parquet')
            qtr_assets = yf.Ticker(ticker).quarterly_balance_sheet.T
            qtr_assets.insert(0, 'symbol', ticker)
            qtr_assets.reset_index()
            qtr_assets.to_parquet(f'{path}/{ticker.lower()}_qtr_assets_financial.parquet')
            dates = yf.Ticker(ticker).earnings_dates.T
            dates.insert(0, 'symbol', ticker)
            dates.reset_index()
            dates.to_parquet(f'{path}/{ticker.lower()}_release_dates_financial.parquet')
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
            calls = chain.calls
            calls.insert(0, 'type', 'calls')
            calls.insert(1, 'symbol', ticker)
            puts = chain.puts
            puts.insert(0, 'type', 'puts')
            puts.insert(1, 'symbol', ticker)
            calls.to_parquet(f'{path}/{ticker.lower()}_{today}_call_options.parquet')
            puts.to_parquet(f'{path}/{ticker.lower()}_{today}_put_options.parquet')
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
print('Composites:')

# # #
print('\n     Downloading holdings')
path = dir_holdings
for ticker in tqdm(etfs):
    download_holdings(ticker, path)

file_name = f'{dir_uploads}/comp_holdings_ssga.parquet'
duckdb.sql(f"""
    copy (
        select *
        from read_parquet('{path}/ssga*', union_by_name=True)
    ) to '{file_name}'
""")
upload_to_gcs(file_name, bucket_name, file_name)

# # #
print('\n     Downloading price')
path = dir_composites
for ticker in tqdm(etfs):
    download_price(ticker, path, '1h', '2y')  
    download_price(ticker, path, '1d', '5y')  
    download_price(ticker, path, '1wk', 'max')

for tf in ('1h', '1d', '1wk'):
    file_name = f'{dir_uploads}/comp_price_{tf}.parquet'
    duckdb.sql(f"""
        copy (
            select *
            from read_parquet('{path}/*{tf}_price.parquet', union_by_name=True)
        ) to '{file_name}'
    """)
    upload_to_gcs(file_name, bucket_name, file_name)

# # #
print('\n     Downloading metadata')
path = dir_composites
for ticker in tqdm(etfs):
    download_metadata(ticker, path)

file_name = f'{dir_uploads}/comp_metadata.parquet'
duckdb.sql(f"""
    copy (
        select *
        from read_parquet('{path}/*metadata.parquet', union_by_name=True)
    ) to '{file_name}'
""")
upload_to_gcs(file_name, bucket_name, file_name)

# # #
print('\n     Downloading option chain')
path = dir_options_comp
for ticker in tqdm(etfs):
    download_options(ticker, path)

for opt in ('call', 'put'):
    file_name = f'{dir_uploads}/comp_options_{opt}s.parquet'
    duckdb.sql(f"""
        copy (
            select *
            from read_parquet('{path}/*{opt}_options.parquet', union_by_name=True)
        ) to '{file_name}'
    """)
    upload_to_gcs(file_name, bucket_name, file_name)

# # # # # # # # # # # # # # # # # # # #
print('\nConstituents:')

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

# # #
print('\n     Downloading price')
path = dir_constituents
for ticker in tqdm(uniques):
    download_price(ticker, path, '1h', '2y')  
    download_price(ticker, path, '1d', '5y')  
    download_price(ticker, path, '1wk', 'max')
    # download_price_fast(ticker, path, '1h', '2y')  
    # download_price_fast(ticker, path, '1d', '5y')  
    # download_price_fast(ticker, path, '1wk', 'max')

for tf in ('1h', '1d', '1wk'):
    file_name = f'{dir_uploads}/consti_price_{tf}.parquet'
    duckdb.sql(f"""
        copy (
            select *
            from read_parquet('{path}/*{tf}_price.parquet', union_by_name=True)
        ) to '{file_name}'
    """)
    upload_to_gcs(file_name, bucket_name, file_name)

# # #
print('\n     Downloading metadata')
path = dir_constituents
for ticker in tqdm(uniques):
    download_metadata(ticker, path)

file_name = f'{dir_uploads}/consti_metadata.parquet'
duckdb.sql(f"""
    copy (
        select *
        from read_parquet('{path}/*metadata.parquet', union_by_name=True)
    ) to '{file_name}'
""")
upload_to_gcs(file_name, bucket_name, file_name)

# # #
print('\n     Downloading option chain')
path = dir_options_consti
for ticker in tqdm(uniques):
    download_options(ticker, path)

for opt in ('call', 'put'):
    file_name = f'{dir_uploads}/consti_options_{opt}s.parquet'
    duckdb.sql(f"""
        copy (
            select *
            from read_parquet('{path}/*{opt}_options.parquet', union_by_name=True)
        ) to '{file_name}'
    """)
    upload_to_gcs(file_name, bucket_name, file_name)

# # #
print('\n     Downloading financials')
path = dir_constituents
for ticker in tqdm(uniques):
    download_financials(ticker, path)

for opt in (
    'ttm_income', 'ttm_cashflow',
    'qtr_income', 'qtr_cashflow', 'qtr_assets', 
    'dates'):
    file_name = f'{dir_uploads}/consti_financials_{opt}.parquet'
    duckdb.sql(f"""
        copy (
            select *
            from read_parquet('{path}/*{opt}_financial.parquet', union_by_name=True)
        ) to '{file_name}'
    """)
    upload_to_gcs(file_name, bucket_name, file_name)

print(f'\nDone in {time.time()-start_time:.2f}s')
input(f'\nPress any key to exit.')