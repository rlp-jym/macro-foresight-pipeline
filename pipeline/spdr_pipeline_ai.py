import os
import click
import yfinance as yf
import pandas as pd
import duckdb
from sqlalchemy import create_engine

@click.command()
@click.option(
    '--period', default='1800-01-01', help='Start date (YYYY-MM-DD)'
)

def run_pipeline(period):
    
    # SPDR Sector ETFs
    ticker_list = [
        'XLC', 'XLY', 'XLP', 'XLE', 'XLF', 'XLV', 'XLI', 'XLB', 'XLRE', 'XAR',
        'KBE', 'XBI', 'KCE', 'XHE', 'XHS', 'XHB', 'KIE', 'XME', 'XES', 'XOP', 
        'XPH', 'KRE', 'XRT', 'XSD', 'XSW', 'XTL', 'XTN', 'XLK', 'XLU'
    ]

    # 1. Download Price
    df_price = yf.download(ticker_list, start=period)['Close']
    df_price.dropna(inplace=True)
    df_price = df_price.reset_index()
    df_price_long = df_price.melt(id_vars=['Date'], var_name='symbol', value_name='price')
    df_price_long = df_price_long.sort_values(['Date', 'symbol']).reset_index(drop=True).rename(columns={'Date': 'date', 'price': 'close'})

    # 2. Download Metadata
    metadata_list = []
    for symbol in ticker_list:
        try:
            info = yf.Ticker(symbol).info
            metadata_list.append({
                'symbol': symbol,
                'name': info.get('longName') or info.get('shortName') or 'N/A',
                'aum': info.get('totalAssets', 0),
                'div_yield': (info.get('yield') or 0) * 100,
            })
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
    df_metadata = pd.DataFrame(metadata_list)

    # 3. Download Top Sectors
    ranks = 2
    sector_weight = []
    for symbol in ticker_list:
        try:
            ticker = yf.Ticker(symbol)
            weightings = ticker.funds_data.sector_weightings
            if weightings and isinstance(weightings, dict):
                sorted_items = sorted(weightings.items(), key=lambda x: x[1], reverse=True)[:ranks]
                row = {'symbol': symbol}
                for i, (sector, weight) in enumerate(sorted_items, start=1):
                    row[f'top{i}_sector'] = sector
                    row[f'top{i}_weight'] = weight * 100
                for i in range(len(sorted_items) + 1, ranks + 1):
                    row[f'top{i}_sector'] = None
                    row[f'top{i}_weight'] = None
            else:
                row = {'symbol': symbol}
                for i in range(1, ranks + 1):
                    row[f'top{i}_sector'] = None
                    row[f'top{i}_weight'] = None
            sector_weight.append(row)
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            row = {'symbol': symbol}
            for i in range(1, ranks + 1):
                row[f'top{i}_sector'] = None
                row[f'top{i}_weight'] = None
            sector_weight.append(row)
    df_top_sector = pd.DataFrame(sector_weight)

    # 4. Combine Metadata and Top Sectors
    df_metadata_extended = duckdb.sql("""
    SELECT
        a.symbol, name, 
        ROUND(TRY_CAST(aum AS BIGINT) / 1e9, 2) AS aum_bn,
        ROUND(TRY_CAST(div_yield AS DOUBLE), 2) AS div_yield_pct,
        top1_sector AS top1,
        ROUND(TRY_CAST(top1_weight AS DOUBLE), 2) AS top1_pct,
        top2_sector AS top2,
        ROUND(TRY_CAST(top2_weight AS DOUBLE), 2) AS top2_pct,
        ROUND(TRY_CAST(top1_weight AS DOUBLE) + TRY_CAST(top2_weight AS DOUBLE), 2) AS top_pct,
        ROUND(100 - (TRY_CAST(top1_weight AS DOUBLE) + TRY_CAST(top2_weight AS DOUBLE)), 2) AS others_pct
    FROM df_metadata a
    LEFT JOIN df_top_sector b ON a.symbol = b.symbol
    ORDER BY aum_bn DESC
    """).fetchdf()

    # 5. Export to Postgres
    db_url = os.environ.get(
        "DATABASE_URL","postgresql+psycopg://spdr:spdr@172.17.0.1:5432/spdr_etfs").replace(
        "postgresql://", "postgresql+psycopg://")
    engine = create_engine(db_url)

    df_metadata_extended.head(n=0).to_sql(
        'spdr_etfs_meta', engine, 
        if_exists='replace', #index=False
    )
    df_metadata_extended.to_sql(
        'spdr_etfs_meta', engine, 
        if_exists='append', #index=False
    )

    df_price_long.head(n=0).to_sql(
        'spdr_etfs_price', engine, 
        if_exists='replace', #index=False
    )
    df_price_long.to_sql(
        'spdr_etfs_price', engine, 
        if_exists='append', #index=False
        chunksize=1000
    )

if __name__ == '__main__':
    run_pipeline(standalone_mode=False)