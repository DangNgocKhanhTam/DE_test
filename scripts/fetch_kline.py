import pandas as pd
from datetime import datetime
import os
from pathlib import Path
import json
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from binance.binance_api import BinanceAPI, datetime_to_timestamp_ms

def read_transaction(file_path:str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    print(f'------Read {file_path} sucessfully------')
    df['created_at'] = pd.to_datetime(df["created_at"])
    print(f'------Convert created_at to datetime--------')
    return df 

def identify_currencies_and_time_range(df: pd.DataFrame) -> tuple:
    unique_currencies = df['destination_currency'].unique().tolist()
    print(f'-----Found {len(unique_currencies)} destination currency')
    currency_counts = df.groupby("destination_currency")["txn_id"].count().sort_values(ascending= False)
    print(f'-----Number of transactions by destination currency---------')
    print(currency_counts)

    start_date = df['created_at'].min()
    end_date = df['created_at'].max()
    print(f'-------start_date : {start_date}---------')
    print(f'-------end_date : {end_date}---------')

    time_range = end_date - start_date 
    print(f'----timerange: {time_range.days} days, {time_range.seconds // 3600} hour---------')

    return unique_currencies, start_date, end_date

def get_available_usdt_pair (api: BinanceAPI, currencies: list) -> list:

    all_usdt_pairs = api.get_currency_pairs()
    available_pairs = []
    unavailable_pairs = []
    print(f'---Number of USDT pair in Binance: {len(all_usdt_pairs)}')

    for i in currencies: 
        # Skip destination currency is USDT 
        if i == "USDT":
            continue 

        pair = f'{i}USDT'

        if pair in all_usdt_pairs:
            available_pairs.append(pair)
            print(f'------{pair} - is available in Binance-----')
        else:
            unavailable_pairs.append(i)
            print(f'------{i} - is unavailable in Binance------')
    print(f'-----Having unavailable {len(unavailable_pairs)} pairs/{len(currencies)} destination currencies : {unavailable_pairs}')
    print(f'-----Having available {len(available_pairs)} pairs/{len(currencies)} destination currencies ')
    return available_pairs

def fetch_and_save_data (api: BinanceAPI, start_date: datetime, end_date: datetime, pairs: list, output_dir: str):
    start_timestamp = datetime_to_timestamp_ms(start_date)
    end_timestamp = datetime_to_timestamp_ms(end_date)
    # get close price for currrency pair 
    for i, pair in enumerate(pairs, 1):
        print('pair', pair)
        print(f'---------{i}/{len(pairs)} is processing... ----------')
        try: 
            klines = api.get_klines_in_period(symbol=pair, start_time= start_timestamp,end_time=end_timestamp, interval= '1h')

            if not klines: 
                print(f'------No data in pair {pair}-------')
                continue 
            # Save data in JSONL 
            os.makedirs(output_dir, exist_ok=True) 
            output_file = os.path.join(output_dir, f"{pair}.jsonl")

            saved_file = 0
            with open(output_file, 'w', encoding= 'utf-8') as f: 
                for kline in klines: 
                    kline_dict = {
                        'symbol': pair,
                        'open_time': kline[0],           
                        'open': float(kline[1]),         
                        'high': float(kline[2]),        
                        'low': float(kline[3]),         
                        'close': float(kline[4]),        
                        'volume': float(kline[5]),       
                        'close_time': kline[6],          
                        'quote_volume': float(kline[7]), 
                        'trades': int(kline[8]),         
                        'taker_buy_base_volume': float(kline[9]),
                        'taker_buy_quote_volume': float(kline[10])
                    }
                    f.write(json.dumps(kline_dict) + "\n")
                    saved_file += 1 

            print(f'-----Saved {saved_file} klines in : {output_file}')
            
        except Exception as e : 
            print(f'--------Error in processing pair: {pair}: {e}')
            continue 

def main(): 
    transactions_file = os.path.join('data', 'transactions.csv')
    output_dir = 'output/raw_rates'
    try: 
        df = read_transaction(transactions_file)
    except FileExistsError:
        print(f'------Not found {transactions_file}--------')
        return 

    except Exception as e: 
        print(f'-------Error when reading file : {transactions_file} ---------')
        return
    unique_currencies, start_date, end_date = identify_currencies_and_time_range(df)

    api = BinanceAPI()
    get_available_usdt_pair(api= api, currencies=unique_currencies)
    available_pairs = get_available_usdt_pair(api= api, currencies=unique_currencies)
    

    fetch_and_save_data(
        api= api, 
        pairs=available_pairs, 
        start_date= start_date, 
        end_date=end_date, 
        output_dir=output_dir
    )

    print(f'--------Sucessfully fetch data in folder: {output_dir}/')

    jsonl_file = [i for i in os.listdir(output_dir)]
    print(f"-----Total JSONL files in '{output_dir}': {len(jsonl_file)} -----")


if __name__ == "__main__":
    main()







