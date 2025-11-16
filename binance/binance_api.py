import requests
from typing import List, Dict, Optional
import time 
from datetime import datetime, timedelta

class BinanceAPI: 
    base_url = "https://api.binance.com/api/v3"

    def __init__(self):
        self.session = requests.Session()
    
    def get_currency_pairs (self) -> List[str]:
        try: 
            url = f'{self.base_url}/exchangeInfo'
            response = self.session.get(url)
            response.raise_for_status()

            data = response.json()
            currency_pairs = []
            for i in data.get("symbols", []):
                if i.get("status") == 'TRADING' and i.get('quoteAsset') == "USDT" : 
                    currency_pairs.append(i.get('symbol'))
            print(f'-------Currency pair available in USDT: {sorted(currency_pairs[:10])}------------')
            return sorted(currency_pairs)
        except requests.exceptions.RequestException as e : 
            print("Error in getting currency pairs: {e}")
            return []
    def get_klines(self, symbol: str, interval: str = '1h', limit: int = 1000, start_time: Optional[int] = None , end_time: Optional[int] = None, time_zone: str = '0') -> List[List]: 
        url = f"{self.base_url}/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }

        if start_time: 
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        if time_zone:
            params['timeZone'] = time_zone
        
        try: 
            response = self.session.get(url, params= params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e: 
            print(f'Errors occur when getting kline in {symbol}: {e}')
            return []
    
    def get_klines_in_period (self, symbol: str, start_time: Optional[int] = None, end_time: Optional[int] = None, interval: str = '1h',time_zone: str = '0'):

        current_start = start_time

        all_klines = []
        while True: 
            data_klines = self.get_klines(
                symbol= symbol, 
                interval= interval,
                start_time= current_start,
                end_time= end_time, 
                limit= 1000, 
                time_zone=time_zone
            )
            print(f'\n ------len data_klines: {len(data_klines)}----------')
            if not data_klines:
                print("No more data returned → stop")
                break
            print(f"Fetched {len(data_klines)} klines")
            all_klines.extend(data_klines)
            
            # if data_klines < 1000 means that data has ended
            if len(data_klines) < 1000:
                print("Last page reached → stop")
                break

            # update start_time for next request after break 
            last_close_time = data_klines[-1][6]
            current_start = last_close_time + 1
            print("next_start =", current_start)

            
            if end_time and current_start >= end_time: 
                print("Reached end_time → stop")
                break


            time.sleep(0.1)
        return all_klines

def datetime_to_timestamp_ms ( dt: datetime):
    return int(dt.timestamp() * 1000)
    


  

if __name__ == "__main__":
    a = BinanceAPI()
    b = a.get_currency_pairs()
    print(b)