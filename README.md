# 1. Fetching Exchange Rates from Binance and Loading into BigQuery (Bronze Layer)

## Code Flow

```
binance/
└── binance_api.py         # BinanceAPI Class for API calls

scripts/
├── fetch_kline.py          # Main script to fetch rates
└── load_to_bigquery.py     # Script to load into BigQuery

output/
└── raw_rates/              # Directory for JSONL files
    ├── BTCUSDT.jsonl
    ├── ETHUSDT.jsonl
    └── ...
```

<img width="384" height="396" alt="image" src="https://github.com/user-attachments/assets/89b81724-d5b6-4925-b045-e487609bbdb5" />



## Execution Flow

### 1. Determine Data Scope

From the `transactions.csv` file, determine:

**Currencies to fetch rates for**:
  
- Read the `destination_currency` column to get a list of unique currencies.

- Exclude `USDT` (as it is the conversion unit).

**Time range**:
  
- Start date: `min(created_at)`

- End date: `max(created_at)`

**Script:** `scripts/fetch_kline.py` (function `identify_currencies_and_time_range`)

```python
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
```

### 2. Check for Available Currency Pairs on Binance

1.  Call the `/api/v3/exchangeInfo` API to retrieve a list of all currently traded USDT pairs.
2.  For each currency from 1.1 , check if the pair `{CURRENCY}USDT` exists.
   
       Example: `BTCUSDT`, `ETHUSDT`, `ADAUSDT`
    
4.  Only fetch rates for pairs available on Binance.

**Script:** `scripts/fetch_kline.py` (function `get_available_usdt_pair`)

**API Class:** `binance/binance_api.py` (function `get_currency_pairs`)

```python
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
```

### 3. Fetch Exchange Rate Data from Binance API

For each identified currency pair:

3.1  **Endpoint:** `/api/v3/klines`

3.2  **Parameters:**
* `symbol`: The currency pair (example:  `BTCUSDT`)
* `interval`: `1h` (hourly)
* `startTime`: Timestamp (milliseconds) of the start date
* `endTime`: Timestamp (milliseconds) of the end date
* `limit`: 1000 (limit per request)

3.3  **Handle Pagination:**
* The Binance API limits to 1000 records per request.
* If data > 1000 records, multiple calls are needed:
    * First request: `startTime` = start date
    * Subsequent requests: `startTime` = `close_time` of the last record + 1ms
    * Stop when: records returned < 1000 or `startTime >= endTime`

3.4  **Save Data:**

* Format: **JSONL** (each line is a JSON object)

* Directory: `output/raw_rates/`

* File name: `{SYMBOL}.jsonl` (e.g., `BTCUSDT.jsonl`)

* Structure of each record:
    
```json
{
  "symbol": "BTCUSDT",
  "open_time": 1625097600000,
  "open": 35000.0,
  "high": 35500.0,
  "low": 34800.0,
  "close": 35200.0,
  "volume": 1000.5,
  "close_time": 1625101200000,
  "quote_volume": 35200000.0,
  "trades": 5000,
  "taker_buy_base_volume": 500.2,
  "taker_buy_quote_volume": 17600000.0
}
```


**Script:** `scripts/fetch_kline.py` (function `fetch_and_save_data`)

**API Class:** `binance/binance_api.py` (function `get_klines_in_period`)



### 4. Load Data into BigQuery

1.  **BigQuery Connection:**
* Use the service account JSON from `config/service_account.json`
* Project ID: `cusma-383203`
* Dataset: `raw_data`

2.  **Create Dataset (if not exists):**
* Location: `asia-southeast1`

3.  **Load Exchange Rate Data:**
* Read all `.jsonl` files from `output/raw_rates/`
* Combine all records into a single **DataFrame**
* Load into the table `raw_data.rates`
* Write disposition: `WRITE_TRUNCATE` (overwrite old data)

**Script:** `scripts/load_to_bigquery.py` (function `load_rates`)


<img width="506" height="142" alt="image" src="https://github.com/user-attachments/assets/21bbad40-42be-410a-9c8c-8ad8719ba3c2" />


