
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


# Data Model with dbt



## Data Model Architecture: 3-Layer Structure

<img width="1132" height="487" alt="image" src="https://github.com/user-attachments/assets/5dd3557f-39d8-48cb-88a8-15576554b1fd" />


### Why Star Schema

1. **BI-friendly:** Star schema is the standard for BI, easy to query and good performance
2. **Clear separation:** Dimensions and Facts are separated, easy to maintain
3. **SCD Type 2:** Naturally supports tracking KYC level history
4. **Easy to extend:** Can add new dimensions or facts without affecting existing structure

### Code Structure
```
models/
└── finance/
    ├── staging/
    │   ├── stg_users.sql
    │   ├── stg_transactions.sql
    │   ├── stg_rates.sql
    │   └── schema.yml          # Tests and documentation
    ├── intermediate/
    │   ├── int_users_scd2.sql
    │   ├── int_rates_usdt.sql
    │   └── int_transactions_with_rates.sql
    └── marts/
        ├── dim_users.sql
        ├── dim_currencies.sql
        ├── dim_date.sql
        ├── fct_transactions.sql
        ├── tb_transactions_by_kyc.sql
        ├── tb_transactions_tracking.sql
        └── schema.yml          # Tests and documentation
```
## Layer Details

### 1. BRONZE LAYER (Staging)

#### 1.1. stg_users

**File:** `models/finance/staging/stg_users.sql`

```sql
-- Read from raw_data.users
-- Cast: user_id, kyc_level → int64
-- Cast: created_at, updated_at → timestamp
```

#### 1.2. stg_transactions

**File:** `models/finance/staging/stg_transactions.sql`

```sql
-- Read from raw_data.transaction
-- Cast: txn_id, user_id → int64
-- Cast: amounts → float64
-- Cast: created_at → timestamp
```

#### 1.3. stg_rates

**File:** `models/finance/staging/stg_rates.sql`

```sql
-- Read from raw_data.rates
-- Rename columns: close → close_price, open → open_price
-- Convert timestamp: open_time, close_time (milliseconds) → timestamp
-- Create: price_timestamp, close_timestamp
```

**Configuration:** Materialized as `view` (lightweight, no storage cost)

### 2. SILVER LAYER (Intermediate)

**Purpose:** Handle business logic, joins, complex transformations

#### 2.1. int_users_scd2 (SCD Type 2)

**File:** `models/finance/intermediate/int_users_scd2.sql`

**Purpose:** Create SCD Type 2 to track KYC level change history

**Logic:**

1. **Identify periods:**
   ```sql
   period_start = created_at
   period_end = COALESCE(updated_at, '9999-12-31')  -- If not updated = current
   ```

2. **Create surrogate key:**
   ```sql
   user_kyc_key = dbt_utils.generate_surrogate_key(['user_id', 'period_start'])
   ```

3. **Mark current record:**
   ```sql
   is_current = (updated_at IS NULL OR updated_at = created_at)
   ```

**Result:** Each user can have multiple records, each representing a period with a specific KYC level.

**Example:**
```
user_id | kyc_level | effective_from | effective_to      | is_current
--------|-----------|----------------|-------------------|------------
100     | 1         | 2025-01-01     | 2025-06-15        | false
100     | 2         | 2025-06-15     | 9999-12-31        | true
```

**Configuration:** Materialized as `table` (needs fast queries)

#### 2.2. int_rates_usdt

**File:** `models/finance/intermediate/int_rates_usdt.sql`

**Purpose:** Normalize rates, extract currency from symbol

**Logic:**

1. **Parse symbol:**
   ```sql
   currency = REGEXP_EXTRACT(symbol, r'^(.+)USDT$')
   -- BTCUSDT → BTC
   -- ETHUSDT → ETH
   ```

2. **Normalize:**
   - Only take pairs ending with USDT
   - Use `close_price` as `usdt_price`
   - Create `price_date` from `price_timestamp`

**Configuration:** 
- Materialized as `table`
- Partitioned by `price_date`
- Clustered by `symbol`

#### 2.3. int_transactions_with_rates

**File:** `models/finance/intermediate/int_transactions_with_rates.sql`

**Purpose:** Join rates to transactions and calculate USD values

**Logic:**

1. **Find nearest rate for source_currency:**
   ```sql
   -- Get rate where price_timestamp <= transaction.created_at
   -- Sort by price_timestamp DESC, take first record
   ```

2. **Find nearest rate for destination_currency:**
   ```sql
   -- Similar to source_currency
   ```

3. **Calculate USD values:**
   ```sql
   source_amount_usd = source_amount * source_usdt_price
   destination_amount_usd = destination_amount * destination_usdt_price
   ```

**Configuration:**
- Materialized as `table`
- Partitioned by `date_created`

### 3. GOLD LAYER (Marts)

**Purpose:** Create star schema ready for BI

#### 3.1. Dimensions

##### dim_users

**File:** `models/finance/marts/dim_users.sql`

**Purpose:** Dimension table for users with SCD Type 2

```sql
-- Copy from int_users_scd2
-- Sort by user_id, effective_from
```

**Structure:**
- `user_kyc_key`: Surrogate key
- `user_id`: Natural key
- `kyc_level`: KYC level in this period
- `effective_from`: Period start
- `effective_to`: Period end
- `is_current`: Whether this is the current record

##### dim_currencies

**File:** `models/finance/marts/dim_currencies.sql`

**Purpose:** Dimension table for currencies

```sql
-- Get distinct currencies from transactions
-- Add currency_name (manual mapping)
```

##### dim_date

**File:** `models/finance/marts/dim_date.sql`

**Purpose:** Date dimension for time-based analysis

```sql
-- Generate date series from min(created_at) to max(created_at)
-- Extract: year, quarter, month, day, day_of_week, etc.
```

#### 3.2. Facts

##### fct_transactions

**File:** `models/finance/marts/fct_transactions.sql`

**Purpose:** Main fact table with USD values and historical KYC

**Important Logic - Join KYC historical:**

```sql
LEFT JOIN users_scd2 u
    ON t.user_id = u.user_id
    AND t.created_at >= u.effective_from
    AND t.created_at < u.effective_to
```

**Explanation:**
- Join with `int_users_scd2` to get KYC level at transaction time
- Condition: `transaction.created_at` is within range `[effective_from, effective_to)`
- Result: `kyc_level_at_transaction` = KYC level at transaction time, not current KYC level

**Structure:**
- `txn_id`: Primary key
- `user_id`: Foreign key → dim_users
- `kyc_level_at_transaction`: KYC level at transaction time (historical)
- `transaction_date`: Date dimension
- `source_amount_usd`, `destination_amount_usd`: USD values
- Other fields from transactions

**Configuration:**
- Materialized as `table`
- Partitioned by `transaction_date`

#### 3.3. Aggregations

##### tb_transactions_by_kyc

**File:** `models/finance/marts/tb_transactions_by_kyc.sql`

**Purpose:** Aggregation by KYC level

```sql
SELECT
    kyc_level_at_transaction,
    COUNT(DISTINCT txn_id) as nb_transaction,
    COUNT(DISTINCT CASE WHEN status = 'completed' THEN txn_id END) as nb_completed_transaction,
    COUNT(DISTINCT user_id) as nb_users,
    SUM(destination_amount_usd) as total_volume_usd,
    SUM(CASE WHEN status = 'completed' THEN destination_amount_usd END) as completed_volume_usd
FROM fct_transactions
GROUP BY kyc_level_at_transaction
```

## Addressing Business Requirements

### Requirement 1: Total transaction volume (USD) by day/month/quarter

**Query:**

```sql
{{
    config(
        materialized='table'
    )
}}

select
    transaction_date timeframe,
    "daily" timeframe_type,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd else 0 end) as completed_volume_usd,
from {{ ref('fct_transactions') }}
group by ALL 

UNION ALL 

select
    DATE_TRUNC(transaction_date, WEEK(MONDAY)) timeframe,
    "weekly" timeframe_type,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd else 0 end) as completed_volume_usd,
from {{ ref('fct_transactions') }}
group by ALL 

UNION ALL 

select
    DATE_TRUNC(transaction_date, month) timeframe,
    "monthly" timeframe_type,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd else 0 end) as completed_volume_usd,
from {{ ref('fct_transactions') }}
group by ALL 

UNION ALL 

select
    DATE_TRUNC(transaction_date, QUARTER) timeframe,
    "quarterly" timeframe_type,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd else 0 end) as completed_volume_usd,
from {{ ref('fct_transactions') }}
group by ALL 
```

### Requirement 2: Completed transactions by KYC level

**Query:**

```sql 
{{
    config(
        materialized='table',
    )
}}
      

select
    kyc_level_at_transaction,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd END) as completed_volume_usd
from {{ ref ("fct_transactions")}}
where kyc_level_at_transaction is not null
group by all

```


### 4. DBT documentation and Lineage

```bash
dbt docs generate
```
<img width="1419" height="784" alt="image" src="https://github.com/user-attachments/assets/6bfaf33c-e885-4073-aa81-0590fda23315" />

```bash
# Serve docs 
dbt docs serve
```
<img width="1407" height="770" alt="image" src="https://github.com/user-attachments/assets/b69b9f56-10cc-44fe-b037-39e7eaaa3059" />


### 5. Run with Airflow


## Run DAG: data_ingestion

```bash
# Task 1: Fetch rates from Binance
python scripts/fetch_kline.py

# Task 2: Load into BigQuery
python scripts/load_to_bigquery.py
```

## DAG `dbt_finance` automatically runs:

1. `dbt_deps`: Install dependencies
2. `dbt_run_finance_staging`: Run staging models
3. `dbt_run_finance_intermediate`: Run intermediate models
4. `dbt_run_finance_marts`: Run marts models
5. `dbt_test_finance_all`: Run all tests
6. `dbt_docs_generate`: Generate documentation


<img width="1436" height="491" alt="image" src="https://github.com/user-attachments/assets/efc2b1ad-2bb8-45cc-951a-9f35bf7bd8ec" />

<img width="1426" height="563" alt="image" src="https://github.com/user-attachments/assets/39548df8-ad15-4156-b3d7-6634ef5a247f" />

<img width="1428" height="556" alt="image" src="https://github.com/user-attachments/assets/92ebbfd0-9a2e-442b-8d2b-bf9c673d35f2" />




   
