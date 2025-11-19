# Data Warehouse Project với dbt, BigQuery và Great Expectations

## Tổng quan

Dự án xây dựng Data Warehouse trên BigQuery sử dụng dbt để transform dữ liệu theo mô hình Bronze-Silver-Gold. Pipeline được orchestrate bằng Apache Airflow với validation bằng Great Expectations.

## Cấu trúc Project

```
DE_test/
├── dags/                      # Airflow DAGs
│   ├── api_fetch_rates.py     # DAG cho API fetching
│   ├── data_collection.py     # DAG cho data ingestion
│   ├── dbt_modeling.py        # DAG cho dbt transformations
│   └── utils/                 # Utility functions
│       ├── ge_validator.py    # Great Expectations validator
│       └── __init__.py
├── models/                    # dbt models
│   ├── staging/               # Bronze layer
│   ├── int/                   # Silver layer
│   └── marts/                 # Gold layer
├── great_expectations/        # Great Expectations config
│   ├── great_expectations.yml
│   └── expectations/          # Expectation suites
├── scripts/                   # Python scripts
│   ├── load_to_bigquery.py   # Load raw data
│   └── fetch_kline.py        # Fetch exchange rates
├── config/                    # Configuration files
│   └── service_account.json  # GCP service account
├── data/                      # Raw data files
│   ├── users.csv
│   └── transactions.csv
├── dbt_project.yml           # dbt project config
├── profiles.yml              # dbt profiles config
├── docker-compose.yml        # Airflow Docker Compose
└── ARCHITECTURE.md           # Architecture documentation
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure dbt

dbt profiles được cấu hình trong `profiles.yml`. Đảm bảo:
- Service account JSON file có quyền truy cập BigQuery
- Project ID và dataset names đúng

### 3. Setup BigQuery

Tạo các datasets trong BigQuery:
- `raw_data`: Raw data từ files
- `dwh_dev.staging`: Bronze layer (development)
- `dwh_dev.intermediate`: Silver layer (development)
- `dwh_dev.analytics`: Gold layer (development)

### 4. Setup Airflow với Docker Compose

```bash
# 1. Tạo .env file (đã có sẵn)
# AIRFLOW_UID=$(id -u)  # Linux/Mac

# 2. Tạo folders
mkdir -p logs plugins

# 3. Initialize Airflow
docker-compose up airflow-init

# 4. Start Airflow
docker-compose up -d

# 5. Truy cập UI
# http://localhost:8080
# Username: airflow
# Password: airflow
```

### 5. Setup Great Expectations (Optional)

Great Expectations đã được cấu hình sẵn. Nếu cần tạo mới:

```bash
great_expectations init
```

## Chạy Pipeline

### Manual với dbt

```bash
# Run staging models
dbt run --select staging

# Run intermediate models
dbt run --select int

# Run marts models
dbt run --select marts

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

### Với Airflow

1. **API Fetch DAG** (`api_fetch_rates`):
   - Fetch rates từ Binance API
   - Save vào JSONL files

2. **Data Collection DAG** (`data_collection`):
   - Load raw data từ CSV/JSONL vào BigQuery
   - Validate data loaded successfully

3. **dbt Modeling DAG** (`dbt_modeling`):
   - Đợi data collection hoàn thành
   - Run dbt transformations qua các layers
   - **Validate với Great Expectations**
   - Run tests và generate docs

## Business Questions

DWH được thiết kế để trả lời 3 câu hỏi nghiệp vụ:

1. **Tổng khối lượng giao dịch (USD) theo ngày/tháng/quý**
   - Query từ `analytics.fct_transactions` hoặc `analytics.agg_transactions_daily`

2. **Giao dịch completed theo KYC level**
   - Query từ `analytics.agg_transactions_by_kyc`

3. **KYC level tại thời điểm transaction (Historical)**
   - `fct_transactions.kyc_level_at_transaction` chứa KYC level tại thời điểm transaction
   - Sử dụng SCD Type 2 để track lịch sử

## Data Quality với Great Expectations

Great Expectations được tích hợp để validate data quality:

- **Expectation Suites**: Định nghĩa trong `great_expectations/expectations/`
- **Validation Tasks**: Chạy sau dbt run trong Airflow DAGs
- **Validation Checks**: Row count, nulls, uniqueness, value ranges

## Documentation

Xem `ARCHITECTURE.md` để biết chi tiết về:
- Data model design
- Bronze-Silver-Gold layers
- SCD Type 2 implementation
- Orchestration strategy
- Great Expectations integration

## Notes

- Staging models được materialized as `view` (lightweight)
- Intermediate và Marts models được materialized as `table` (performance)
- SCD Type 2 cho phép point-in-time lookups của KYC levels
- Great Expectations validation chạy sau mỗi dbt run

