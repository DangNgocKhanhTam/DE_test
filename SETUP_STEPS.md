# Hướng dẫn Setup từng bước

## Bước 1: Kiểm tra Service Account ✅

Đảm bảo file `config/service_account.json` tồn tại và có quyền:
- BigQuery Data Editor
- BigQuery Job User
- BigQuery User

**Kiểm tra:**
```bash
ls -la config/service_account.json
```

## Bước 2: Test dbt Connection

Test xem dbt có kết nối được với BigQuery không:

```bash
cd /Users/tamdang/Documents/DE_Test/DE_test

# Test connection
dbt debug
```

**Kết quả mong đợi:**
- ✅ Connection OK
- ✅ Profile OK
- ✅ Configuration OK

**Nếu lỗi:**
- Kiểm tra `profiles.yml` có đúng path không
- Kiểm tra service account có permissions không
- Kiểm tra project_id và dataset_id có đúng không

## Bước 3: Tạo BigQuery Datasets

Tạo các datasets cần thiết trong BigQuery:

### Option 1: Dùng BigQuery Console
1. Vào https://console.cloud.google.com/bigquery
2. Tạo dataset `raw_data` trong project `cusma-383203`
3. Tạo dataset `dwh_dev` trong project `cusma-383203`

### Option 2: Dùng dbt (Tự động tạo khi run)

dbt sẽ tự động tạo datasets khi chạy lần đầu, nhưng bạn có thể tạo trước:

```bash
# Tạo raw_data dataset (nếu chưa có)
bq mk --dataset --location=asia-southeast1 cusma-383203:raw_data

# Tạo dwh_dev dataset (nếu chưa có)
bq mk --dataset --location=asia-southeast1 cusma-383203:dwh_dev
```

## Bước 4: Load Raw Data vào BigQuery

Load dữ liệu thô vào BigQuery dataset `raw_data`:

```bash
# Chạy script load data
python scripts/load_to_bigquery.py
```

**Kiểm tra:**
- Vào BigQuery Console
- Kiểm tra dataset `raw_data` có 3 tables:
  - `users`
  - `transaction`
  - `rates` (nếu đã fetch)

## Bước 5: Test dbt Models (Local)

Test dbt models trước khi chạy trong Airflow:

```bash
# 1. Install dbt dependencies (nếu có)
dbt deps

# 2. Test connection
dbt debug

# 3. Run staging models (Bronze layer)
dbt run --select staging

# 4. Test staging models
dbt test --select staging

# 5. Run intermediate models (Silver layer)
dbt run --select int

# 6. Run marts models (Gold layer)
dbt run --select marts

# 7. Test all models
dbt test

# 8. Generate docs
dbt docs generate
dbt docs serve
```

**Kiểm tra trong BigQuery:**
- Dataset `dwh_dev.staging` có các views
- Dataset `dwh_dev.intermediate` có các tables
- Dataset `dwh_dev.analytics` có các tables

## Bước 6: Setup Airflow với Docker

### 6.1. Tạo .env file

```bash
# Tạo .env file
cat > .env << EOF
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
EOF
```

### 6.2. Tạo các thư mục cần thiết

```bash
mkdir -p logs plugins output/raw_rates
```

### 6.3. Initialize Airflow

```bash
# Initialize Airflow database
docker-compose up airflow-init
```

**Kết quả mong đợi:**
- ✅ Database initialized
- ✅ Admin user created

### 6.4. Start Airflow

```bash
# Start Airflow services
docker-compose up -d

# Check logs
docker-compose logs -f airflow-scheduler
```

### 6.5. Access Airflow UI

- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

## Bước 7: Kiểm tra DAGs trong Airflow

1. Mở Airflow UI: http://localhost:8080
2. Login với `airflow` / `airflow`
3. Kiểm tra có 3 DAGs:
   - `api_fetch_rates`
   - `data_collection`
   - `dbt_modeling`
4. Enable các DAGs (toggle switch)

## Bước 8: Test DAGs

### 8.1. Test API Fetch DAG

1. Trigger DAG `api_fetch_rates` manually
2. Kiểm tra task `fetch_rates_from_binance` chạy thành công
3. Kiểm tra files trong `output/raw_rates/`

### 8.2. Test Data Collection DAG

1. Trigger DAG `data_collection` manually
2. Kiểm tra task `load_raw_data_to_bigquery` chạy thành công
3. Kiểm tra BigQuery dataset `raw_data` có data

### 8.3. Test dbt Modeling DAG

1. Trigger DAG `dbt_modeling` manually
2. Kiểm tra các tasks chạy theo thứ tự:
   - `wait_for_data_collection`
   - `dbt_deps`
   - `dbt_run_staging`
   - `dbt_test_staging`
   - `validate_staging_ge`
   - `dbt_run_intermediate`
   - `dbt_run_marts`
   - `dbt_test_all`
   - `validate_marts_ge`
   - `dbt_docs_generate`

## Bước 9: Kiểm tra Kết quả

### 9.1. Kiểm tra trong BigQuery

```sql
-- Kiểm tra staging layer
SELECT * FROM `cusma-383203.dwh_dev.staging.stg_users` LIMIT 10;
SELECT * FROM `cusma-383203.dwh_dev.staging.stg_transactions` LIMIT 10;

-- Kiểm tra intermediate layer
SELECT * FROM `cusma-383203.dwh_dev.intermediate.int_users_scd2` LIMIT 10;

-- Kiểm tra marts layer
SELECT * FROM `cusma-383203.dwh_dev.analytics.fct_transactions` LIMIT 10;
SELECT * FROM `cusma-383203.dwh_dev.analytics.agg_transactions_daily` LIMIT 10;
```

### 9.2. Test Business Requirements

**Requirement 1: Tổng khối lượng giao dịch USD theo ngày/tháng/quý**
```sql
SELECT 
    transaction_year,
    transaction_quarter,
    transaction_month,
    SUM(transaction_amount_usd) as total_volume_usd
FROM `cusma-383203.dwh_dev.analytics.fct_transactions`
GROUP BY transaction_year, transaction_quarter, transaction_month
ORDER BY transaction_year, transaction_quarter, transaction_month;
```

**Requirement 2: Giao dịch completed theo KYC level**
```sql
SELECT 
    kyc_level_at_transaction,
    COUNT(*) as transaction_count,
    SUM(transaction_amount_usd) as total_volume_usd
FROM `cusma-383203.dwh_dev.analytics.fct_transactions`
WHERE status = 'completed'
GROUP BY kyc_level_at_transaction
ORDER BY kyc_level_at_transaction;
```

**Requirement 3: KYC level tại thời điểm transaction (Historical)**
```sql
SELECT 
    txn_id,
    user_id,
    transaction_timestamp,
    kyc_level_at_transaction,
    status,
    transaction_amount_usd
FROM `cusma-383203.dwh_dev.analytics.fct_transactions`
WHERE user_id = 160249  -- Example user
ORDER BY transaction_timestamp;
```

## Bước 10: Troubleshooting

### Lỗi dbt connection

```bash
# Test connection
dbt debug

# Kiểm tra profiles.yml
cat profiles.yml

# Kiểm tra service account
ls -la config/service_account.json
```

### Lỗi Airflow DAGs

```bash
# Check logs
docker-compose logs -f airflow-scheduler

# Restart services
docker-compose restart

# Check DAG syntax
python -m py_compile dags/*.py
```

### Lỗi Great Expectations

```bash
# Kiểm tra GE context
ls -la great_expectations/

# Test GE validator
python -c "
from dags.utils.ge_validator import GEValidator
validator = GEValidator()
print('GE Validator initialized')
"
```

## Checklist

- [ ] Service account JSON file tồn tại
- [ ] dbt debug thành công
- [ ] BigQuery datasets đã tạo
- [ ] Raw data đã load vào BigQuery
- [ ] dbt models chạy thành công local
- [ ] Airflow đã start và accessible
- [ ] DAGs hiển thị trong Airflow UI
- [ ] DAGs chạy thành công
- [ ] Data đã được transform qua các layers
- [ ] Business requirements queries chạy đúng

## Next Steps

1. **Schedule DAGs**: Set schedule cho các DAGs (đã set @daily)
2. **Monitor**: Setup monitoring và alerting
3. **Optimize**: Tối ưu performance nếu cần
4. **Document**: Update documentation khi có thay đổi

