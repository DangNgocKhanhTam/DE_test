# Data Warehouse Architecture

## Tổng quan

Data Warehouse được xây dựng trên BigQuery sử dụng dbt (data build tool) để transform dữ liệu theo mô hình **Bronze-Silver-Gold** (Medallion Architecture). Pipeline được orchestrate bằng Apache Airflow với validation bằng **Great Expectations**.

## Kiến trúc Data Model

### Mô hình: Star Schema với SCD Type 2

Chúng tôi chọn **Star Schema** kết hợp với **SCD Type 2** (Slowly Changing Dimension Type 2) cho các lý do sau:

1. **Hiệu suất truy vấn**: Star Schema tối ưu cho BI tools với các truy vấn aggregate nhanh
2. **Dễ hiểu**: Cấu trúc đơn giản, dễ maintain và mở rộng
3. **Lịch sử KYC**: SCD Type 2 cho phép track lịch sử thay đổi kyc_level của users
4. **Tính nhất quán**: Fact table là single source of truth cho tất cả metrics

### Sơ đồ Data Model

```
                    ┌─────────────────┐
                    │  dim_date       │
                    │  (Date Dim)     │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │                 │
         ┌──────────┴──────────┐      │
         │                     │      │
    ┌────┴─────┐      ┌────────┴──────┴─────┐
    │dim_users │      │   fct_transactions  │
    │(SCD Type2)      │   (Fact Table)      │
    └────┬─────┘      └────────┬────────────┘
         │                     │
         │              ┌──────┴──────┐
         │              │             │
    ┌────┴──────────────┴──┐   ┌─────┴──────────┐
    │  dim_currencies      │   │  Aggregated    │
    │  (Currency Dim)      │   │  Facts         │
    └──────────────────────┘   └────────────────┘
```

## Cấu trúc Layers

### 1. Bronze Layer (Raw Data)
**Location**: `models/staging/`

**Mục đích**: 
- Load và clean dữ liệu thô từ BigQuery
- Standardize data types và column names
- Thêm data quality tests

**Models**:
- `stg_users`: Users data với type casting
- `stg_transactions`: Transactions data với type casting
- `stg_rates`: Exchange rates từ Binance API

**Data Tests**:
- Unique constraints trên primary keys
- Not null constraints trên các cột quan trọng
- Referential integrity (foreign keys)
- Accepted values cho status và kyc_level

**Great Expectations**:
- Expectation suites cho mỗi staging model
- Row count checks
- Data type validation
- Value range checks

### 2. Silver Layer (Intermediate)
**Location**: `models/int/`

**Mục đích**:
- Business logic transformations
- SCD Type 2 implementation cho users
- Join rates với transactions để tính USD values

**Models**:
- `int_users_scd2`: Users với SCD Type 2 để track lịch sử kyc_level
  - `valid_from`: Thời điểm bắt đầu hiệu lực của kyc_level
  - `valid_to`: Thời điểm kết thúc hiệu lực
  - `is_current`: Flag cho record hiện tại
  
- `int_rates_usdt`: Transform rates để extract base currency và USDT price
  
- `int_transactions_with_rates`: Join transactions với rates để tính USD values
  - Sử dụng point-in-time lookup để lấy rate gần nhất tại thời điểm transaction

### 3. Gold Layer (Marts)
**Location**: `models/marts/`

**Mục đích**:
- Final models sẵn sàng cho BI team
- Fact và Dimension tables
- Pre-aggregated tables cho performance

**Models**:

#### Fact Tables:
- `fct_transactions`: Main fact table
  - Tất cả transactions với USD values
  - **kyc_level_at_transaction**: KYC level tại thời điểm transaction (không phải current level)
  - Time dimensions (date, month, quarter, year)

#### Dimension Tables:
- `dim_users`: Users dimension với SCD Type 2
- `dim_currencies`: Currency dimension
- `dim_date`: Date dimension cho time-based analysis

#### Aggregated Facts:
- `agg_transactions_daily`: Daily transaction volume in USD
- `agg_transactions_by_kyc`: Completed transactions by KYC level (at transaction time)

## Giải quyết Business Requirements

### Requirement 1: Tổng khối lượng giao dịch (USD) theo ngày/tháng/quý

**Solution**: 
- `fct_transactions` có `transaction_amount_usd` và time dimensions
- `agg_transactions_daily` pre-aggregated theo ngày
- Query example:
```sql
SELECT 
    transaction_year,
    transaction_quarter,
    transaction_month,
    SUM(transaction_amount_usd) as total_volume_usd
FROM analytics.fct_transactions
GROUP BY transaction_year, transaction_quarter, transaction_month
```

### Requirement 2: Giao dịch completed theo KYC level

**Solution**:
- `agg_transactions_by_kyc` pre-aggregated cho completed transactions
- Filter `status = 'completed'` và group by `kyc_level_at_transaction`
- Query example:
```sql
SELECT 
    kyc_level_at_transaction,
    COUNT(*) as transaction_count,
    SUM(transaction_amount_usd) as total_volume_usd
FROM analytics.fct_transactions
WHERE status = 'completed'
GROUP BY kyc_level_at_transaction
```

### Requirement 3: KYC level tại thời điểm transaction (Historical)

**Solution**: 
- SCD Type 2 trong `dim_users` và `int_users_scd2`
- Point-in-time join trong `fct_transactions`:
```sql
LEFT JOIN users_scd2 u
    ON t.user_id = u.user_id
    AND t.created_at >= u.valid_from
    AND t.created_at < u.valid_to
```
- `fct_transactions.kyc_level_at_transaction` chứa KYC level tại thời điểm transaction, không phải current level

## Orchestration với Airflow

### Tách riêng Collection và Modeling

Pipeline được tách thành **3 DAGs độc lập**:

#### 1. DAG: `api_fetch_rates` (API Fetching)

**Schedule**: Daily (`@daily`)

**Mục đích**: Fetch exchange rates từ Binance API

**Tasks**:
1. `fetch_rates_from_binance`: Fetch rates từ Binance API và save vào JSONL files

**Output**: JSONL files trong `output/raw_rates/`

#### 2. DAG: `data_collection` (Data Ingestion)

**Schedule**: Daily (`@daily`)

**Mục đích**: Load raw data từ files vào BigQuery

**Tasks**:
1. `wait_for_api_fetch` (optional): Sensor để đợi API fetch hoàn thành
2. `load_raw_data_to_bigquery`: Load CSV và JSONL files vào BigQuery `raw_data` dataset
3. `validate_raw_data`: Validate data đã được load thành công

**Output**: Raw data trong BigQuery dataset `raw_data`

#### 3. DAG: `dbt_modeling` (Data Transformation)

**Schedule**: Daily (`@daily`)

**Mục đích**: Transform data qua Bronze-Silver-Gold layers

**Tasks**:
1. `wait_for_data_collection`: Sensor để đợi data collection DAG hoàn thành
2. `dbt_deps`: Install dbt dependencies
3. `dbt_run_staging`: Run staging models (Bronze layer)
4. `dbt_test_staging`: Test staging models với dbt tests
5. `validate_staging_ge`: **Validate với Great Expectations**
6. `dbt_run_intermediate`: Run intermediate models (Silver layer)
7. `dbt_run_marts`: Run marts models (Gold layer)
8. `dbt_test_all`: Run all tests
9. `validate_marts_ge`: **Validate với Great Expectations**
10. `dbt_docs_generate`: Generate documentation

**Input**: Raw data từ `data_collection` DAG
**Output**: Transformed data trong BigQuery datasets `staging`, `intermediate`, `analytics`

## Great Expectations Integration

### Setup

Great Expectations được tích hợp vào pipeline để validate data quality:

1. **Expectation Suites**: Định nghĩa trong `great_expectations/expectations/`
   - `stg_users/expectation_suite.json`
   - `stg_transactions/expectation_suite.json`
   - `fct_transactions/expectation_suite.json`

2. **Validation trong Airflow**: 
   - `dags/utils/ge_validator.py`: GE validator class
   - Validation tasks trong DAGs sau khi dbt run

3. **Validation Checks**:
   - Row count validation
   - Null checks
   - Unique constraints
   - Value range checks
   - Data type validation

### Benefits

- **Industry Standard**: Great Expectations là industry standard cho data quality
- **Rich Validation**: Nhiều expectation types
- **Documentation**: Tự động generate data docs
- **Alerting**: Có thể tích hợp alerting khi validation fail

## Setup và Configuration

### 1. dbt Configuration

**File**: `dbt_project.yml`
- Project name: `de_test_dwh`
- Profiles: `profiles.yml`
- Materialization:
  - Staging: `view` (lightweight, always fresh)
  - Intermediate: `table` (performance)
  - Marts: `table` (performance, BI queries)

**File**: `profiles.yml`
- Target: `dev` (development) hoặc `prod` (production)
- BigQuery connection với service account
- Location: `asia-southeast1`

### 2. BigQuery Setup

**Datasets**:
- `raw_data`: Raw data từ CSV/JSONL files
- `dwh_dev.staging`: Bronze layer (development)
- `dwh_dev.intermediate`: Silver layer (development)
- `dwh_dev.analytics`: Gold layer (development)
- `dwh_prod.*`: Production datasets (tương tự)

### 3. Airflow Setup với Docker Compose

**DAG Location**: `dags/`

**Requirements**:
- Apache Airflow >= 2.7.0
- Google Cloud providers
- dbt installed và configured
- Great Expectations installed

**Environment Variables** (`.env`):
- `AIRFLOW_UID`: User ID for Airflow
- `_AIRFLOW_WWW_USER_USERNAME`: Airflow username
- `_AIRFLOW_WWW_USER_PASSWORD`: Airflow password

**Start Airflow**:
```bash
# Initialize
docker-compose up airflow-init

# Start
docker-compose up -d

# Access UI
# http://localhost:8080
# Username: airflow
# Password: airflow
```

## Data Quality

### Tests Implemented

1. **dbt Tests**:
   - Unique constraints trên primary keys
   - Not null constraints
   - Referential integrity
   - Accepted values validation

2. **Great Expectations**:
   - Row count validation
   - Data type checks
   - Value range validation
   - Custom business rules

### Monitoring

- Airflow DAG runs và failures
- dbt test results
- Great Expectations validation results
- BigQuery query logs
- Data freshness metrics

## Performance Considerations

1. **Materialization Strategy**:
   - Staging: Views (lightweight, always fresh)
   - Intermediate/Silver: Tables (performance cho complex joins)
   - Marts/Gold: Tables (optimized cho BI queries)

2. **Indexing/Sorting**:
   - Tables sorted by frequently queried columns
   - Clustering trên time columns (BigQuery automatic)

3. **Aggregations**:
   - Pre-aggregated tables (`agg_*`) cho common queries
   - Reduces query time và cost

## Tài liệu tham khảo

- [dbt Documentation](https://docs.getdbt.com/)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Star Schema Design](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/star-schema/)

