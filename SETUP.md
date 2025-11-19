# Setup Guide

## Quick Start

### 1. Prerequisites

- Docker và Docker Compose
- Google Cloud Project với BigQuery enabled
- Service Account với BigQuery permissions
- Python 3.8+ (nếu chạy local)

### 2. Setup Service Account

1. Tạo service account trong GCP
2. Grant permissions:
   - BigQuery Data Editor
   - BigQuery Job User
   - BigQuery User
3. Download JSON key và save vào `config/service_account.json`

### 3. Setup BigQuery Datasets

Tạo các datasets trong BigQuery:
```sql
CREATE SCHEMA IF NOT EXISTS `cusma-383203.raw_data`
  OPTIONS(location='asia-southeast1');

CREATE SCHEMA IF NOT EXISTS `cusma-383203.dwh_dev`
  OPTIONS(location='asia-southeast1');
```

### 4. Start Airflow với Docker

```bash
cd /Users/tamdang/Documents/DE_Test/DE_test

# Create .env file
echo "AIRFLOW_UID=50000" > .env

# Create required directories
mkdir -p logs plugins output/raw_rates

# Initialize Airflow
docker-compose up airflow-init

# Start Airflow
docker-compose up -d

# Check logs
docker-compose logs -f airflow-scheduler
```

### 5. Access Airflow UI

- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

### 6. Run DAGs

1. Enable DAGs trong Airflow UI
2. Trigger manually hoặc đợi schedule
3. Monitor execution trong UI

## Manual Testing

### Test dbt locally

```bash
# Install dbt
pip install dbt-bigquery

# Test connection
dbt debug

# Run models
dbt run --select staging
dbt run --select int
dbt run --select marts

# Run tests
dbt test
```

### Test Great Expectations

```bash
# Install GE
pip install great-expectations

# Initialize (if needed)
great_expectations init

# Validate
python -c "
from dags.utils.ge_validator import validate_with_ge
result = validate_with_ge(
    table_name='stg_users',
    expectation_suite_name='stg_users',
    project_id='cusma-383203',
    dataset_id='dwh_dev',
    schema='staging'
)
print(result)
"
```

## Troubleshooting

### Airflow DAGs không hiển thị

- Kiểm tra logs: `docker-compose logs -f airflow-scheduler`
- Kiểm tra DAGs có syntax errors không
- Restart scheduler: `docker-compose restart airflow-scheduler`

### dbt connection errors

- Kiểm tra `profiles.yml` có đúng không
- Kiểm tra service account có permissions không
- Test connection: `dbt debug`

### Great Expectations errors

- Kiểm tra GE context có tồn tại không
- Kiểm tra expectation suites có đúng format không
- Kiểm tra BigQuery connection string

### Permission errors

- Kiểm tra service account JSON file
- Kiểm tra file permissions
- Kiểm tra Docker volume mounts

