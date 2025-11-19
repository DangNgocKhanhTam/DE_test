# Domain-Based Model Structure

## Cấu trúc mới

Models được tổ chức theo **domain** thay vì theo layer:

```
models/
└── finance/
    ├── staging/          # Finance Bronze layer
    │   ├── stg_users.sql
    │   ├── stg_transactions.sql
    │   ├── stg_rates.sql
    │   └── schema.yml
    ├── intermediate/     # Finance Silver layer
    │   ├── int_users_scd2.sql
    │   ├── int_rates_usdt.sql
    │   └── int_transactions_with_rates.sql
    └── marts/            # Finance Gold layer
        ├── fct_transactions.sql
        ├── dim_users.sql
        ├── dim_currencies.sql
        ├── dim_date.sql
        └── schema.yml
```

## BigQuery Datasets

Mỗi domain có 3 schemas riêng:

- `finance_staging` - Finance staging models
- `finance_intermediate` - Finance intermediate models
- `finance_analytics` - Finance marts models

## Airflow DAGs

### DAG: `dbt_finance`

Schedule theo domain finance:
- `dbt run --select finance.staging` → `finance_staging` schema
- `dbt run --select finance.intermediate` → `finance_intermediate` schema
- `dbt run --select finance.marts` → `finance_analytics` schema

## Thêm domain mới

Khi cần thêm domain (ví dụ: marketing):

1. Tạo cấu trúc:
```bash
mkdir -p models/marketing/{staging,intermediate,marts}
```

2. Thêm vào `dbt_project.yml`:
```yaml
models:
  de_test_dwh:
    marketing:
      staging:
        +materialized: view
        +schema: marketing_staging
      intermediate:
        +materialized: table
        +schema: marketing_intermediate
      marts:
        +materialized: table
        +schema: marketing_analytics
```

3. Tạo DAG mới: `dags/dbt_marketing.py`

## Lợi ích

- ✅ Mỗi domain độc lập
- ✅ Schedule riêng biệt
- ✅ Dễ mở rộng
- ✅ Clear ownership

