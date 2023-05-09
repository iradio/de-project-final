# Debug tools and commands

## Airflow
Attach bash do Docker container and run it
Cleanup DAGs runs
```bash
airflow tasks clear fast_stg
```
Run DAG for dates range
```bash
airflow dags backfill -s 2022-10-1 -e 2022-11-1 stg_currencies_and_transactions
airflow dags backfill -s 2022-10-1 -e 2022-11-1 cdm_global_metrics
```
