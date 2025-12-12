
## Main Goals

1. Handle a first-pass parsing of each raw data CSV in the `models/staging/` folder. Copy the pattern from the 'stg_patients.sql' file.

2. All models currently in the marts folder should be fleshed out to meet the criteria in the model's description.

3. Create at least 5 tests to identify data quality issues in the pipeline.

## Secondary Goals

4. Parse the lab result JSON files into a table and expose it for a BI Analytics team.

5. Set up the entire DAG (full models+tests orchestration) to run via dbt in a single command (eg `dbt run`)

## Stretch Goals

6. Set up incremental updating strategies for downstream models which incorporate more than one data source

7. Create additional analytics tables that capture interesting patterns or data
