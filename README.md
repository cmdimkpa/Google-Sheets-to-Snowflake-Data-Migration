# Google-Sheets-to-Snowflake-Migration

Featured in: https://hevodata.com/blog/google-sheets-to-snowflake/

## Updates - May 2021

The software was recently updated to:

- fix a bug where the old authentication methods are no longer working
- improve the CLI argument interface
- add bulk writes (write several records at once) to make it faster and more practical

## Usage

```
usage: migrate.py [-h] [--rate-limit-delay RATE_LIMIT_DELAY]
                  [--max-concurrent-write MAX_CONCURRENT_WRITE]
                  --target-sheet-name TARGET_SHEET_NAME --columns-to-read
                  COLUMNS_TO_READ --max-rows-to-copy MAX_ROWS_TO_COPY
                  --target-warehouse TARGET_WAREHOUSE --target-database
                  TARGET_DATABASE --target-table TARGET_TABLE --target-schema
                  TARGET_SCHEMA --target-field-names TARGET_FIELD_NAMES
                  --target-role TARGET_ROLE
migrate.py: the following arguments are required: --target-sheet-name, --columns-to-read, --max-rows-to-copy,
--target-warehouse, --target-database, --target-table, --target-schema, --target-field-names, --target-role
```

## Example

```
python3 migrate.py --target-sheet-name data-uplink-logs --columns-to-read A,B,C,D --max-rows-to-copy 26 
--target-warehouse COMPUTE_WH --target-database TEST_GSHEET_MIGRATION --target-table GSHEETS_MIGRATION 
--target-schema PUBLIC --target-field-names CLIENT_ID,NETWORK_TYPE,BYTES,UNIX_TIMESTAMP --target-role SYSADMIN
```

## Screenshots

<img width="1188" alt="Screenshot 2021-05-28 at 06 04 26" src="https://user-images.githubusercontent.com/26833356/119932146-a62fd500-bf7a-11eb-84d4-8f045079fb7a.png">

<img width="1116" alt="Screenshot 2021-05-28 at 06 06 08" src="https://user-images.githubusercontent.com/26833356/119932285-df684500-bf7a-11eb-9d97-a84b04872bbc.png">
