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

## Screenshots

