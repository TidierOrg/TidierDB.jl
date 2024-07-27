# TidierDB.jl updates

## v0.3. - 2024-07-28
- adds support for reading from multiple files at once as a vector of paths in `db_table` when using DuckDB
- adds streaming support when using DuckDB when `@collect(stream = true)`

## v0.3. - 2024-07-25
- Introduces package extensions for:
    - Postgres, ClickHouse, MySQL, MsSQL, SQLite, Oracle, Athena, and Google BigQuery
    - (Documentation)[https://tidierorg.github.io/TidierDB.jl/latest/examples/generated/UserGuide/getting_started/] updated for using these backends.  
- Change `set_sql_mode()` to use types not symbols (ie `set_sql_mode(snowflake())` not `set_sql_mode(:snowflake)`)

## v0.2.4 - 2024-07-12
- Switches to DuckDB to 1.0 version
- Adds support for `iceberg` tables via DuckDB to read iceberg paths in `db_table` when `iceberg = true` 
- Adds support for DuckDB's beta `delta_scan` to read delta paths in `db_table` when `delta = true` 
- Adds `connect()` support for DuckDB MotherDuck 

## v0.2.3 - 2024-07-07
- Adds direct path support for `db_table` when using DuckDB
- Adds `connect` ability for AWS and Google Cloud to allow querying via S3 + DuckDB 
- Adds documentation for S3 + DuckDB with TidierDB

## v0.2.2 - 2024-06-27
- Adds support for Databricks SQL Rest API
- Adds docs for Databricks use
- Fixes float/int type conversion when Snowflake collects to dataframe

## v0.2.1 - 2024-06-21
- `@collect` bug fix

## v0.2.0 - 2024-06-21
- Fixes case sensitivity with TidierDB metadata to make queries case insensitive when using Snowflake

## v0.1.9 - 2024-06-20
- Small fix to internal `finalize_query` function for Snowflake

## v0.1.8 - 2024-06-20
- Adds support for Snowflake SQL Rest API using OAuth token connection
- Adds Snowflake support for `connect()`
- Adds docs for Snowflake use

## v0.1.7 - 2024-06-17
- Adds support for Oracle backend via ODBC.jl connection

## v0.1.6 - 2024-06-11
- Adds `@interpolate` and documentation around building macros with TidierDB chains and interpolation

## v0.1.5 - 2024-06-05
- Adjusts Athena backend join syntax to match all other backends

## v0.1.4 - 2024-05-14
- Adds Google Big Query support
- use `connect` with GBQ JSON credentials and project id establish connection

## v0.1.3 - 2024-05-09
- Adds `@full_join`, `@semi_join`, `@anti_join`
- Fixes bug to allow joining tables for Athena backend
- Refines all join syntaxes to remove need for symbols
- Adds `from_query` to allow building from saved query multiple times
- Adds `connect()` - a universal connection function for all supported backends (except Athena)
- Interpolation bug fix to allow interpolating vector of strings with the syntax: `@filter(column_name in [!!vector]) ` 

## v0.1.2 - 2024-05-07
- Adds AWS Athena backend support via AWS.jl

## v0.1.1 - 2024-04-12
- Fixes metadata retrieval for MariaDB
- allows for Table.Name style naming in `@select`
