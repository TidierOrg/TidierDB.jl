# TidierDB.jl updates
## v0.4.2 - 2024-10-
- add support for performing greater than 2 joins using TidierDB queries in a single chain and additional tests
- add `dmy`, `mdy`, `ymd` support DuckDB, Postgres, GBQ, Clickhouse, MySQL, MsSQL, Athena, MsSQL
- add date related tests
- adds `copy_to` for MsSQL to write dataframe to database
- improve Google Big Query type mapping when collecting to df
- change `gbq()`'s `connect()` to accept `location` as second argument
- `str_detect` now supports regex for all backends except MsSQL with tests 

## v0.4.1 - 2024-10-02
- Adds 50 tests comparing TidierDB to TidierData to assure accuracy across a complex chains of operations, including combinations of `@mutate`, `@summarize`, `@filter`, `@select`, `@group_by` and `@join` operations. 

## v0.4.0 - 2024-10-01
- adds `@create_view`
- adds `drop_view`
- adds support for joining a queried table with anothe queried table 
- adds joining [docs](https://tidierorg.github.io/TidierDB.jl/latest/examples/generated/UserGuide/ex_joining/) to outline using `t()` or `@create_view` for post wrangling joins 
- bug fix to allow cross database/schema joins with duckdb

## v0.3.5 - 2024-09-28
- improves DuckDB `connect()` interface and documentation 
- enhances `@window_frame` to allow for just a `to` or `from` argument, as well as autodetection for `preceding`, `following` and `unbounded` for the frame boundaries. 

## v0.3.4 - 2024 2024-09-23
TidierDB works with nearly any exisiting SQL function, now there are docs about it.
- Docs on using any exisiting SQL function in TidierDB
- Docs on user defined functions (UDFs) in TidierDB
- Adds `agg()` to use any aggregate built into a database to be used in `@mutate`. support for `agg()` in across. (`@summarize` continues to all aggregate SQL functions without `agg()`)
- Adds `t(query)` as a more efficient alternative to reference tables.
```
table = db_table(db, "name")
@chain t(table) ... 
```
- Bugfix: fixes MsSQL joins 
- Bugfix: window functions
- Bugfix: json paths supported for `json` DuckDB functions

## v0.3.3 - 2024-08-29
- Bugfix: `@mutate` allows type conversion as part of larger mutate expressions

## v0.3.2 - 2024-08-26
- adds `@head` for limiting number of collected rows
- adds support for reading URLS in `db_table` with ClickHouse 
- adds support for reading from multiple files at once as a vector of urls in `db_table` when using ClickHouse
    - ie `db_table(db, ["url1", "url2"])`
- Bugfix: `@count` updates metadata
- Bugfix: `update_con` can be part of chain (useful for expiring Snowflake tokens) 
- Bugfix to allow CrateDB and RisingWave backends via LibPQ
- adds `connect()` support for Microsoft SQL Server 
- adds `show_tables` for most backends to view exisiting tables
- Docs comparing TidierDB to Ibis 
- Docs around using `*` for reading in multiple files from folder
- Docs for `db_table`
- Docs for previewing or saving intermediate tables in ongoing `@chain`

## v0.3.1 - 2024-07-28
- adds support for reading from multiple files at once as a vector of paths in `db_table` when using DuckDB
    - ie `db_table(db, ["path1", "path2"])`
- adds streaming support when using DuckDB with `@collect(stream = true)`
- allows user to customize file reading via `db_table(db, "read_*(path, args)")` when using DuckDB

## v0.3.0 - 2024-07-25
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
