# TidierDB.jl updates
## v0.8.0 - 2025-02-12
- adds `@transmute`
- adds `@unnest_wider` and `@unnest_longer`
- bug fixes around dates 
- fixes duckdb compat issue for windows users 
    - keeps active `duck12` branch active for users who want to use DuckDB 1.2
- bumps DataFrames.jl compat to 1.5

## v0.7.1 - 2025-02-04
- Prints queries in color by default (optional)
- fixes bug when using `agg()` with window ordering and framing
- include default support for all of the following window functions
    - `lead`, `lag`, `dense_rank`, `nth_value`, `ntile`, `rank_dense`, `row_number`, `first_value`, `last_value`, `cume_dist`
- add ability to change what functions are on this list to avoid the use of agg in the following manner 
    - `push!(TidierDB.window_agg_fxns, :kurtosis);`
- fixes edge case query construction issues with `@mutate`,`@filter`,`@*_join`
- fix edge case with `@select` call after group_by -> summarize
- add Google Sheet support 
```
ghseet_connect(db)
DB.db_table(db, "https://docs.google.com/spreadsheets/d/rest/of/link")
```

## v0.7.0 - 2025-01-26
- `db_table` now supports viewing a dataframe directly - `db_table(db, df, "name4db")`
- `copy_to` will copy a table to the DuckDB db, instead of creating a view

## v0.6.3 - 2025-01-20
- Resolve issue when filtering immediately after joining

## v0.6.2 - 2025-01-09
- adds `@intersect` and `@setdiff` (SQLs `INTERSECT` and `EXCEPT`) respectively, with optional `all` argument
- adds support for `all` arg to `@union` (equivalent to `@union_all`)

## v0.6.1 - 2025-01-07
- Bumps julia LTS to 1.10

## v0.6.0 - 2025-01-07
- Adds support for joining on multiple columns
- Adds support for inequality joins
- Adds support for AsOf / rolling joins
- Equi-joins no longer duplicate key columns
- Fixes bug to allow array columns to be mutated in 

## v0.5.3 - 2024-12-13
- adds `@relocate`
- bug fix when reading file paths with `*` wildcard with DuckDB
- Fix edge case when creating an `array` column in `@mutate` 

## v0.5.2 - 2024-12-03
- adds support `_by` support to `@mutate` and `@summarize` for grouping within the macro call.
- adds support for `n()` in `@mutate`
- add support for unnesting content to mutate/filter etc via `column[key]`syntax
- `db_table(db, name)` now supports `.geoparquet` paths for DuckDB

## v0.5.1 - 2024-11-08
- support for [reusing TidierDB queries](https://tidierorg.github.io/TidierDB.jl/latest/examples/generated/UserGuide/functions_pass_to_DB/#interpolating-queries) inside other macros, including `@mutate`, `@filter`, `@summarize`
- adds `@union_all` to bind all rows not just distinct rows as with `@union`
- joining syntax now supports `(table1, table2, col_name)` when joining columns have shared name
- `if_else` now has optional final argument for handling missing values to match TidierData

# TidierDB.jl updates
## v0.5.0 - 2024-10-15
Breaking Changes: 
- All join syntax now matches TidierData's `(table1, table2, t1_col = t2_col)`
Additions:
- `@compute`for DuckDB, MySQL, PostGres, GBQ to write a table to the db at the end of a query.
- expands `@create_view` to MySQL, PostGres, GBQ 
- Support for performing multiple joins of TidierDB queries in a single chain with further tests
-  `dmy`, `mdy`, `ymd` support DuckDB, Postgres, GBQ, Clickhouse, MySQL, MsSQL, Athena, MsSQL
- Date related tests
- `copy_to` for MysQL to write a dataframe to MySQL database
Improvements:
- improve Google Big Query type mapping when collecting to dataframe
- change `gbq()`'s `connect()` to accept `location` as second argument
- `str_detect` now supports regex for all backends except MsSQL + some tests
- `@select(!table.name)` now works to deselect a column

Docs:
- Add duckplyr/duckdb reproducible example to docs
- Improve interpolation docs

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
