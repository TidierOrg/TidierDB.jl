# TidierDB.jl updates

## v0.1.3 - 2024-05-TBD
- Adds `@full_join`, `@semi_join`, `@anti_join`
- Adds `connect()` - a universal connection funciton for all supported backends
- Adds `sql_agg()` - allows any aggregate SQL function not availabe in backend parsers to be used in `@mutate`. Simply write the function as written in SQL syntax as a string wrapped in `sql_agg`, and subsequent windowing is handled by `@mutate`.

## v0.1.2 - 2024-05-07
- Adds AWS Athena backend support

## v0.1.1 - 2024-04-12
- Fixes metadata retrieval for MariaDB
- allows for Table.Name style naming in `@select`