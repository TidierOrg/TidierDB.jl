# TidierDB.jl updates

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
