# TidierDB.jl updates

## v0.1.3 - 2024-05-TBD
- Adds `@full_join`, `@semi_join`, `@anti_join`
- Refines all join syntax to remove need for symbols.
- adds `from_query`to allow building from saved query multiple times
- Adds `connect()` - a universal connection funciton for all supported backends

## v0.1.2 - 2024-05-07
- Adds AWS Athena backend support

## v0.1.1 - 2024-04-12
- Fixes metadata retrieval for MariaDB
- allows for Table.Name style naming in `@select`