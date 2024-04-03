## What is TidierDB.jl

TiderDB.jl is a 100% Julia implementation of the dbplyr R package (and similar to python's ibis package).

The main goal of TidierDB.jl is to bring the ease of use and simple syntax of Tidier.jl to mutliple SQL backends,
making data analysis smoother by abstracting away subtle syntax differences between backends.

## Currently supported backends include:
- DuckDB (the default) `set_sql_mode(:duckdb)`
- ClickHouse `set_sql_mode(:clickhouse)`
- SQLite `set_sql_mode(:lite)`
- MySQL `set_sql_mode(:mysql)`
- MSSQL `set_sql_mode(:mssql)`
- Postgres `set_sql_mode(:postgres)`

Change the backend by using `set_sql_mode()`

## Installation

For the stable version:

```
] add TidierDB
```

TidierDB.jl currently supports the following top-level macros:

- `@arrange`
- `@group_by` 
- `@filter`
- `@select`
- `@mutate` supports `across` 
- `@summarize` / `@summarise` supports `across` 
- `@distinct`
- `@left_join`, `@right_join`, `@inner_join` (slight syntax differences from TidierData.jl)
- `@count`
- `@slice_min`, `@slice_max`, `@slice_sample`
- `@window_order` and `window_frame`
- `@show_query`
- `@collect`

Supported helper functions for most backends include
- `across()`
- `desc()`
- `if_else()` and `case_when()`
- `n()` 
- `starts_with()`, `ends_with()`, and `contains()`
- `as_float()`, `as_integer()`, and `as_string()`
- `is_missing()`
- `missing_if()` and `replace_missing()`

From TidierStrings.jl
- `str_detect`, `str_replace`, `str_replace_all`, `str_remove_all`, `str_remove`

From TidierDates.jl
-  `year`, `month`, `day`, `hour`, `min`, `second`, `floor_date`, `difftime`

Supported aggregate functions (as supported by the backend) with more to come
- `mean`, `minimium`, `maximum`, `std`, `sum`, `cumsum`, `cor`, `cov`, `var`

- `copy_to` (for DuckDB, MySQL, SQLite)

DuckDB specifically enables copy_to to directly reading in .parquet, .json, .csv, https file paths.
```
path = "file_path.parquet"
copy_to(conn, file_path, "table_name")
```

Bang bang `!!` Interpolation for columns and values is supported.

There are a few subtle but important differences from Tidier.jl outlined [here](https://github.com/drizk1/TidierDB.jl/blob/main/docs/examples/UserGuide/key_differences.jl).

Missing a function or backend?

You can actually use any (non-agg) sql fucntion in mutate with the correct sql syntax and it will will still run.
But open an issue, and we would be happy to address it.

Finally, some examples
```
using TidierDB
mem = duckdb_open(":memory:");
db = duckdb_connect(mem);
path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
copy_to(db, path, "mtcars2");
@chain start_query_meta(db, :mtcars2) begin
    @filter(model != starts_with("M"))
    @group_by(cyl)
    @summarize(mpg = mean(mpg))
    @mutate(sqaured = mpg^2, 
               rounded = round(mpg), 
               efficiency = case_when(
                             mpg >= cyl^2 , 12,
                             mpg < 15.2 , 14,
                              44))            
    @filter(efficiency>12)                       
    @arrange(rounded)
    @show_query
    #@collect
end
```
```
WITH cte_1 AS (
SELECT *
        FROM mtcars2
        WHERE NOT (model LIKE 'M%')),
cte_2 AS (
SELECT cyl, AVG(mpg) AS mpg
        FROM cte_1
        GROUP BY cyl),
cte_3 AS (
SELECT  cyl, mpg, POWER(mpg, 2) AS sqaured, ROUND(mpg) AS rounded, CASE WHEN mpg >= POWER(cyl, 2) THEN 12 WHEN mpg < 15.2 THEN 14 ELSE 44 END AS efficiency
        FROM cte_2 ),
cte_4 AS (
SELECT *
        FROM cte_3
        WHERE efficiency > 12)  
SELECT *
        FROM cte_4  
        ORDER BY rounded ASC
```
Now instead of ending the chain with `@show_query`, we use `@collect` to pull the df into the local environment
```
2×5 DataFrame
 Row │ cyl    mpg      sqaured  rounded  efficiency 
     │ Int64  Float64  Float64  Float64  Int64      
─────┼──────────────────────────────────────────────
   1 │     8  14.75    217.562     15.0          14
   2 │     6  19.7333  389.404     20.0          44
```
`across` in `summarize`
```
@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @summarize(across((starts_with("a"), ends_with("s")), (mean, sum)))
    #@show_query
    @collect
end
```
```
3×5 DataFrame
 Row │ cyl    mean_am   mean_vs   sum_am  sum_vs 
     │ Int64  Float64   Float64   Int64   Int64  
─────┼───────────────────────────────────────────
   1 │     4  0.727273  0.909091       8      10
   2 │     6  0.428571  0.571429       3       4
   3 │     8  0.142857  0.0            2       0
```


This links to [examples](https://github.com/drizk1/TidierDB.jl/blob/main/testing_files/olympics_examples_fromweb.jl) which achieve the same result as the SQL queries.