# TidierDB.jl

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/TidierOrg/TidierDB.jl/blob/main/LICENSE)
[![Docs: Latest](https://img.shields.io/badge/Docs-Latest-blue.svg)](https://tidierorg.github.io/TidierDB.jl/latest)
[![Downloads](https://img.shields.io/badge/dynamic/json?url=http%3A%2F%2Fjuliapkgstats.com%2Fapi%2Fv1%2Fmonthly_downloads%2FTidierDB&query=total_requests&suffix=%2Fmonth&label=Downloads)](http://juliapkgstats.com/pkg/TidierDB)
[![Coverage Status](https://coveralls.io/repos/github/TidierOrg/TidierDB.jl/badge.svg?branch=main)](https://coveralls.io/github/TidierOrg/TidierDB.jl?branch=main)

<img src="docs/src/assets/logo.png" align="right" style="padding-left:10px;" width="150"/>

## What is TidierDB.jl?

TiderDB.jl is a 100% Julia implementation of the dbplyr R package, and similar to Python's ibis package.

The main goal of TidierDB.jl is to bring the syntax of Tidier.jl to multiple SQL backends, making it possible to analyze data directly on databases without needing to copy the entire database into memory.

## Currently supported backends include:

|   |   |   |   |
|---------|----------|----------|----------|
| DuckDB (default) | `duckdb()` |ClickHouse | `clickhouse()`
| SQLite | `sqlite()` | Postgres | `postgres()` |
| MySQL | `mysql()` | MariaDB | `mysql()` |
| MSSQL | `mssql()` | Athena | `athena()` |
| Snowflake | `snowflake()` | Databricks | `databricks()` |
| Google Big Query | `gbq()` | Oracle | `oracle()` |

Change the backend using `set_sql_mode()` - for example  - `set_sql_mode(databricks())`

## Installation

For the stable version:

```
] add TidierDB
```

TidierDB.jl currently supports the following top-level macros:

| **Category**                     | **Supported Macros and Functions**                                                                                                                                               |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Data Manipulation**     | `@arrange`, `@group_by`, `@filter`, `@select`, `@mutate` (supports `across`), `@summarize`/`@summarise` (supports `across`), `@distinct`, `@relocate`                                 |
| **Joining**                  | `@left_join`, `@right_join`, `@inner_join`, `@anti_join`, `@full_join`, `@semi_join`, `@union`, `@union_all`                                         |
| **Slice and Order**       | `@slice_min`, `@slice_max`, `@slice_sample`, `@order`, `@window_order`, `@window_frame`                                                                                                |
| **Utility**               | `@show_query`, `@collect`, `@head`, `@count`, `show_tables`, `@create_view` , `drop_view`                                                                                                                                          |
| **Helper Functions**             | `across`, `desc`, `if_else`, `case_when`, `n`, `starts_with`, `ends_with`, `contains`, `as_float`, `as_integer`, `as_string`, `is_missing`, `missing_if`, `replace_missing` |
| **TidierStrings.jl Functions** | `str_detect`, `str_replace`, `str_replace_all`, `str_remove_all`, `str_remove`                                                                                               |
| **TidierDates.jl Functions**   | `year`, `month`, `day`, `hour`, `min`, `second`, `floor_date`, `difftime`, `mdy`, `ymd`, `dmy`                                                                                                    |
| **Aggregate Functions**          | `mean`, `minimum`, `maximum`, `std`, `sum`, `cumsum`, `cor`, `cov`, `var`, all aggregate sql fxns

`@summarize` supports any SQL aggregate function in addition to the list above. Simply write the function as written in SQL syntax and it will work.
`@mutate` supports all builtin SQL functions as well.

When using the DuckDB backend, if `db_table` recieves a file path (`.parquet`, `.json`, `.csv`, `iceberg` or `delta`), it does not copy it into memory. This allows for queries on files too big for memory. `db_table` also supports S3 bucket locations via DuckDB.

## What is the recommended way to use TidierDB?

Typically, you will want to use TidierDB alongside TidierData because there are certain functionality (such as pivoting) which are only supported in TidierData and can only be performed on data frames.

Our recommended path for using TidierDB is to import the package so that there are no namespace conflicts with TidierData. Once TidierDB is integrated with Tidier, then Tidier will automatically load the packages in this fashion.

First, let's develop and execute a query using TidierDB. Notice that all top-level macros and functions originating from TidierDB start with a `DB` prefix. Any functions defined within macros do *not* need to be prefixed within `DB` because they are actually pseudofunctions that are in actuality converted into SQL code.

Even though the code reads similarly to TidierData, note that no computational work actually occurs until you run `DB.@collect()`, which runs the SQL query and instantiates the result as a DataFrame.

```julia
using TidierData
import TidierDB as DB

db = DB.connect(DB.duckdb());
path_or_name = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"

mtcars = DB.db_table(db, path_or_name);

@chain DB.t(mtcars) begin
    DB.@filter(!starts_with(model, "M"))
    DB.@group_by(cyl)
    DB.@summarize(mpg = mean(mpg))
    DB.@mutate(mpg_squared = mpg^2,
               mpg_rounded = round(mpg),
               mpg_efficiency = case_when(
                                 mpg >= cyl^2 , "efficient",
                                 mpg < 15.2 , "inefficient",
                                 "moderate"))
    DB.@filter(mpg_efficiency in ("moderate", "efficient"))
    DB.@arrange(desc(mpg_rounded))
    DB.@collect
end
```

```
2×5 DataFrame
 Row │ cyl     mpg       mpg_squared  mpg_rounded  mpg_efficiency
     │ Int64?  Float64?  Float64?     Float64?     String?
─────┼────────────────────────────────────────────────────────────
   1 │      4   27.3444      747.719         27.0  efficient
   2 │      6   19.7333      389.404         20.0  moderate
```

## What if we wanted to pivot the result?

We cannot do this using TidierDB. However, we can call `@pivot_longer()` from TidierData *after* the result of the query has been instantiated as a DataFrame, like this:

```julia
@chain DB.t(mtcars) begin
    DB.@filter(!starts_with(model, "M"))
    DB.@group_by(cyl)
    DB.@summarize(mpg = mean(mpg))
    DB.@mutate(mpg_squared = mpg^2,
               mpg_rounded = round(mpg),
               mpg_efficiency = case_when(
                                 mpg >= cyl^2 , "efficient",
                                 mpg < 15.2 , "inefficient",
                                 "moderate"))
    DB.@filter(mpg_efficiency in ("moderate", "efficient"))
    DB.@arrange(desc(mpg_rounded))
    DB.@collect
    @pivot_longer(everything(), names_to = "variable", values_to = "value")
end
```

```
10×2 DataFrame
 Row │ variable        value
     │ String          Any
─────┼───────────────────────────
   1 │ cyl             4
   2 │ cyl             6
   3 │ mpg             27.3444
   4 │ mpg             19.7333
   5 │ mpg_squared     747.719
   6 │ mpg_squared     389.404
   7 │ mpg_rounded     27.0
   8 │ mpg_rounded     20.0
   9 │ mpg_efficiency  efficient
  10 │ mpg_efficiency  moderate
```

## What SQL query does TidierDB generate for a given piece of Julia code?

We can replace `DB.collect()` with `DB.@show_query` to reveal the underlying SQL query being generated by TidierDB. To handle complex queries, TidierDB makes heavy use of Common Table Expressions (CTE), which are a useful tool to organize long queries.

```julia
@chain DB.t(mtcars) begin
    DB.@filter(!starts_with(model, "M"))
    DB.@group_by(cyl)
    DB.@summarize(mpg = mean(mpg))
    DB.@mutate(mpg_squared = mpg^2,
               mpg_rounded = round(mpg),
               mpg_efficiency = case_when(
                                 mpg >= cyl^2 , "efficient",
                                 mpg < 15.2 , "inefficient",
                                 "moderate"))
    DB.@filter(mpg_efficiency in ("moderate", "efficient"))
    DB.@arrange(desc(mpg_rounded))
    DB.@show_query
end
```

```
WITH cte_1 AS (
SELECT *
        FROM mtcars
        WHERE NOT (starts_with(model, 'M'))),
cte_2 AS (
SELECT cyl, AVG(mpg) AS mpg
        FROM cte_1
        GROUP BY cyl),
cte_3 AS (
SELECT  cyl, mpg, POWER(mpg, 2) AS mpg_squared, ROUND(mpg) AS mpg_rounded, CASE WHEN mpg >= POWER(cyl, 2) THEN 'efficient' WHEN mpg < 15.2 THEN 'inefficient' ELSE 'moderate' END AS mpg_efficiency
        FROM cte_2 ),
cte_4 AS (
SELECT *
        FROM cte_3
        WHERE mpg_efficiency in ('moderate', 'efficient'))
SELECT *
        FROM cte_4
        ORDER BY mpg_rounded DESC
```

## TidierDB is already quite fully-featured, supporting advanced TidierData functions like `across()` for multi-column selection.

```julia
@chain DB.t(mtcars) begin
    DB.@group_by(cyl)
    DB.@summarize(across((starts_with("a"), ends_with("s")), (mean, sum)))
    DB.@collect
end
```

```
3×5 DataFrame
 Row │ cyl     am_mean   vs_mean   am_sum   vs_sum
     │ Int64?  Float64?  Float64?  Int128?  Int128?
─────┼──────────────────────────────────────────────
   1 │      4  0.727273  0.909091        8       10
   2 │      6  0.428571  0.571429        3        4
   3 │      8  0.142857  0.0             2        0
```

Bang bang `!!` interpolation for columns and values is also supported.

There are a few subtle but important differences from Tidier.jl outlined [here](https://tidierorg.github.io/TidierDB.jl/latest/examples/generated/UserGuide/key_differences/).

## Missing a function or backend?

You can use any existing SQL function within `@mutate` with the correct SQL syntax and it should just work.

But if you run into problems please open an issue, and we will be happy to take a look!
