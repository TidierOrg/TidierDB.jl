## What is TidierDB.jl?

<img src="/assets/logo.png" align="left" style="padding-right:10px"; width="150"></img>

TiderDB.jl is a 100% Julia implementation of the dbplyr R package, and similar to Python's ibis package.

The main goal of TidierDB.jl is to bring the syntax of Tidier.jl to multiple SQL backends, making it possible to analyze data directly on databases without needing to copy the entire database into memory.

## Currently supported backends include:

- DuckDB (the default) `set_sql_mode(:duckdb)`
- ClickHouse `set_sql_mode(:clickhouse)`
- SQLite `set_sql_mode(:lite)`
- MySQL and MariaDB `set_sql_mode(:mysql)`
- MSSQL `set_sql_mode(:mssql)`
- Postgres `set_sql_mode(:postgres)`

The style of SQL that is generated can be modified using `set_sql_mode()`.

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
- `@mutate`, which supports `across()` 
- `@summarize` and `@summarise`, which supports `across()` 
- `@distinct`
- `@left_join`, `@right_join`, `@inner_join` (slight syntax differences from TidierData.jl)
- `@count`
- `@slice_min`, `@slice_max`, `@slice_sample`
- `@window_order` and `window_frame`
- `@show_query`
- `@collect`

Supported helper functions for most backends include:
- `across()`
- `desc()`
- `if_else()` and `case_when()`
- `n()` 
- `starts_with()`, `ends_with()`, and `contains()`
- `as_float()`, `as_integer()`, and `as_string()`
- `is_missing()`
- `missing_if()` and `replace_missing()`

From TidierStrings.jl:
- `str_detect`, `str_replace`, `str_replace_all`, `str_remove_all`, `str_remove`

From TidierDates.jl:
-  `year`, `month`, `day`, `hour`, `min`, `second`, `floor_date`, `difftime`

Supported aggregate functions (as supported by the backend) with more to come
- `mean`, `minimium`, `maximum`, `std`, `sum`, `cumsum`, `cor`, `cov`, `var`
- `@summarize` supports any SQL aggregate function in addition to the list above. Simply write the function as written in SQL syntax and it will work 
- `agg_str` allows any SQL aggregate function not listed above to be used in `@mutate`. Simply write the function expression as written in SQL syntax as a string wrapped in `agg_str`, and subsequent windowing is handled by `@mutate`.
- `copy_to` (for DuckDB, MySQL, SQLite)

DuckDB specifically enables copy_to to directly reading in `.parquet`, `.json`, `.csv`, and `.arrow` file, including https file paths.

```julia
path = "file_path.parquet"
copy_to(conn, file_path, "table_name")
```

## What is the recommended way to use TidierDB?

Typically, you will want to use TidierDB alongside TidierData because there are certain functionality (such as pivoting) which are only supported in TidierData and can only be performed on data frames.

Our recommended path for using TidierDB is to import the package so that there are no namespace conflicts with TidierData. Once TidierDB is integrated with Tidier, then Tidier will automatically load the packages in this fashion.

First, let's develop and execute a query using TidierDB. Notice that all top-level macros and functions originating from TidierDB start with a `DB` prefix. Any functions defined within macros do *not* need to be prefixed within `DB` because they are actually pseudofunctions that are in actuality converted into SQL code.

Even though the code reads similarly to TidierData, note that no computational work actually occurs until you run `DB.@collect()`, which runs the SQL query and instantiates the result as a DataFrame.

```julia
using TidierData
import TidierDB as DB

mem = DB.duckdb_open(":memory:");
db = DB.duckdb_connect(mem);
path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
DB.copy_to(db, path, "mtcars");

@chain DB.db_table(db, :mtcars) begin
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
@chain DB.db_table(db, :mtcars) begin
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
@chain DB.db_table(db, :mtcars) begin
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
@chain DB.db_table(db, :mtcars) begin
    DB.@group_by(cyl)
    DB.@summarize(across((starts_with("a"), ends_with("s")), (mean, sum)))
    DB.@collect
end
```

```
3×5 DataFrame
 Row │ cyl     mean_am   mean_vs   sum_am   sum_vs  
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