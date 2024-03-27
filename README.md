# TidierDB.jl

Currently supported macros:
- `@arrange`
- `@group_by` 
- `@filter`
- `@select`
- `@mutate` 
- `@summarize` / `@summarise` supports `across` with tidy selection
- `@distinct`
- `@left_join`, `@right_join`, `@inner_join` (slight syntax differences)
- `@count`
- `@slice_min`, `@slice_max`, `@slice_sample`
- `@show_query`
- `@collect`
- `@window_order` and `window_frame` are coming soon


Supported helper functions include
- `if_else` and `case_when` (case_when has slight syntax difference (`,` instead of `=>`))
- `replace_missing` and `missing_if`
- `is_missing`
- `starts_with`, `ends_with`, `contains`
- `as_integer`, `as_float`, `as_string`
- `!` negation
- `across` 

Switch to Postgres using 
`set_sql_mode(:postgres)`

Switch to DuckDB using 
`set_sql_mode(:duckdb)`

Postgres and Duck DB support includes
- Postgres specific aggregate functions: `corr`, `cov`, `std`, `var`
- From TidierStrings.jl `str_detect`, `str_replace`, `str_replace_all`, `str_remove_all`, `str_remove`
- From TidierDates.jl `year`, `month`, `day`, `hour`, `min`, `second`, `floor_date`


 Tidy selection for columns is supported in `@select`, `@group_by` and `across` in `@summarize`.
 
 Bang bang `!!` Interpolation for columns and values supported (with some edge cases to be fixed) in: `@select`, `@group_by`, `@filter`, `@summarize`, `@mutate`, `@count`, `@rename`. Other macros to join soon. 

CTEs are used to capture sequential changes, rather than subqueries (this can always be changed)

This links to [examples](https://github.com/drizk1/TidierDB.jl/blob/main/testing_files/olympics_examples_fromweb.jl) which achieve the same result as the SQL queries.

Below, a few examples are illustrated, including examples with across and interpolation

```
@chain start_query_meta(db, :mtcars2) begin
    @filter(Column1 != starts_with("M"))
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
        WHERE NOT (Column1 LIKE 'M%')),
cte_2 AS (
SELECT cyl, AVG(mpg) AS mpg
        FROM cte_1
        GROUP BY cyl),
cte_3 AS (
SELECT  *, POWER(mpg, 2) AS sqaured, ROUND(mpg) AS rounded, CASE WHEN mpg >= POWER(cyl, 2) THEN 12 WHEN mpg < 15.2 THEN 14 ELSE 44 END AS efficiency
        FROM cte_2
        GROUP BY cyl)  
SELECT *
        FROM cte_3
        GROUP BY cyl 
        HAVING efficiency > 12  
        ORDER BY rounded ASC
```
```
@chain start_query_meta(db, :mtcars2) begin
    @filter(Column1 != starts_with("M"))
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
    @collect   
end
```
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

```
@chain start_query_meta(db, :mtcars2) begin
    @filter(Column1 == starts_with("M"))
    @left_join(:join_test3, ID, Column1) ## autodetects the table/cte to apply to ID and, more importantly, column
    @select(mpg, vs:ID) ## also supports `starts_with`, `ends_with`, `contains`
    @collect
end 
```
```
WITH cte_1 AS (
SELECT *
        FROM mtcars2
        WHERE Column1 LIKE 'M%')  
SELECT mpg, vs, am, gear, carb, ID
        FROM cte_1
        LEFT
        JOIN join_test3 ON join_test3.ID = cte_1.Column1
```
```
10×6 DataFrame
 Row │ mpg      vs     am     gear   carb     ID            
     │ Float64  Int64  Int64  Int64  Int64?   String        
─────┼──────────────────────────────────────────────────────
   1 │    21.0      0      1      4  missing  Mazda RX4
   2 │    21.0      0      1      4  missing  Mazda RX4 Wag
   3 │    24.4      1      0      4        2  Merc 240D
   4 │    22.8      1      0      4        2  Merc 230
   5 │    19.2      1      0      4        4  Merc 280
   6 │    17.8      1      0      4        4  Merc 280C
   7 │    16.4      0      0      3        3  Merc 450SE
   8 │    17.3      0      0      3        3  Merc 450SL
   9 │    15.2      0      0      3        3  Merc 450SLC
  10 │    15.0      0      1      5        8  Maserati Bora
```
Interpolation support
```
other_var = "Column1"
my_var = :gear
my_val = 3.7
third_var= "mpg"
@chain start_query_meta(db, :mtcars2) begin
    @filter(!!other_var != starts_with("M"))
    @group_by(cyl)
    @summarize(mpg = mean(!!third_var))
    @mutate(test = !!my_var * !!my_val,
        sqaured = (!!my_var)^2, 
               rounded = round(!!my_var), 
               efficiency = case_when(
                             mpg >= (!!my_var)^2 , 12,
                             mpg < !!my_val , 14,
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
        WHERE NOT (Column1 LIKE 'M%')),
cte_2 AS (
SELECT cyl, AVG(mpg) AS mpg
        FROM cte_1
        GROUP BY cyl),
cte_3 AS (
SELECT  cyl, mpg, gear * 3.7 AS test, POWER(gear, 2) AS sqaured, ROUND(gear) AS rounded, CASE WHEN mpg >= POWER(gear, 2) THEN 12 WHEN mpg < 3.7 THEN 14 ELSE 44 END AS efficiency
        FROM cte_2 ),
cte_4 AS (
SELECT *
        FROM cte_3
        WHERE efficiency > 12)  
SELECT *
        FROM cte_4  
        ORDER BY rounded ASC
```