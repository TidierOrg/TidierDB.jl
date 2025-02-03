const docstring_select = 
"""
    @select(sql_query, columns)

Select specified columns from a SQL table.

# Arguments
- `sql_query::SQLQuery`: the SQL query to select columns from.
- `columns`: Expressions specifying the columns to select. Columns can be specified by 
        - name, `table.name`
        - selectors - `starts_with()` 
        - ranges - `col1:col5`
        - excluded with `!` notation

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> df_mem = db_table(db, df, "df_view");

julia> @chain t(df_mem) begin
         @select(groups:percent)
         @collect
       end
10×3 DataFrame
 Row │ groups  value  percent 
     │ String  Int64  Float64 
─────┼────────────────────────
   1 │ bb          1      0.1
   2 │ aa          2      0.2
   3 │ bb          3      0.3
   4 │ aa          4      0.4
   5 │ bb          5      0.5
   6 │ aa          1      0.6
   7 │ bb          2      0.7
   8 │ aa          3      0.8
   9 │ bb          4      0.9
  10 │ aa          5      1.0

julia> @chain t(df_mem) begin
         @select(contains("e"))
         @collect
       end
10×2 DataFrame
 Row │ value  percent 
     │ Int64  Float64 
─────┼────────────────
   1 │     1      0.1
   2 │     2      0.2
   3 │     3      0.3
   4 │     4      0.4
   5 │     5      0.5
   6 │     1      0.6
   7 │     2      0.7
   8 │     3      0.8
   9 │     4      0.9
  10 │     5      1.0
```
"""

const docstring_filter =
"""
    @filter(sql_query, conditions...)

Filter rows in a SQL table based on specified conditions.

# Arguments
- `sql_query::SQLQuery`: The SQL query to filter rows from.
- `conditions`: Expressions specifying the conditions that rows must satisfy to be included in the output. 
                   Rows for which the expression evaluates to `true` will be included in the result. 
                   Multiple conditions can be combined using logical operators (`&&`, `||`). `@filter` will automatically 
                   detect whether the conditions belong in WHERE vs HAVING. 

                   Temporarily, it is best to use begin and end when filtering multiple conditions. (ex 2 below)
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @filter(percent > .5)
         @collect
       end
5×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AF      aa          1      0.6
   2 │ AG      bb          2      0.7
   3 │ AH      aa          3      0.8
   4 │ AI      bb          4      0.9
   5 │ AJ      aa          5      1.0

julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @summarise(mean = mean(percent))
         @filter begin 
           groups == "bb" || # logical operators can still be used like this
           mean > .5
         end
         @arrange(groups)
         @collect
       end
2×2 DataFrame
 Row │ groups  mean    
     │ String  Float64 
─────┼─────────────────
   1 │ aa          0.6
   2 │ bb          0.5

julia> q = @chain db_table(db, df, "df_view") @summarize(mean = mean(value));

julia> @eval @chain db_table(db, df, "df_view") begin
         @filter(value < \$q) 
         @collect
       end
4×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AA      bb          1      0.1
   2 │ AB      aa          2      0.2
   3 │ AF      aa          1      0.6
   4 │ AG      bb          2      0.7
```
"""

const docstring_group_by = 
"""
    @group_by(sql_query, columns...)

Group SQL table rows by specified column(s). If grouping is performed as a terminal operation without a subsequent mutatation or summarization (as in the example below), then the resulting data frame will be ungrouped when `@collect` is applied.

# Arguments
- `sql_query`: The SQL query to operate on.
- `exprs`: Expressions specifying the columns to group by. Columns can be specified by name.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @arrange(groups)
         @collect
       end
2×1 DataFrame
 Row │ groups 
     │ String 
─────┼────────
   1 │ aa
   2 │ bb
```
"""

const docstring_mutate =
"""
    @mutate(sql_query, exprs...; _by, _frame, _order)

Mutate SQL table by adding new columns or modifying existing ones.

# Arguments
- `sql_query::SQLQuery`: The SQL query to operate on.
- `exprs`: Expressions for mutating the table. New columns can be added or existing columns modified using `column_name = expression syntax`, where expression can involve existing columns.
- `_by`: optional argument that supports single column names, or vectors of columns to allow for grouping for the transformation in the macro call
- `_frame`: optional argument that allows window frames to be determined within `@mutate`. supports single digits or tuples of numbers. supports `desc()` prefix
- `_order`: optional argument that allows window orders to be determined within `@mutate`. supports single columns or vectors of names  

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @mutate(value = value * 4, new_col = percent^2)
         @collect
       end
10×5 DataFrame
 Row │ id      groups  value  percent  new_col 
     │ String  String  Int64  Float64  Float64 
─────┼─────────────────────────────────────────
   1 │ AA      bb          4      0.1     0.01
   2 │ AB      aa          8      0.2     0.04
   3 │ AC      bb         12      0.3     0.09
   4 │ AD      aa         16      0.4     0.16
   5 │ AE      bb         20      0.5     0.25
   6 │ AF      aa          4      0.6     0.36
   7 │ AG      bb          8      0.7     0.49
   8 │ AH      aa         12      0.8     0.64
   9 │ AI      bb         16      0.9     0.81
  10 │ AJ      aa         20      1.0     1.0

julia> @chain db_table(db, df, "df_view") begin
         @mutate(max = maximum(percent), sum = sum(percent), _by = groups)
         @collect
       end
10×6 DataFrame
 Row │ id      groups  value  percent  max      sum     
     │ String  String  Int64  Float64  Float64  Float64 
─────┼──────────────────────────────────────────────────
   1 │ AB      aa          2      0.2      1.0      3.0
   2 │ AD      aa          4      0.4      1.0      3.0
   3 │ AF      aa          1      0.6      1.0      3.0
   4 │ AH      aa          3      0.8      1.0      3.0
   5 │ AJ      aa          5      1.0      1.0      3.0
   6 │ AA      bb          1      0.1      0.9      2.5
   7 │ AC      bb          3      0.3      0.9      2.5
   8 │ AE      bb          5      0.5      0.9      2.5
   9 │ AG      bb          2      0.7      0.9      2.5
  10 │ AI      bb          4      0.9      0.9      2.5

julia> @chain db_table(db, df, "df_view") begin
          @mutate(value1 = sum(value), 
                      _order = percent, 
                      _frame = (-1, 1), 
                      _by = groups) 
          @mutate(value2 = sum(value), 
                      _order = desc(percent),
                      _frame = 2)  
          @arrange(groups)
          @collect
       end
10×6 DataFrame
 Row │ id      groups  value  percent  value1  value2  
     │ String  String  Int64  Float64  Int128  Int128? 
─────┼─────────────────────────────────────────────────
   1 │ AJ      aa          5      1.0       8       21
   2 │ AH      aa          3      0.8       9       16
   3 │ AF      aa          1      0.6       8       10
   4 │ AD      aa          4      0.4       7        3
   5 │ AB      aa          2      0.2       6  missing 
   6 │ AI      bb          4      0.9       6       18
   7 │ AG      bb          2      0.7      11       15
   8 │ AE      bb          5      0.5      10        6
   9 │ AC      bb          3      0.3       9        1
  10 │ AA      bb          1      0.1       4  missing 

julia> @chain db_table(db, df, "df_view") begin
         @mutate(across([:value, :percent], agg(kurtosis)))
         @collect
       end
10×6 DataFrame
 Row │ id      groups  value  percent  value_kurtosis  percent_kurtosis 
     │ String  String  Int64  Float64  Float64         Float64          
─────┼──────────────────────────────────────────────────────────────────
   1 │ AA      bb          1      0.1        -1.33393              -1.2
   2 │ AB      aa          2      0.2        -1.33393              -1.2
   3 │ AC      bb          3      0.3        -1.33393              -1.2
   4 │ AD      aa          4      0.4        -1.33393              -1.2
   5 │ AE      bb          5      0.5        -1.33393              -1.2
   6 │ AF      aa          1      0.6        -1.33393              -1.2
   7 │ AG      bb          2      0.7        -1.33393              -1.2
   8 │ AH      aa          3      0.8        -1.33393              -1.2
   9 │ AI      bb          4      0.9        -1.33393              -1.2
  10 │ AJ      aa          5      1.0        -1.33393              -1.2

julia> @chain db_table(db, df, "df_view") begin
          @mutate(value2 = sum(value), 
                      _order = desc([:value, :percent]),
                      _frame = 2);  
          @collect
       end;
```
"""

const docstring_summarize =
"""
       @summarize(sql_query, exprs...; _by)

Aggregate and summarize specified columns of a SQL table.

# Arguments
- `sql_query::SQLQuery`: The SQL query to summarize
- `exprs`: Expressions defining the aggregation and summarization operations. These can specify simple aggregations like mean, sum, and count, or more complex expressions involving existing column values.
- `_by`: optional argument that supports single column names, or vectors of columns to allow for grouping for the aggregatation in the macro call
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @summarise(across((ends_with("e"), starts_with("p")), (mean, sum)))
         @arrange(groups)
         @collect
       end
2×5 DataFrame
 Row │ groups  value_mean  percent_mean  value_sum  percent_sum 
     │ String  Float64     Float64       Int128     Float64     
─────┼──────────────────────────────────────────────────────────
   1 │ aa             3.0           0.6         15          3.0
   2 │ bb             3.0           0.5         15          2.5

julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @summarise(test = sum(percent), n = n())
         @arrange(groups)
         @collect
       end
2×3 DataFrame
 Row │ groups  test     n     
     │ String  Float64  Int64 
─────┼────────────────────────
   1 │ aa          3.0      5
   2 │ bb          2.5      5

julia> @chain db_table(db, df, "df_view") begin
                @summarise(test = sum(percent), n = n(), _by = groups)
                @arrange(groups)
                @collect
              end
2×3 DataFrame
 Row │ groups  test     n     
     │ String  Float64  Int64 
─────┼────────────────────────
   1 │ aa          3.0      5
   2 │ bb          2.5      5
```
"""
const docstring_summarise =
"""
       @summarise(sql_query, exprs...)

Aggregate and summarize specified columns of a SQL table.

# Arguments
- `sql_query::SQLQuery`: query to be summarized 
- `exprs`: Expressions defining the aggregation and summarization operations. 
    These can specify simple aggregations like mean, sum, and count, ' or more complex expressions 
    involving existing column values. `@summarize`  supports all SQL database aggregate functions
    as long as they are written with matching syntax
    


# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @summarise(across((value:percent), (mean, sum)))
         @arrange(groups)
         @collect
       end
2×5 DataFrame
 Row │ groups  value_mean  percent_mean  value_sum  percent_sum 
     │ String  Float64     Float64       Int128     Float64     
─────┼──────────────────────────────────────────────────────────
   1 │ aa             3.0           0.6         15          3.0
   2 │ bb             3.0           0.5         15          2.5

julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @summarise(test = sum(percent), n = n())
         @arrange(groups)
         @collect
       end
2×3 DataFrame
 Row │ groups  test     n     
     │ String  Float64  Int64 
─────┼────────────────────────
   1 │ aa          3.0      5
   2 │ bb          2.5      5
```
"""

const docstring_slice_min =
"""
    @slice_min(sql_query, column, n = 1)

Select rows with the smallest values in specified column. This will always return ties. 

# Arguments
- `sql_query::SQLQuery`: The SQL query to operate on.
- `column`: Column to identify the smallest values.
- `n`: The number of rows to select with the smallest values for each specified column. Default is 1, which selects the row with the smallest value.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @slice_min(value, n = 2)
         @arrange(groups, percent) # arranged due to duckdb multi threading
         @collect
       end
4×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AB      aa          2      0.2         2
   2 │ AF      aa          1      0.6         1
   3 │ AA      bb          1      0.1         1
   4 │ AG      bb          2      0.7         2

julia> @chain db_table(db, df, "df_view") begin
         @slice_min(value)
         @collect
       end
2×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AA      bb          1      0.1         1
   2 │ AF      aa          1      0.6         1

julia> @chain db_table(db, df, "df_view") begin
         @filter(percent > .1)
         @slice_min(percent)
         @collect
       end
1×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AB      aa          2      0.2         1

julia> @chain db_table(db, df, "df_view") begin
         @group_by groups
         @slice_min(percent)
         @arrange groups
         @collect
       end
2×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AB      aa          2      0.2         1
   2 │ AA      bb          1      0.1         1

julia> @chain db_table(db, df, "df_view") begin
         @summarize(percent_mean = mean(percent), _by = groups)
         @slice_min(percent_mean)
         @collect
       end
1×3 DataFrame
 Row │ groups  percent_mean  rank_col 
     │ String  Float64       Int64    
─────┼────────────────────────────────
   1 │ bb               0.5         1
```
"""

const docstring_slice_max =
"""
    @slice_max(sql_query, column, n = 1)

Select rows with the largest values in specified column. This will always return ties. 

# Arguments
- `sql_query::SQLQuery`: The SQL query to operate on.
- `column`: Column to identify the smallest values.
- `n`: The number of rows to select with the largest values for each specified column. Default is 1, which selects the row with the smallest value.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @slice_max(value, n = 2)
         @arrange(groups)
         @collect
       end
4×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AJ      aa          5      1.0         1
   2 │ AD      aa          4      0.4         2
   3 │ AE      bb          5      0.5         1
   4 │ AI      bb          4      0.9         2

julia> @chain db_table(db, df, "df_view") begin
         @slice_max(value)
         @collect
       end
2×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AE      bb          5      0.5         1
   2 │ AJ      aa          5      1.0         1

julia> @chain db_table(db, df, "df_view") begin
        @filter(percent < .9)
        @slice_max(percent)
        @collect
       end
1×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AH      aa          3      0.8         1

julia>  @chain db_table(db, df, "df_view") begin
         @group_by groups
         @slice_max(percent)
         @arrange groups
         @collect
       end
2×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AJ      aa          5      1.0         1
   2 │ AI      bb          4      0.9         1

julia> @chain db_table(db, df, "df_view") begin
         @summarize(percent_mean = mean(percent), _by = groups)
         @slice_max(percent_mean)
         @collect
       end
1×3 DataFrame
 Row │ groups  percent_mean  rank_col 
     │ String  Float64       Int64    
─────┼────────────────────────────────
   1 │ aa               0.6         1
```
"""

const docstring_slice_sample =
"""
    @slice_sample(sql_query, n)

Randomly select a specified number of rows from a SQL table.
# Arguments
- `sql_query::SQLQuery`: The SQL query to sample
- `n`: The number of rows to randomly select.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @group_by(groups)
         @slice_sample(n = 2)
         @collect
       end;

julia> @chain db_table(db, df, "df_view") begin
       @slice_sample()
       @collect
       end;
```
"""

const docstring_arrange =
"""
    @arrange(sql_query, columns...)

Order SQL table rows based on specified column(s). Of note, `@arrange` should not be used when performing ordered window functions, 
`@window_order`, or preferably the `_order` argument in `@mutate` should be used instead

# Arguments
- `sql_query::SQLQuery`: The SQL query to arrange
- `columns`: Columns to order the rows by. Can include multiple columns for nested sorting. Wrap column name with `desc()` for descending order.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> @chain db_table(db, df, "df_view") begin
         @arrange(value, desc(percent))
         @collect
       end
10×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AF      aa          1      0.6
   2 │ AA      bb          1      0.1
   3 │ AG      bb          2      0.7
   4 │ AB      aa          2      0.2
   5 │ AH      aa          3      0.8
   6 │ AC      bb          3      0.3
   7 │ AI      bb          4      0.9
   8 │ AD      aa          4      0.4
   9 │ AJ      aa          5      1.0
  10 │ AE      bb          5      0.5

julia> @chain db_table(db, df, "df_view") begin
         @arrange(desc(df_view.value))
         @collect
       end
10×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AE      bb          5      0.5
   2 │ AJ      aa          5      1.0
   3 │ AD      aa          4      0.4
   4 │ AI      bb          4      0.9
   5 │ AC      bb          3      0.3
   6 │ AH      aa          3      0.8
   7 │ AB      aa          2      0.2
   8 │ AG      bb          2      0.7
   9 │ AA      bb          1      0.1
  10 │ AF      aa          1      0.6
```
"""

const docstring_count =
"""
    @count(sql_query, columns...)

Count the number of rows grouped by specified column(s).

# Arguments
- `sql_query::SQLQuery`: The SQL query to operate on.
- `columns`: Columns to group by before counting. If no columns are specified, counts all rows in the query.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> @chain db_table(db, df, "df_view") begin
         @count(groups)
         @arrange(groups)
         @collect
       end
2×2 DataFrame
 Row │ groups  count 
     │ String  Int64 
─────┼───────────────
   1 │ aa          5
   2 │ bb          5
```
"""

const docstring_distinct =
"""
    @distinct(sql_query, columns...)

Select distinct rows based on specified column(s). Distinct works differently in TidierData vs SQL and
therefore TidierDB. Distinct will also select only the only columns it is given (or all if given none)

# Arguments
`sql_query::SQLQuery`: The SQL query to operate on.
`columns`: Columns to determine uniqueness. If no columns are specified, all columns are used to identify distinct rows.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
         @distinct(value)
         @arrange(value)
         @collect
       end
5×1 DataFrame
 Row │ value 
     │ Int64 
─────┼───────
   1 │     1
   2 │     2
   3 │     3
   4 │     4
   5 │     5

julia> @chain db_table(db, df, "df_view") begin
         @distinct
         @arrange(id)
         @collect
       end
10×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AA      bb          1      0.1
   2 │ AB      aa          2      0.2
   3 │ AC      bb          3      0.3
   4 │ AD      aa          4      0.4
   5 │ AE      bb          5      0.5
   6 │ AF      aa          1      0.6
   7 │ AG      bb          2      0.7
   8 │ AH      aa          3      0.8
   9 │ AI      bb          4      0.9
  10 │ AJ      aa          5      1.0
```
"""

const docstring_left_join =
"""
    @left_join(sql_query, join_table, orignal_table_col == new_table_col)

Perform a left join between two SQL queries based on a specified condition. 
Joins can be equi joins or inequality joins. For equi joins, the joining table 
key column is dropped. Inequality joins can be made into AsOf or rolling joins 
by wrapping the inequality in closest(key >= key2). With inequality joins, the 
columns from both tables are kept. Multiple joining criteria can be added, but 
need to be separated by commas, ie `closest(key >= key2), key3 == key3`

# Arguments
- `sql_query::SQLQuery`: The primary SQL query to operate on.
- `join_table::{SQLQuery, String}`: The secondary SQL table to join with the primary query table.
- `orignal_table_col`: Column from the original table that matches for join.  Accepts cols as bare column names or strings 
- `new_table_col`: Column from the new table that matches for join.  Accepts cols as bare column names or strings
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
                score = [88, 92, 77, 83, 95, 68, 74]);

julia> db = connect(duckdb());

julia> dfm = db_table(db, df, "df_mem"); dfj = db_table(db, df2, "df_join");

julia> @chain t(dfm) begin
         @left_join(t(dfj), id == id2 )
         @collect
       end
10×6 DataFrame
 Row │ id      groups  value  percent  category  score   
     │ String  String  Int64  Float64  String?   Int64?  
─────┼───────────────────────────────────────────────────
   1 │ AA      bb          1      0.1  X              88
   2 │ AC      bb          3      0.3  Y              92
   3 │ AE      bb          5      0.5  X              77
   4 │ AG      bb          2      0.7  Y              83
   5 │ AI      bb          4      0.9  X              95
   6 │ AB      aa          2      0.2  missing   missing 
   7 │ AD      aa          4      0.4  missing   missing 
   8 │ AF      aa          1      0.6  missing   missing 
   9 │ AH      aa          3      0.8  missing   missing 
  10 │ AJ      aa          5      1.0  missing   missing 

julia> query = @chain db_table(db, "df_join") begin
                  @filter(score > 85) # only show scores above 85 in joining table
                end;

julia> @chain t(dfm) begin
         @left_join(t(query), id == id2)
         @collect
       end
10×6 DataFrame
 Row │ id      groups  value  percent  category  score   
     │ String  String  Int64  Float64  String?   Int64?  
─────┼───────────────────────────────────────────────────
   1 │ AA      bb          1      0.1  X              88
   2 │ AC      bb          3      0.3  Y              92
   3 │ AI      bb          4      0.9  X              95
   4 │ AB      aa          2      0.2  missing   missing 
   5 │ AD      aa          4      0.4  missing   missing 
   6 │ AE      bb          5      0.5  missing   missing 
   7 │ AF      aa          1      0.6  missing   missing 
   8 │ AG      bb          2      0.7  missing   missing 
   9 │ AH      aa          3      0.8  missing   missing 
  10 │ AJ      aa          5      1.0  missing   missing 

julia>  @chain t(dfm) begin
         @mutate(test = percent * 100)
         @left_join(t(dfj), test <= score, id = id2)
         @collect
       end;


julia>  @chain t(dfm) begin
         @mutate(test = percent * 200)
         @left_join(t(dfj), closest(test >= score)) # asof join
         @collect
       end;
```
"""

const docstring_right_join =
"""
    @right_join(sql_query, join_table, orignal_table_col == new_table_col)

Perform a right join between two SQL queries based on a specified condition. 
Joins can be equi joins or inequality joins. For equi joins, the joining table 
key column is dropped. Inequality joins can be made into AsOf or rolling joins 
by wrapping the inequality in closest(key >= key2). With inequality joins, the 
columns from both tables are kept. Multiple joining criteria can be added, but 
need to be separated by commas, ie `closest(key >= key2), key3 == key3`

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table::{SQLQuery, String}`: The secondary SQL table to join with the primary query table.
- `orignal_table_col`: Column from the original table that matches for join.  Accepts cols as bare column names or strings 
- `new_table_col`: Column from the new table that matches for join.  Accepts columnss as bare column names or strings

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
                score = [88, 92, 77, 83, 95, 68, 74]);

julia> db = connect(duckdb());


julia> dfj = db_table(db, df2, "df_join");

julia> @chain db_table(db, df, "df_view") begin
         @right_join(t(dfj), id == id2)
         @collect
       end
7×6 DataFrame
 Row │ id      groups   value    percent    category  score 
     │ String  String?  Int64?   Float64?   String    Int64 
─────┼──────────────────────────────────────────────────────
   1 │ AA      bb             1        0.1  X            88
   2 │ AC      bb             3        0.3  Y            92
   3 │ AE      bb             5        0.5  X            77
   4 │ AG      bb             2        0.7  Y            83
   5 │ AI      bb             4        0.9  X            95
   6 │ AK      missing  missing  missing    Y            68
   7 │ AM      missing  missing  missing    X            74

julia> query = @chain t(dfj) begin
                  @filter(score >= 74) # only show scores above 85 in joining table
                end;

julia> @chain db_table(db, df, "df_view") begin
         @right_join(t(query), id == id2)
         @collect
       end
6×6 DataFrame
 Row │ id      groups   value    percent    category  score 
     │ String  String?  Int64?   Float64?   String    Int64 
─────┼──────────────────────────────────────────────────────
   1 │ AA      bb             1        0.1  X            88
   2 │ AC      bb             3        0.3  Y            92
   3 │ AE      bb             5        0.5  X            77
   4 │ AG      bb             2        0.7  Y            83
   5 │ AI      bb             4        0.9  X            95
   6 │ AM      missing  missing  missing    X            74
```
"""

const docstring_inner_join =
"""
    @inner_join(sql_query, join_table, orignal_table_col == new_table_col)

Perform an inner join between two SQL queries based on a specified condition. 
Joins can be equi joins or inequality joins. For equi joins, the joining table 
key column is dropped. Inequality joins can be made into AsOf or rolling joins 
by wrapping the inequality in closest(key >= key2). With inequality joins, the 
columns from both tables are kept. Multiple joining criteria can be added, but 
need to be separated by commas, ie `closest(key >= key2), key3 == key3`

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table::{SQLQuery, String}`: The secondary SQL table to join with the primary query table.
- `orignal_table_col`: Column from the original table that matches for join.  Accepts cols as bare column names or strings 
- `new_table_col`: Column from the new table that matches for join.  Accepts columns as bare column names or strings
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
                score = [88, 92, 77, 83, 95, 68, 74]);

julia> db = connect(duckdb());


julia> dfj = db_table(db, df2, "df_join");

julia> @chain db_table(db, df, "df_view") begin
         @inner_join(t(dfj), id == id2)
         @collect
       end
5×6 DataFrame
 Row │ id      groups  value  percent  category  score 
     │ String  String  Int64  Float64  String    Int64 
─────┼─────────────────────────────────────────────────
   1 │ AA      bb          1      0.1  X            88
   2 │ AC      bb          3      0.3  Y            92
   3 │ AE      bb          5      0.5  X            77
   4 │ AG      bb          2      0.7  Y            83
   5 │ AI      bb          4      0.9  X            95
```
"""
const docstring_full_join =
"""
    @inner_join(sql_query, join_table, orignal_table_col == new_table_col)
    
Perform an full join between two SQL queries based on a specified condition. 
Joins can be equi joins or inequality joins. For equi joins, the joining table 
key column is dropped. Inequality joins can be made into AsOf or rolling joins 
by wrapping the inequality in closest(key >= key2). With inequality joins, the 
columns from both tables are kept. Multiple joining criteria can be added, but 
need to be separated by commas, ie `closest(key >= key2), key3 == key3`

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table::{SQLQuery, String}`: The secondary SQL table to join with the primary query table.
- `orignal_table_col`: Column from the original table that matches for join.  Accepts cols as bare column names or strings 
- `new_table_col`: Column from the new table that matches for join.  Accepts cols as bare column names or strings
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> df2 = DataFrame(id = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
                score = [88, 92, 77, 83, 95, 68, 74]);

julia> db = connect(duckdb());


julia> dfj = db_table(db, df2, "df_join");

julia> @chain db_table(db, df, "df_view") begin
         @full_join((@chain db_table(db, "df_join") @filter(score > 70)), id == id)
         @collect
       end
11×6 DataFrame
 Row │ id      groups   value    percent    category  score   
     │ String  String?  Int64?   Float64?   String?   Int64?  
─────┼────────────────────────────────────────────────────────
   1 │ AA      bb             1        0.1  X              88
   2 │ AC      bb             3        0.3  Y              92
   3 │ AE      bb             5        0.5  X              77
   4 │ AG      bb             2        0.7  Y              83
   5 │ AI      bb             4        0.9  X              95
   6 │ AB      aa             2        0.2  missing   missing 
   7 │ AD      aa             4        0.4  missing   missing 
   8 │ AF      aa             1        0.6  missing   missing 
   9 │ AH      aa             3        0.8  missing   missing 
  10 │ AJ      aa             5        1.0  missing   missing 
  11 │ AM      missing  missing  missing    X              74
```
"""

const docstring_semi_join =
"""
    @semi_join(sql_query, join_table, orignal_table_col == new_table_col)

Perform an semi join between two SQL queries based on a specified condition. 
Joins can be equi joins or inequality joins. For equi joins, the joining table 
key column is dropped. Inequality joins can be made into AsOf or rolling joins 
by wrapping the inequality in closest(key >= key2). With inequality joins, the 
columns from both tables are kept. Multiple joining criteria can be added, but 
need to be separated by commas, ie `closest(key >= key2), key3 == key3`

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table::{SQLQuery, String}`: The secondary SQL table to join with the primary query table.
- `orignal_table_col`: Column from the original table that matches for join.  Accepts cols as bare column names or strings 
- `new_table_col`: Column from the new table that matches for join.  Accepts cols as bare column names or strings
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
                score = [88, 92, 77, 83, 95, 68, 74]);

julia> db = connect(duckdb());


julia> dfj = db_table(db, df2, "df_join");

julia> @chain db_table(db, df, "df_view") begin
         @semi_join(t(dfj), id == id2)
         @collect
       end
5×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AA      bb          1      0.1
   2 │ AC      bb          3      0.3
   3 │ AE      bb          5      0.5
   4 │ AG      bb          2      0.7
   5 │ AI      bb          4      0.9
```
"""

const docstring_anti_join =
"""
    @anti_join(sql_query, join_table, orignal_table_col == new_table_col)

Perform an anti join between two SQL queries based on a specified condition. 
Joins can be equi joins or inequality joins. For equi joins, the joining table 
key column is dropped. Inequality joins can be made into AsOf or rolling joins 
by wrapping the inequality in closest(key >= key2). With inequality joins, the 
columns from both tables are kept. Multiple joining criteria can be added, but 
need to be separated by commas, ie `closest(key >= key2), key3 == key3`

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table::{SQLQuery, String}`: The secondary SQL table to join with the primary query table.
- `orignal_table_col`: Column from the original table that matches for join.  Accepts cols as bare column names or strings 
- `new_table_col`: Column from the new table that matches for join.  Accepts cols as bare column names or strings
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
                score = [88, 92, 77, 83, 95, 68, 74]);

julia> db = connect(duckdb());


julia> dfj = db_table(db, df2, "df_join");

julia> @chain db_table(db, df, "df_view") begin
        @anti_join(t(dfj), id == id2)
        @collect
       end
5×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AB      aa          2      0.2
   2 │ AD      aa          4      0.4
   3 │ AF      aa          1      0.6
   4 │ AH      aa          3      0.8
   5 │ AJ      aa          5      1.0
```
"""


const docstring_rename =
"""
    @rename(sql_query, renamings...)

Rename one or more columns in a SQL query.

# Arguments
-`sql_query`: The SQL query to operate on.
-`renamings`: One or more pairs of old and new column names, specified as new name = old name 

# Examples 
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
       @rename(new_name = percent)
       @collect
       end
10×4 DataFrame
 Row │ id      groups  value  new_name 
     │ String  String  Int64  Float64  
─────┼─────────────────────────────────
   1 │ AA      bb          1       0.1
   2 │ AB      aa          2       0.2
   3 │ AC      bb          3       0.3
   4 │ AD      aa          4       0.4
   5 │ AE      bb          5       0.5
   6 │ AF      aa          1       0.6
   7 │ AG      bb          2       0.7
   8 │ AH      aa          3       0.8
   9 │ AI      bb          4       0.9
  10 │ AJ      aa          5       1.0
```
"""

const docstring_copy_to =
"""
       copy_to(conn, df_or_path, "name")
Allows user to copy a df to the database connection. Currently supports DuckDB, SQLite, MySql

# Arguments
-`conn`: the database connection
-`df`: dataframe to be copied or path to serve as source. With DuckDB, path supports .csv, .json, .parquet to be used without copying intermediary df.
-`name`: name as string for the database to be used
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "test");
```
"""

const docstring_window_order =
"""
       @window_order(sql_query, columns...)

Specify the order of rows for window functions within a SQL query.

# Arguments
- `sql_query`: The SQL query to operate on.
- `columns`: Columns to order the rows by for the window function. Can include multiple columns for nested sorting. Prepend a column name with - for descending order.
# Examples 
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin
        @group_by groups
        @window_frame(3)
        @window_order(desc(percent))
        @mutate(avg = mean(value))
       #@show_query 
       end;
```
"""

const docstring_window_frame = 
"""
    @window_frame(sql_query, args...)

Define the window frame for window functions in a SQL query, specifying the range of rows to include in the calculation relative to the current row.

# Arguments
- `sqlquery::SQLQuery`: The SQLQuery instance to which the window frame will be applied.
- `args...`: A variable number of arguments specifying the frame boundaries. These can be:
    - `from`: The starting point of the frame. Can be a positive or negative integer, 0 or empty. When empty, it will use UNBOUNDED
    - `to`: The ending point of the frame. Can be a positive or negative integer,  0 or empty. When empty, it will use UNBOUNDED
    - if only one integer is provided without specifying `to` or `from` it will default to from, and to will be UNBOUNDED.
    - if no arguments are given, both will be UNBOUNDED
# Examples 
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> df_mem = db_table(db, df, "df_view");

julia> @chain t(df_mem) begin
        @group_by groups
        @window_frame(3)
        @mutate(avg = mean(percent))
        #@show_query
       end;

julia> @chain t(df_mem) begin
        @group_by groups
        @window_frame(-3, 3)
        @mutate(avg = mean(percent))
        #@show_query
       end;

julia> @chain t(df_mem) begin
        @group_by groups
        @window_frame(to = -3)
        @mutate(avg = mean(percent))
        #@show_query
        @collect
       end;

julia> @chain t(df_mem) begin
        @group_by groups
        @window_frame(from = -3)
        @mutate(avg = mean(percent))
        #@show_query
        @collect
       end;
```
"""

const docstring_connect = 
"""
    connect(backend; kwargs...)

This function establishes a database connection based on the specified backend and connection parameters and sets the SQL mode

# Arguments
- `backend`: type specifying the database backend to connect to. Supported backends are:
  - `duckdb()`, `sqlite()`(SQLite), `mssql()`, `mysql()`(for MariaDB and MySQL), `clickhouse()`, `postgres()` 
- `kwargs`: Keyword arguments specifying the connection parameters for the selected backend. The required parameters vary depending on the backend:

# Returns
- A database connection object based on the selected backend.

# Examples
```jldoctest
# Connect to MySQL
# conn = connect(mysql(); host="localhost", user="root", password="password", db="mydb")
# Connect to PostgreSQL using LibPQ
# conn = connect(postgres(); host="localhost", dbname="mydb", user="postgres", password="password")
# Connect to ClickHouse
# conn = connect(clickhouse(); host="localhost", port=9000, database="mydb", user="default", password="")
# Connect to SQLite
# conn = connect(sqlite())
# Connect to Google Big Query
# conn = connect(gbq(), "json_user_key_path", "location")
# Connect to Snowflake
# conn = connect(snowflake(), "ac_id", "token", "Database_name", "Schema_name", "warehouse_name")
# Connect to Microsoft SQL Server
# conn = connect(mssql(), "DRIVER={ODBC Driver 18 for SQL Server};SERVER=host,1433;UID=sa;PWD=YourPassword;Encrypt=no;TrustServerCertificate=yes")
# Connect to DuckDB
# connect to Google Cloud via DuckDB
# google_db = connect(duckdb(), :gbq, access_key="string", secret_key="string")
# Connect to AWS via DuckDB
# aws_db = connect2(duckdb(), :aws, aws_access_key_id=get(ENV, "AWS_ACCESS_KEY_ID", "access_key"), aws_secret_access_key=get(ENV, "AWS_SECRET_ACCESS_KEY", "secret_access key"), aws_region=get(ENV, "AWS_DEFAULT_REGION", "us-east-1"))
# Connect to MotherDuck
# connect(duckdb(), ""md://..."") for first connection, vs connect(duckdb(), "md:") for reconnection
# Connect to exisiting database file
# connect(duckdb(), "path/to/database.duckdb")
# Open an in-memory database
julia> db = connect(duckdb())
DuckDB.DB(":memory:")
```
"""

const docstring_db_table =
"""
    db_table(database, table_name, athena_params, delta = false, iceberg = false, alias = "", df_name)

`db_table` starts the underlying SQL query struct, adding the metadata and table. If paths are passed directly to db_table instead of a 
name it will not copy it to memory, but rather ready directly from the file. `db_table` only supports direct file paths to a table. DataFrames 
are read as a view. It does not support database file paths such as `dbname.duckdb` or `dbname.sqlite`. Such files must be used with `connect first`

# Arguments
- `database`: The Database or connection object
- `table_name`: tablename as a string (dataframe, name, local path, or URL).
      - CSV/TSV  
      - Parquet
      - Json 
      - Iceberg
      - Delta
      - S3 tables from AWS or Google Cloud 
     - DuckDB and ClickHouse support vectors of paths and URLs. 
     - DuckDB and ClickHouse also support use of `*` wildcards to read all files of a type in a location such as:
        - `db_table(db, "Path/to/testing_files/*.parquet")`
- `delta`: must be true to read delta files
- `iceberg`: must be true to read iceberg finalize_ctes
- `alias`: optional argument when using a `*` wildcard in a file path, that allows user to determine an alias for the data being read in. If empty, it will refer to table as `data`
- `df_name` when using a DataFrame as the second argument, a third string argument must be supplied to become the name of the view.
# Example
```jldoctest

julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> db_table(db, df, "df_mem");

julia> db_table(db, "main.df_mem");
```
"""

const docstring_collect =
"""
    @collect(sql_query, stream = false)

`db_table` starts the underlying SQL query struct, adding the metadata and table. 

# Arguments
- `sql_query`: The SQL query to operate on.
- `stream`: optional streaming for query/execution of results when using duck db. Defaults to false
# Example
```julia
julia> db = connect(duckdb());

julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);
                        

julia> @collect db_table(db, "df_mem")
10×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AA      bb          1      0.1
   2 │ AB      aa          2      0.2
   3 │ AC      bb          3      0.3
   4 │ AD      aa          4      0.4
   5 │ AE      bb          5      0.5
   6 │ AF      aa          1      0.6
   7 │ AG      bb          2      0.7
   8 │ AH      aa          3      0.8
   9 │ AI      bb          4      0.9
  10 │ AJ      aa          5      1.0
```
""" 

const docstring_head =
"""
    @head(sql_query, value)

Limit SQL table number of rows returned based on specified value. 
`LIMIT` in SQL

# Arguments
- `sql_query`: The SQL query to operate on.
- `value`: Number to limit how many rows are returned. If left empty, it will default to 6 rows

# Examples
```jldoctest
julia> db = connect(duckdb());

julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);
                     

julia> @chain db_table(db, df, "df_view") begin
        @head(1) ## supports expressions ie `3-2` would return the same df below
        @collect
       end
1×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AA      bb          1      0.1
```
"""

const docstring_show_tables =
"""
    show_tables(con; GBQ_datasetname)

Shows tables available in database. currently supports DuckDB, databricks, Snowflake, GBQ, SQLite, LibPQ

# Arguments
- `con` : connection to backend
- `GBQ_datasetname` : string of dataset name
# Examples
```jldoctest
julia> db = connect(duckdb());

julia> show_tables(db);
```
"""

const docstring_from_query =
"""
    from_query(query)

This is an alias for `t()`. Refer to SQL query without changing the underlying struct. This is an alternate and convenient way to refer to an exisiting DB table

# Arguments
- `query`: The SQL query to reference

# Examples
```jldoctest

julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> dfm = db_table(db, df, "df");


julia> @chain t(dfm) @collect
10×4 DataFrame
 Row │ id      groups  value  percent 
     │ String  String  Int64  Float64 
─────┼────────────────────────────────
   1 │ AA      bb          1      0.1
   2 │ AB      aa          2      0.2
   3 │ AC      bb          3      0.3
   4 │ AD      aa          4      0.4
   5 │ AE      bb          5      0.5
   6 │ AF      aa          1      0.6
   7 │ AG      bb          2      0.7
   8 │ AH      aa          3      0.8
   9 │ AI      bb          4      0.9
  10 │ AJ      aa          5      1.0

julia> query_part =  @chain t(df_mem) @select groups:percent; 

julia> @chain t(query_part) @filter(value == 4) @collect
2×3 DataFrame
 Row │ groups   value   percent  
     │ String?  Int64?  Float64? 
─────┼───────────────────────────
   1 │ aa            4       0.4
   2 │ bb            4       0.9

julia> from_query(df_mem)
SQLQuery("", "df_mem", "", "", "", "", "", "", false, false, 4×4 DataFrame
 Row │ name     type     current_selxn  table_name 
     │ String?  String?  Int64          String     
─────┼─────────────────────────────────────────────
   1 │ id       VARCHAR              1  df_mem
   2 │ groups   VARCHAR              1  df_mem
   3 │ value    BIGINT               1  df_mem
   4 │ percent  DOUBLE               1  df_mem, false, DuckDB.DB(":memory:"), TidierDB.CTE[], 0, nothing, "", "")
```
"""

const docstring_union = 
"""
    @union(sql_query1, sql_query2, all = false)

Combine two SQL queries using the `UNION` operator.

# Arguments
- `sql_query1`: The first SQL query to combine.
- `sql_query2`: The second SQL query to combine.
- `all`: Defaults to false, when true it will will return duplicates. `UNION ALL`

# Returns
- A lazy query of all distinct rows in the second query bound to the first

# Examples
```jldoctest
julia> db = connect(duckdb());

julia> df1 = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> df2 = DataFrame(id = [4, 5, 6], value = [40, 50, 60]);

julia> df1_table = db_table(db, df1, "df1");

julia> df2_table = db_table(db, df2, "df2");

julia> @chain t(df1_table) @union(df2_table) @collect
6×2 DataFrame
 Row │ id     value 
     │ Int64  Int64 
─────┼──────────────
   1 │     1     10
   2 │     2     20
   3 │     3     30
   4 │     4     40
   5 │     5     50
   6 │     6     60

julia> @chain t(df1_table) begin 
        @union("df1", all = false)
        @collect
       end
3×2 DataFrame
 Row │ id     value 
     │ Int64  Int64 
─────┼──────────────
   1 │     1     10
   2 │     2     20
   3 │     3     30

julia> @chain t(df1_table) begin 
        @union("df1", all = true) 
        @collect
       end
6×2 DataFrame
 Row │ id     value 
     │ Int64  Int64 
─────┼──────────────
   1 │     1     10
   2 │     2     20
   3 │     3     30
   4 │     1     10
   5 │     2     20
   6 │     3     30

julia> query = @chain t(df2_table) @filter(value == 50);

julia> @chain t(df1_table) begin 
        @mutate(id = id + 5)
        @filter(id > 6)
        @union(t(query))
        @collect
       end
3×2 DataFrame
 Row │ id     value 
     │ Int64  Int64 
─────┼──────────────
   1 │     7     20
   2 │     8     30
   3 │     5     50
```
"""

const docstring_union_all = 
"""
    @union(sql_query1, sql_query2)

Combine two SQL queries using the `UNION ALL ` operator.

# Arguments
- `sql_query1`: The first SQL query to combine.
- `sql_query2`: The second SQL query to combine.

# Returns
- A lazy query of all rows in the second query bound to the first

# Examples
```jldoctest
julia> db = connect(duckdb());

julia> df1 = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> df1_table = db_table(db, df1, "df1");

julia> @chain t(df1_table) @union_all(df1_table) @collect
6×2 DataFrame
 Row │ id     value 
     │ Int64  Int64 
─────┼──────────────
   1 │     1     10
   2 │     2     20
   3 │     3     30
   4 │     1     10
   5 │     2     20
   6 │     3     30
```
"""

const docstring_intersect = 
"""
    @intersect(sql_query1, sql_query2, all = false)

Combine two SQL queries/tables using `INTERSECT`

# Arguments
- `sql_query1`: The first SQL query to combine.
- `sql_query2`: The second SQL query to combine.
- `all`: Defaults to false, when true it will return duplicates. `INTERSECT ALL`

# Returns
- A lazy query of all rows in the second query bound to the first

# Examples
```jldoctest
julia> db = connect(duckdb());

julia> df1 = DataFrame(id = [1, 2, 2, 3, 4],
        name = ["Alice", "Bob", "Bob", "Charlie", "David"]);

julia> df2 = DataFrame( id = [2, 2, 3, 5],
       name = ["Bob", "Bob", "Charlie", "Eve"]);

julia> df1_table = db_table(db, df1, "df1"); 

julia> df2_table = db_table(db, df2, "df2"); 

julia> @chain t(df1_table) @intersect(df2_table) @collect
2×2 DataFrame
 Row │ id     name    
     │ Int64  String  
─────┼────────────────
   1 │     2  Bob
   2 │     3  Charlie

julia> @chain t(df1_table) @intersect(df2_table, all = true) @collect
3×2 DataFrame
 Row │ id     name    
     │ Int64  String  
─────┼────────────────
   1 │     3  Charlie
   2 │     2  Bob
   3 │     2  Bob
```
"""

const docstring_setdiff = 
"""
    @setdiff(sql_query1, sql_query2, all = false)

Combine two SQL queries/tables using `EXECPT`

# Arguments
- `sql_query1`: The first SQL query to combine.
- `sql_query2`: The second SQL query to combine.
- `all`: Defaults to false, when true it will return duplicates. `EXCEPT ALL`

# Returns
- A lazy query of all rows in the second query bound to the first

# Examples
```jldoctest
julia> db = connect(duckdb());

julia> df1 = DataFrame(id = [1, 1, 2, 2, 3, 4],
        name = ["Alice", "Alice", "Bob", "Bob", "Charlie", "David"]);

julia> df2 = DataFrame(id = [2, 2, 3, 5],
       name = ["Bob", "Bob", "Charlie", "Eve"]);

julia> df1_table = db_table(db, df1, "df1"); 

julia> df2_table = db_table(db, df2, "df2");

julia> @chain t(df1_table) @setdiff(df2_table) @collect
2×2 DataFrame
 Row │ id     name   
     │ Int64  String 
─────┼───────────────
   1 │     1  Alice
   2 │     4  David

julia> @chain t(df1_table) @setdiff(df2_table, all = true) @collect
3×2 DataFrame
 Row │ id     name   
     │ Int64  String 
─────┼───────────────
   1 │     1  Alice
   2 │     1  Alice
   3 │     4  David
```
"""

const docstring_create_view =
"""
    @create_view(sql_query, name, replace = true)

Create a view from a SQL query. Currently supports DuckDB, MySQL, GBQ, Postgres

# Arguments
- `sql_query`: The SQL query to create a view from.
- `name`: The name of the view to create.
- `replace`: Boolean value that defaults to false so as not to replace exisiting views

# Examples
```jldoctest
julia> db = connect(duckdb());

julia> df = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> @chain db_table(db, df, "df1") @create_view(viewer); # will not overwrite existing view

julia> db_table(db, "viewer");

julia> @chain db_table(db, df, "df1") @create_view(viewer, true); # will overwrite exisiting view
```
"""

const docstring_drop_view =
"""
    drop_view(sql_query, name)

Drop a view. Currently supports DuckDB, MySQL, GBQ, Postgres

# Arguments
- `sql_query`: The SQL query to create a view from.
- `name`: The name of the view to drop.

# Examples
```jldoctest
julia> db = connect(duckdb());

julia> df = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> @chain db_table(db, df, "df1") @create_view(viewer);

julia> drop_view(db, "viewer");
```
"""

const docstring_compute =
"""
    @compute(sql_query, name, replace = false)

Creates a remote table on database memory from a SQL query. Currently supports DuckDB, MySQL, GBQ, Postgres


# Arguments
- `sql_query`: The SQL query to create a table from.
- `name`: The name of the table to create.
- `replace`: defaults to false if table should be replaced if it already exists.

# Examples
```jldoctest
julia> db = connect(duckdb());

julia> df = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> @chain db_table(db, df, "df1") @compute(table2, true);

julia> db_table(db, "table2")
SQLQuery("", "table", "", "", "", "", "", "", false, false, 2×4 DataFrame
 Row │ name    type    current_selxn  table_name 
     │ String  String  Int64          String     
─────┼───────────────────────────────────────────
   1 │ id      BIGINT              1  table2
   2 │ value   BIGINT              1  table2, false, DuckDB.DB(":memory:"), TidierDB.CTE[], 0, nothing, "", "")
```
"""

const docstring_warnings =
"""
    warnings(show::Bool)

Sets the global warning flag to the specified boolean value.

# Arguments
- `flag::Bool`: A boolean value to set the warning flag. If `true`, warnings will be enabled; if `false`, warnings will be disabled.

# Default Behavior
By default, the warning flag is set to `false`, meaning that warnings are disabled unless explicitly enabled by setting this function with `true`.

# Example
```
julia> warnings(true);
```
"""

const docstring_relocate =
"""
    @relocate(sql_query, columns, before = nothing, after = nothing)

Rearranges the columns in the queried table. This function allows for moving specified columns to a new position within the table, either before or after a given target column. The `columns`, `before`, and `after` arguments all accept tidy selection functions. Only one of `before` or `after` should be specified. If neither are specified, the selected columns will be moved to the beginning of the table.

# Arguments
- `sql_query`: The SQL query to create a table from.
- `columns`: Column or columns to to be moved.
- `before`: (Optional) Column or columns before which the specified columns will be moved. If not provided or `nothing`, this argument is ignored.
- `after`: (Optional) Column or columns after which the specified columns will be moved. If not provided or `nothing`, this argument is ignored. 

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());


julia> @chain db_table(db, df, "df_view") begin 
        @relocate(groups, value, ends_with("d"), after = percent) 
        @collect
       end
10×4 DataFrame
 Row │ percent  groups  value  id     
     │ Float64  String  Int64  String 
─────┼────────────────────────────────
   1 │     0.1  bb          1  AA
   2 │     0.2  aa          2  AB
   3 │     0.3  bb          3  AC
   4 │     0.4  aa          4  AD
   5 │     0.5  bb          5  AE
   6 │     0.6  aa          1  AF
   7 │     0.7  bb          2  AG
   8 │     0.8  aa          3  AH
   9 │     0.9  bb          4  AI
  10 │     1.0  aa          5  AJ

julia> @chain db_table(db, df, "df_view") begin 
        @relocate([:percent, :groups], before = id) 
        @collect
       end
10×4 DataFrame
 Row │ percent  groups  id      value 
     │ Float64  String  String  Int64 
─────┼────────────────────────────────
   1 │     0.1  bb      AA          1
   2 │     0.2  aa      AB          2
   3 │     0.3  bb      AC          3
   4 │     0.4  aa      AD          4
   5 │     0.5  bb      AE          5
   6 │     0.6  aa      AF          1
   7 │     0.7  bb      AG          2
   8 │     0.8  aa      AH          3
   9 │     0.9  bb      AI          4
  10 │     1.0  aa      AJ          5
```
"""


const docstring_aggregate_and_window_functions =
"""
       Aggregate and Window Functions

Nearly all aggregate functions from any database are supported both `@summarize` and `@mutate`. 

With `@summarize`, an aggregate functions available on a SQL backend can be used as they are in sql with the same syntax (`'` should be replaced with `"`)

`@mutate` supports them as well, however, unless listed below, the function call must be wrapped with `agg()`
- Aggregate Functions: `maximum`, `minimum`, `mean`, `std`, `sum`, `cumsum`
- Window Functions: `lead`, `lag`, `dense_rank`, `nth_value`, `ntile`, `rank_dense`, `row_number`, `first_value`, `last_value`, `cume_dist`

If a function is needed regularly, instead of wrapping it in `agg`, it can also be added to `window_agg_fxns` with `push!` as demonstrated below
       
The list of DuckDB aggregate functions and their syntax can be found [here](https://duckdb.org/docs/sql/functions/aggregates.html#general-aggregate-functions)
Please refer to your backend documentation for a complete list with syntac, but open an issue on TidierDB if your run into roadblocks.  
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                value1 = [i - 4^1 for i in -4.5:4.5], 
                value2 = [i + 2^i for i in 1:10], 
                percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> @chain db_table(db, df, "df_agg") begin
         @summarise(
             r2 = regr_r2(value2, value1),
             across(contains("value"), median), 
             _by = groups)
         @arrange(groups)
         @collect
       end
2×4 DataFrame
 Row │ groups  r2        value1_median  value2_median 
     │ String  Float64   Float64        Float64       
─────┼────────────────────────────────────────────────
   1 │ aa      0.700161           -3.5           70.0
   2 │ bb      0.703783           -4.5           37.0

julia> @chain db_table(db, df, "df_agg") begin
         @mutate(
            slope = agg(regr_slope(value1, value2)),
            var = agg(var_samp(value2)),
            std = std(value2), # since this is in the list above, it does not get wrapped in `agg`
            _by = groups
         )
         @mutate(var = round(var))
         @select !percent
         @arrange(groups)
         @collect
       end
10×7 DataFrame
 Row │ id      groups  value1   value2  slope       var       std     
     │ String  String  Float64  Int64   Float64     Float64   Float64 
─────┼────────────────────────────────────────────────────────────────
   1 │ AB      aa         -7.5       6  0.00608835  188885.0  434.609
   2 │ AD      aa         -5.5      20  0.00608835  188885.0  434.609
   3 │ AF      aa         -3.5      70  0.00608835  188885.0  434.609
   4 │ AH      aa         -1.5     264  0.00608835  188885.0  434.609
   5 │ AJ      aa          0.5    1034  0.00608835  188885.0  434.609
   6 │ AA      bb         -8.5       3  0.0121342    47799.0  218.629
   7 │ AC      bb         -6.5      11  0.0121342    47799.0  218.629
   8 │ AE      bb         -4.5      37  0.0121342    47799.0  218.629
   9 │ AG      bb         -2.5     135  0.0121342    47799.0  218.629
  10 │ AI      bb         -0.5     521  0.0121342    47799.0  218.629

julia> push!(TidierDB.window_agg_fxns, :regr_slope);

julia> @chain db_table(db, df, "df_agg") begin
         @mutate(
            slope = regr_slope(value1, value2), # no longer wrapped in `agg` following the above
            _by = groups
         )
         @select !percent
         @arrange(groups)
         @collect
       end
10×5 DataFrame
 Row │ id      groups  value1   value2  slope      
     │ String  String  Float64  Int64   Float64    
─────┼─────────────────────────────────────────────
   1 │ AB      aa         -7.5       6  0.00608835
   2 │ AD      aa         -5.5      20  0.00608835
   3 │ AF      aa         -3.5      70  0.00608835
   4 │ AH      aa         -1.5     264  0.00608835
   5 │ AJ      aa          0.5    1034  0.00608835
   6 │ AA      bb         -8.5       3  0.0121342
   7 │ AC      bb         -6.5      11  0.0121342
   8 │ AE      bb         -4.5      37  0.0121342
   9 │ AG      bb         -2.5     135  0.0121342
  10 │ AI      bb         -0.5     521  0.0121342
```
"""