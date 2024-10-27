const docstring_select = 
"""
    @select(sql_query, columns)

Select specified columns from a SQL table.

# Arguments
- `sql_query`: The SQL query to select columns from.
- `columns`: Expressions specifying the columns to select. Columns can be specified by name, 
                and new columns can be created with expressions using existing column values.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> df_mem = db_table(db, :df_mem);

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
- `sql_query`: The SQL query to filter rows from.
- `conditions`: Expressions specifying the conditions that rows must satisfy to be included in the output. 
                   Rows for which the expression evaluates to `true` will be included in the result. 
                   Multiple conditions can be combined using logical operators (`&&`, `||`). It will automatically 
                   detect whether the conditions belong in WHERE vs HAVING. 

                   Temporarily, it is best to use begin and end when filtering multiple conditions. (ex 2 below)
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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

julia> @chain db_table(db, :df_mem) begin
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

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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
    @mutate(sql_query, exprs...)

Mutate SQL table rows by adding new columns or modifying existing ones.

# Arguments
- `sql_query`: The SQL query to operate on.
- `exprs`: Expressions for mutating the table. New columns can be added or existing columns modified using column_name = expression syntax, where expression can involve existing columns.
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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
```
"""

const docstring_summarize =
"""
       @summarize(sql_query, exprs...)

Aggregate and summarize specified columns of a SQL table.

# Arguments
- `sql_query`: The SQL query to operate on.
- `exprs`: Expressions defining the aggregation and summarization operations. These can specify simple aggregations like mean, sum, and count, or more complex expressions involving existing column values.
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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

julia> @chain db_table(db, :df_mem) begin
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
const docstring_summarise =
"""
       @summarise(sql_query, exprs...)

Aggregate and summarize specified columns of a SQL table.

# Arguments
- `sql_query`: The SQL query to operate on.
- `exprs`: Expressions defining the aggregation and summarization operations. These can specify simple aggregations like mean, sum, and count, or more complex expressions involving existing column values.
# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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

julia> @chain db_table(db, :df_mem) begin
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
- `sql_query`: The SQL query to operate on.
- `column`: Column to identify the smallest values.
- `n`: The number of rows to select with the smallest values for each specified column. Default is 1, which selects the row with the smallest value.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
         @group_by(groups)
         @slice_min(value, n = 2)
         @collect
       end;

julia> @chain db_table(db, :df_mem) begin
         @slice_min(value)
         @collect
       end
2×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AA      bb          1      0.1         1
   2 │ AF      aa          1      0.6         1
```
"""

const docstring_slice_max =
"""
    @slice_max(sql_query, column, n = 1)

Select rows with the largest values in specified column. This will always return ties. 

# Arguments
- `sql_query`: The SQL query to operate on.
- `column`: Column to identify the smallest values.
- `n`: The number of rows to select with the largest values for each specified column. Default is 1, which selects the row with the smallest value.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
         @group_by(groups)
         @slice_max(value, n = 2)
         @collect
       end;

julia> @chain db_table(db, :df_mem) begin
         @slice_max(value)
         @collect
       end
2×5 DataFrame
 Row │ id      groups  value  percent  rank_col 
     │ String  String  Int64  Float64  Int64    
─────┼──────────────────────────────────────────
   1 │ AE      bb          5      0.5         1
   2 │ AJ      aa          5      1.0         1
```
"""

const docstring_slice_sample =
"""
    @slice_sample(sql_query, n)

Randomly select a specified number of rows from a SQL table.
# Arguments
- `sql_query`: The SQL query to operate on.
- `n`: The number of rows to randomly select.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
         @group_by(groups)
         @slice_sample(n = 2)
         @collect
       end;

julia> @chain db_table(db, :df_mem) begin
       @slice_sample()
       @collect
       end;
```
"""

const docstring_arrange =
"""
    @arrange(sql_query, columns...)

Order SQL table rows based on specified column(s).

# Arguments
- `sql_query`: The SQL query to operate on.
- `columns`: Columns to order the rows by. Can include multiple columns for nested sorting. Wrap column name with `desc()` for descending order.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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
```
"""

const docstring_count =
"""
    @count(sql_query, columns...)

Count the number of rows grouped by specified column(s).

# Arguments
- `sql_query`: The SQL query to operate on.
- `columns`: Columns to group by before counting. If no columns are specified, counts all rows in the query.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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
`sql_query`: The SQL query to operate on.
`columns`: Columns to determine uniqueness. If no columns are specified, all columns are used to identify distinct rows.

# Examples
```jldoctest
julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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

julia> @chain db_table(db, :df_mem) begin
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
    @left_join(sql_query, join_table, orignal_table_col = new_table_col)

Perform a left join between two SQL queries based on a specified condition. 
This syntax here is slightly different than TidierData.jl, however, because 
SQL does not drop the joining column, for the metadata storage, it is 
preferrable for the names to be different 

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table`: The secondary SQL table to join with the primary query table.
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

julia> copy_to(db, df, "df_mem");

julia> copy_to(db, df2, "df_join");

julia> @chain db_table(db, "df_mem") begin
         @left_join("df_join", "id" = "id2" )
         @collect
       end
10×7 DataFrame
 Row │ id      groups  value  percent  id2      category  score   
     │ String  String  Int64  Float64  String?  String?   Int64?  
─────┼────────────────────────────────────────────────────────────
   1 │ AA      bb          1      0.1  AA       X              88
   2 │ AC      bb          3      0.3  AC       Y              92
   3 │ AE      bb          5      0.5  AE       X              77
   4 │ AG      bb          2      0.7  AG       Y              83
   5 │ AI      bb          4      0.9  AI       X              95
   6 │ AB      aa          2      0.2  missing  missing   missing 
   7 │ AD      aa          4      0.4  missing  missing   missing 
   8 │ AF      aa          1      0.6  missing  missing   missing 
   9 │ AH      aa          3      0.8  missing  missing   missing 
  10 │ AJ      aa          5      1.0  missing  missing   missing 

julia> query = @chain db_table(db, "df_join") begin
                  @filter(score > 85) # only show scores above 85 in joining table
                end;

julia> @chain db_table(db, "df_mem") begin
         @left_join(t(query), id = id2)
         @collect
       end
10×7 DataFrame
 Row │ id      groups  value  percent  id2      category  score   
     │ String  String  Int64  Float64  String?  String?   Int64?  
─────┼────────────────────────────────────────────────────────────
   1 │ AA      bb          1      0.1  AA       X              88
   2 │ AC      bb          3      0.3  AC       Y              92
   3 │ AI      bb          4      0.9  AI       X              95
   4 │ AB      aa          2      0.2  missing  missing   missing 
   5 │ AD      aa          4      0.4  missing  missing   missing 
   6 │ AE      bb          5      0.5  missing  missing   missing 
   7 │ AF      aa          1      0.6  missing  missing   missing 
   8 │ AG      bb          2      0.7  missing  missing   missing 
   9 │ AH      aa          3      0.8  missing  missing   missing 
  10 │ AJ      aa          5      1.0  missing  missing   missing 

```
"""

const docstring_right_join =
"""
    @right_join(sql_query, join_table, orignal_table_col = new_table_col)

Perform a right join between two SQL queries based on a specified condition. 
This syntax here is slightly different than TidierData.jl, however, because 
SQL does not drop the joining column, for the metadata storage, it is 
preferrable for the names to be different 

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table`: The secondary SQL table to join with the primary query table.
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

julia> copy_to(db, df, "df_mem");

julia> copy_to(db, df2, "df_join");

julia> @chain db_table(db, :df_mem) begin
         @right_join("df_join", id = id2)
         @collect
       end
7×7 DataFrame
 Row │ id       groups   value    percent    id2     category  score 
     │ String?  String?  Int64?   Float64?   String  String    Int64 
─────┼───────────────────────────────────────────────────────────────
   1 │ AA       bb             1        0.1  AA      X            88
   2 │ AC       bb             3        0.3  AC      Y            92
   3 │ AE       bb             5        0.5  AE      X            77
   4 │ AG       bb             2        0.7  AG      Y            83
   5 │ AI       bb             4        0.9  AI      X            95
   6 │ missing  missing  missing  missing    AK      Y            68
   7 │ missing  missing  missing  missing    AM      X            74

julia> query = @chain db_table(db, "df_join") begin
                  @filter(score >= 74) # only show scores above 85 in joining table
                end;

julia> @chain db_table(db, :df_mem) begin
         @right_join(t(query), id = id2)
         @collect
       end
6×7 DataFrame
 Row │ id       groups   value    percent    id2     category  score 
     │ String?  String?  Int64?   Float64?   String  String    Int64 
─────┼───────────────────────────────────────────────────────────────
   1 │ AA       bb             1        0.1  AA      X            88
   2 │ AC       bb             3        0.3  AC      Y            92
   3 │ AE       bb             5        0.5  AE      X            77
   4 │ AG       bb             2        0.7  AG      Y            83
   5 │ AI       bb             4        0.9  AI      X            95
   6 │ missing  missing  missing  missing    AM      X            74
```
"""

const docstring_inner_join =
"""
    @inner_join(sql_query, join_table, orignal_table_col = new_table_col)

Perform an inner join between two SQL queries based on a specified condition. 
This syntax here is slightly different than TidierData.jl, however, because 
SQL does not drop the joining column, for the metadata storage, it is 
preferrable for the names to be different 

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table`: The secondary SQL table to join with the primary query table.
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

julia> copy_to(db, df, "df_mem");

julia> copy_to(db, df2, "df_join");

julia> @chain db_table(db, :df_mem) begin
         @inner_join("df_join", "id" = id2)
         @collect
       end
5×7 DataFrame
 Row │ id      groups  value  percent  id2     category  score 
     │ String  String  Int64  Float64  String  String    Int64 
─────┼─────────────────────────────────────────────────────────
   1 │ AA      bb          1      0.1  AA      X            88
   2 │ AC      bb          3      0.3  AC      Y            92
   3 │ AE      bb          5      0.5  AE      X            77
   4 │ AG      bb          2      0.7  AG      Y            83
   5 │ AI      bb          4      0.9  AI      X            95
```
"""
const docstring_full_join =
"""
    @inner_join(sql_query, join_table, orignal_table_col = new_table_col)
    
Perform an full join between two SQL queries based on a specified condition. 
This syntax here is slightly different than TidierData.jl, however, because 
SQL does not drop the joining column, for the metadata storage, it is 
preferrable for the names to be different 

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table`: The secondary SQL table to join with the primary query table.
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

julia> copy_to(db, df, "df_mem");

julia> copy_to(db, df2, "df_join");

julia> @chain db_table(db, :df_mem) begin
         @full_join((@chain db_table(db, "df_join") @filter(score > 70)), id)
         #@aside @show_query _
         @collect
       end
11×7 DataFrame
 Row │ id       groups   value    percent    id_1     category  score   
     │ String?  String?  Int64?   Float64?   String?  String?   Int64?  
─────┼──────────────────────────────────────────────────────────────────
   1 │ AA       bb             1        0.1  AA       X              88
   2 │ AC       bb             3        0.3  AC       Y              92
   3 │ AE       bb             5        0.5  AE       X              77
   4 │ AG       bb             2        0.7  AG       Y              83
   5 │ AI       bb             4        0.9  AI       X              95
   6 │ AB       aa             2        0.2  missing  missing   missing 
   7 │ AD       aa             4        0.4  missing  missing   missing 
   8 │ AF       aa             1        0.6  missing  missing   missing 
   9 │ AH       aa             3        0.8  missing  missing   missing 
  10 │ AJ       aa             5        1.0  missing  missing   missing 
  11 │ missing  missing  missing  missing    AM       X              74
```
"""

const docstring_semi_join =
"""
    @semi_join(sql_query, join_table, orignal_table_col = new_table_col)

Perform an semi join between two SQL queries based on a specified condition. 
This syntax here is slightly different than TidierData.jl, however, because 
SQL does not drop the joining column, for the metadata storage, it is 
preferrable for the names to be different 

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table`: The secondary SQL table to join with the primary query table.
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

julia> copy_to(db, df, "df_mem");

julia> copy_to(db, df2, "df_join");

julia> @chain db_table(db, :df_mem) begin
         @semi_join("df_join", id = id2)
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
    @anti_join(sql_query, join_table, orignal_table_col = new_table_col)

Perform an anti join between two SQL queries based on a specified condition. 
This syntax here is slightly different than TidierData.jl, however, because 
SQL does not drop the joining column, for the metadata storage, it is 
preferrable for the names to be different 

# Arguments
- `sql_query`: The primary SQL query to operate on.
- `join_table`: The secondary SQL table to join with the primary query table.
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

julia> copy_to(db, df, "df_mem");

julia> copy_to(db, df2, "df_join");

julia> @chain db_table(db, :df_mem) begin
        @anti_join("df_join", id = id2)
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

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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

julia> copy_to(db, df, "df_mem");

julia> @chain db_table(db, :df_mem) begin
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

julia> copy_to(db, df, "df_mem");

julia> df_mem = db_table(db, :df_mem);

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
       # @window_frame(to = -3)
        @mutate(avg = mean(percent))
        #@show_query
        @collect
       end;

julia> @chain t(df_mem) begin
        @group_by groups
        @window_frame()
        @mutate(avg = mean(percent))
        #@show_query
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
  - MySQL:
    - `host`: The host name or IP address of the MySQL server. Default is "localhost".
    - `user`: The username for authentication. Default is an empty string.
    - `password`: The password for authentication.
    - `db`: The name of the database to connect to (optional).
    - `port`: The port number of the MySQL server (optional).

# Returns
- A database connection object based on the selected backend.

# Examples
```julia
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
DuckDB.Connection(":memory:")
```
"""

const docstring_db_table =
"""
    db_table(database, table_name, athena_params, delta = false, iceberg = false)

`db_table` starts the underlying SQL query struct, adding the metadata and table. If paths are passed directly to db_table instead of a 
name it will not copy it to memory, but rather ready directly from the file. `db_table` only supports direct file paths to a table. It does not support database file paths such as `dbname.duckdb` or `dbname.sqlite`. Such files must be used with `connect first`

# Arguments
- `database`: The Database or connection object
- `table_name`: tablename as a string (name, local path, or URL).
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

# Example
```julia

julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> db_table(db, "df_mem")
TidierDB.SQLQuery("", "df_mem", "", "", "", "", "", "", false, false, 4×4 DataFrame
 Row │ name     type     current_selxn  table_name 
     │ String?  String?  Int64          String     
─────┼─────────────────────────────────────────────
   1 │ id       VARCHAR              1  df_mem
   2 │ groups   VARCHAR              1  df_mem
   3 │ value    BIGINT               1  df_mem
   4 │ percent  DOUBLE               1  df_mem, false, DuckDB.Connection(":memory:"), TidierDB.CTE[], 0, nothing)
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
                        
julia> copy_to(db, df, "df_mem");

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

julia> copy_to(db, df, "df_mem");                     

julia> @chain db_table(db, :df_mem) begin
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
```julia

julia> df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

julia> db = connect(duckdb());

julia> copy_to(db, df, "df_mem");

julia> df_mem = db_table(db, "df_mem");


julia> @chain t(df_mem) @collect
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
    @union(sql_query1, sql_query2)

Combine two SQL queries using the `UNION` operator.

# Arguments
- `sql_query1`: The first SQL query to combine.
- `sql_query2`: The second SQL query to combine.

# Returns
- A lazy query of all distinct rows in the second query bound to the first

# Examples
```julia
julia> db = connect(duckdb());

julia> df1 = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> df2 = DataFrame(id = [4, 5, 6], value = [40, 50, 60]);

julia> copy_to(db, df1, "df1");

julia> copy_to(db, df2, "df2");

julia> df1_table = db_table(db, "df1");

julia> df2_table = db_table(db, "df2");

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

julia> query = @chain t(df2_table) @filter(value == 50);

julia> @chain t(df1_table) begin 
        @union(t(query))
        @collect
       end
4×2 DataFrame
 Row │ id     value 
     │ Int64  Int64 
─────┼──────────────
   1 │     1     10
   2 │     2     20
   3 │     3     30
   4 │     5     50

julia> @chain t(df1_table) begin 
        @union(t(df1_table))
        @collect
       end
3×2 DataFrame
 Row │ id     value 
     │ Int64  Int64 
─────┼──────────────
   1 │     1     10
   2 │     2     20
   3 │     3     30
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
```julia
julia> db = connect(duckdb());

julia> df1 = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> copy_to(db, df1, "df1");

julia> df1_table = db_table(db, "df1");

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

const docstring_create_view =
"""
    @view(sql_query, name, replace = true)

Create a view from a SQL query. Currently supports DuckDB, MySQL, GBQ, Postgres

# Arguments
- `sql_query`: The SQL query to create a view from.
- `name`: The name of the view to create.
- `replace`: defaults to true if view should be replaced

# Examples
```julia
julia> db = connect(duckdb());

julia> df = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> copy_to(db, df, "df1");

julia> @chain db_table(db, "df1") @create_view(viewer);

julia> db_table(db, "viewer")
TidierDB.SQLQuery("", "viewer", "", "", "", "", "", "", false, false, 2×4 DataFrame
 Row │ name    type    current_selxn  table_name 
     │ String  String  Int64          String     
─────┼───────────────────────────────────────────
   1 │ id      BIGINT              1  viewer
   2 │ value   BIGINT              1  viewer, false, DuckDB.DB(":memory:"), TidierDB.CTE[], 0, nothing, "", "", 0)
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
```julia
julia> db = connect(duckdb());

julia> df = DataFrame(id = [1, 2, 3], value = [10, 20, 30]);

julia> copy_to(db, df, "df1");

julia> @chain db_table(db, "df1") @compute(table2, true);

julia> db_table(db, "table2")
SQLQuery("", "table", "", "", "", "", "", "", false, false, 2×4 DataFrame
 Row │ name    type    current_selxn  table_name 
     │ String  String  Int64          String     
─────┼───────────────────────────────────────────
   1 │ id      BIGINT              1  table2
   2 │ value   BIGINT              1  table2, false, DuckDB.DB(":memory:"), TidierDB.CTE[], 0, nothing, "", "")
```
"""


const docstring_drop_view =
"""
    drop_view(db, name)

Drop a view from a database.

# Arguments
- `db`: The database to drop the view from.
- `name`: The name of the view to drop.

# Examples
`drop_view(db, "viewer")`
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