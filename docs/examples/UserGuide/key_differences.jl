# There are a few important syntax and behavior differences between TidierDB.jl and TidierData.jl outlined below. 

# ## Creating a database

# For these examples we will use DuckDB, the default backend, although SQLite, Postgres, MySQL, MariaDB, MSSQL, and ClickHouse are possible. If you have an existing DuckDB connection, then this step is not required. For these examples, we will create a data frame and copy it to an in-memory DuckDB database.

using DataFrames, TidierDB

df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

db = connect(duckdb());

copy_to(db, df, "df_mem"); # copying over the data frame to an in-memory database

# ## Row ordering

# DuckDB benefits from aggressive parallelization of pipelines. This means that if you have multiple threads enabled in Julia, which you can check or set using `Threads.nthreads()`, DuckDB will use multiple threads. However, because many operations are multi-threaded, the resulting row order is inconsistent. If row order needs to be deterministic for your use case, make sure to apply an `@arrange(column_name_1, column_name_2, etc...)` prior to collecting the results.  

# ## Starting a chain

# When using TidierDB, `db_table(connection, :table_name)` is used to start a chain.

# ## Grouped mutation

# In TidierDB, when performing `@group_by` then `@mutate`, the table will be ungrouped after applying all of the mutations in the clause to the grouped data. To perform subsequent grouped operations, the user would have to regroup the data. This is demonstrated below.


@chain db_table(db, :df_mem) begin
    @group_by(groups)
    @summarize(mean_percent = mean(percent))
    @collect
 end

# Regrouping following `@mutate`

@chain db_table(db, :df_mem) begin
    @group_by(groups)
    @mutate(max = maximum(percent), min = minimum(percent))
    @group_by(groups)
    @summarise(mean_percent = mean(percent))
    @collect
end

# ## Joining

# There is one key difference for joining:

# The column on both the new and old table must be specified. They do not need to be the same, and given SQL behavior where both columns are kept when joining two tables, it is preferable if they have different names. This avoids "ambiguous reference" errors that would otherwise come up and complicate the use of tidy selection for columns. 
# If the table that is being newly joined exists on a database, it must be written as a string or Symbol. If it is an exisiting query, it must be wrapped with `t(query)`. Visit the docstrings for more examples. 
df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
                score = [88, 92, 77, 83, 95, 68, 74]);

copy_to(db, df2, "df_join");

@chain db_table(db, :df_mem) begin
    @left_join("df_join", id2, id)
    @collect
end

# ## Differences in `case_when()`

# In TidierDB, after the clause is completed, the result for the new column should is separated by a comma `,`
# in contrast to TidierData.jl, where the result for the new column is separated by a `=>` .

@chain db_table(db, :df_mem) begin
    @mutate(new_col = case_when(percent > .5, "Pass",  # in TidierData, percent > .5 => "Pass", 
                                percent <= .5, "Try Again", # percent <= .5 => "Try Again"
                                true, "middle"))
    @collect
 end

# ## Interpolation

# To use !! Interpolation, instead of being able to define the alternate names/value in the global context, the user has to use `@interpolate`. This will hopefully be fixed in future versions. Otherwise, the behavior is generally the same, although this creates friction around calling functions.

# Also, when using interpolation with exponenents, the interpolated value must go inside of parenthesis. 
# ```julia
# @interpolate((test, :percent)); # this still supports strings, vectors of names, and values

# @chain db_table(db, :df_mem) begin
#     @mutate(new_col = case_when((!!test)^2 > .5, "Pass",
#                                 (!!test)^2 < .5, "Try Again",
#                                 "middle"))
#     @collect
# end
# ```
# ```
# 10×5 DataFrame
#  Row │ id       groups   value   percent   new_col   
#      │ String?  String?  Int64?  Float64?  String?   
# ─────┼───────────────────────────────────────────────
#    1 │ AA       bb            1       0.1  Try Again
#    2 │ AB       aa            2       0.2  Try Again
#    3 │ AC       bb            3       0.3  Try Again
#   ⋮  │    ⋮        ⋮       ⋮        ⋮          ⋮
#    8 │ AH       aa            3       0.8  Pass
#    9 │ AI       bb            4       0.9  Pass
#   10 │ AJ       aa            5       1.0  Pass
#                                        4 rows omitted
# ```
# ## Slicing ties

# `slice_min()` and `@slice_max()` will always return ties due to SQL behavior.
