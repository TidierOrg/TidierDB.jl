# There are a few important syntax and behavior differences between TidierDB.jl and TidierData.jl outlined below. 

# ## Creating a database

# For these examples we will use DuckDB, the default backend. If you have an existing DuckDB connection, then this step is not required. For these examples, we will create a data frame and copy it to an in-memory DuckDB database.

using DataFrames, TidierDB

df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

db = connect(duckdb());

dfv = db_table(db, df, "dfv"); # create a view (not a copy) of the dataframe on a in-memory database

# ## Row ordering

# DuckDB benefits from aggressive parallelization of pipelines. This means that if you have multiple threads enabled in Julia, which you can check or set using `Threads.nthreads()`, DuckDB will use multiple threads. However, because many operations are multi-threaded, the resulting row order is inconsistent. If row order needs to be deterministic for your use case, make sure to apply an `@arrange(column_name_1, column_name_2, etc...)` prior to collecting the results.  

# ## Starting a chain

# When using TidierDB, `db_table(connection, :table_name)` is used to start a chain.

# ## Grouped mutation

# In TidierDB, when performing `@group_by` then `@mutate`, the table will be ungrouped after applying all of the mutations in the clause to the grouped data. To perform subsequent grouped operations, the user would have to regroup the data. This is demonstrated below.

@chain t(dfv) begin
    @group_by(groups)
    @mutate(mean_percent = mean(percent))
    @collect
 end

# Regrouping following `@mutate`

@chain t(dfv) begin
    @group_by(groups)
    @mutate(max = maximum(percent), min = minimum(percent))
    @group_by(groups)
    @summarise(mean_percent = mean(percent))
    @collect
end

# TidierDB also supports `_by` for grouping directly within a mutate clause (a feature coming to TidierData in the the future)

@chain t(dfv) begin
    @mutate(mean_percent = mean(percent),
        _by = groups)
    @collect
 end

# ## Window Functions

# SQL and TidierDB allow for the use of window functions. When ordering a window function, `@arrange` should not be used. Rather, `@window_order` or, preferably, `_order` (and `_frame`) in `@mutate` should be used.
# The following window functions are included by default
#     - `lead`, `lag`, `dense_rank`, `nth_value`, `ntile`, `rank_dense`, `row_number`, `first_value`, `last_value`, `cume_dist`
# The following aggregate functions are included by default
#     - `maximum`, `minimum`, `mean`, `std`, `sum`, `cumsum`
# Window and aggregate functions not listed in the above can be either wrapped in `agg(kurtosis(column))` or added to an internal vector using 
#     - `push!(TidierDB.window_agg_fxns, :kurtosis);`
@chain t(dfv) begin
    @mutate(row_id = row_number(), 
        _by = groups, 
        _order = value
        # _frame is an available argument as well. 
        )
    @arrange(groups, value)
    @aside @show_query _
    @collect
end 

# The above query could have alternatively been written as 
 @chain t(dfv) begin
    @group_by groups
    @window_order value
    @mutate(row_id = row_number())
    @arrange(groups, value)
    @collect
end 

# ## Differences in `case_when()`

# In TidierDB, after the clause is completed, the result for the new column should is separated by a comma `,`
# in contrast to TidierData.jl, where the result for the new column is separated by a `=>` .

@chain t(dfv) begin
    @mutate(new_col = case_when(percent > .5, "Pass",  # in TidierData, percent > .5 => "Pass", 
                                percent <= .5, "Try Again", # percent <= .5 => "Try Again"
                                true, "middle"))
    @collect
 end
