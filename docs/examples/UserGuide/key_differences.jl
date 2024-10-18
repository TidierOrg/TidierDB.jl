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

# ## Differences in `case_when()`

# In TidierDB, after the clause is completed, the result for the new column should is separated by a comma `,`
# in contrast to TidierData.jl, where the result for the new column is separated by a `=>` .

@chain db_table(db, :df_mem) begin
    @mutate(new_col = case_when(percent > .5, "Pass",  # in TidierData, percent > .5 => "Pass", 
                                percent <= .5, "Try Again", # percent <= .5 => "Try Again"
                                true, "middle"))
    @collect
 end

 # ## Joining Tables
 # When joining a table, the column from both tables will be present, in contrast to TidierData which will keep one column