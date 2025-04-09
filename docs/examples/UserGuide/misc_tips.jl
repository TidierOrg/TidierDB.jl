# There are a few miscellaneous feautures of TidierDB that are documented on this page 

using DataFrames, TidierDB

df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

db = connect(duckdb());

dfv = dt(db, df, "dfv");

# ## DuckDB's SUMMARIZE
# DuckDB has a feature tosummarize tables that gives information about the table, such as mean, std, q25, q75 etc.
# To use this feature with TidierDB, simply call an empty `@summarize`. 
@chain dfv @summarize() @collect

# ## show_query/collect
# If you find yourself frequently showing a query while collecting, you can define the following function 
sqc(qry) = @chain qry begin
                @aside @show_query _
                @collect()
            end;

# Call this function at the end of a chain similar the `@show_query` or`@collect` macros
# _printed query is not seen here as it prints to the REPL_
#@chain dfv @summarize() sqc()

# ## Color Printing
# Queries print with some code words in color to the REPL. To turn off this feature, run one of the following.
#   - `TidierDB.color[] = false`
#   - `DB.color[] = false`