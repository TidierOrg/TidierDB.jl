# There are a few miscellaneous feautures of TidierDB that are documented on this page 

using DataFrames, TidierDB

df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

db = connect(duckdb());

dfv = db_table(db, df, "dfv");

# ## DuckDB's SUMMARIZE
# DuckDB has a feature tosummarize tables that gives information about the table, such as mean, std, q25, q75 etc.
# To use this feature with TidierDB, simply call an empty `@summarize`. 
@chain t(dfv) @summarize() @collect

# ## show_query/collect
# If you find yourself frequently showing a query while collecting, you can define the following function 
sqc(qry) = @chain t(qry) begin
                @aside @show_query _
                @collect()
            end;

# Call this function at the end of a chain similar the `@show_query` or`@collect` macros
@chain t(dfv) @summarize() sqc()
