# Leveraging DuckDB, TidierDB works with multiple file types. 
# - 
# - 
# -

# ## `db_table` and `dt`
# What does `dt` do? (`dt` is an alias for `db_table`)

# `dt` starts the underlying SQL query struct, in addition to pulling the table metadata and storing it there. Storing metadata is what enables a lazy interface that also supports tidy selection.  
# - `dt` has two required arguments: `connection` and `table`
# - `table` can be a table name on a database or a path/url to file to read.  When passing `db_table` a path or url, the table is not copied into memory.
#   - Of note, `dt` only support direct file paths to a table. It does not support database file paths such as `dbname.duckdb` or `dbname.sqlite`. Such files must be used with `connect` first.
# - With DuckDB and ClickHouse, if you have a folder of multiple files to read, you can use `*` read in all files matching the pattern, with an optional `alias` argument for what the data should be referred to.
# - For example, the below would read all files that end in `.csv` in the given folder.
# ```
# dt(db, "folder/path/*.csv")
# ``` 
# `databricks` also supports iceberg, delta, and S3 file paths via DuckDB.

# ## Minimizing Compute Costs and Keystrokes
# If you are working with a backend where compute cost is important, it will be important to minimize using `dt` as this will requery for metadata each time. 
# Compute costs are relevant to backends such as AWS, databricks and Snowflake. 

# To do this, save the results of `dt` and use them with `t`. Using `t` pulls the relevant information (metadata, con, etc) from the mutable SQLquery struct, allowing you to repeatedly query and collect the table without requerying for the metadata each time
# > !Tip: 
# > `t()` is an alias for `from_query` This means after saving the results of `dt`, use `t(table)` to refer to the table or prior query 
# ```julia
# table = DB.dt(con, "path")
# @chain DB.t(table) begin
#     ## data wrangling here 
# end 
# ```