# To use TidierDB.jl, you will have to set up a connection. TidierDB.jl gives you access to duckdb via `duckdb_open` and `duckdb_connect`. However, to use MySql, ClickHouse, MSSQL, Postgres, or SQLite, you will have to load those packages in first. 

# If you plan to use TidierDB.jl with TidierData.jl or Tidier.jl, it is most convenenient to load the packages as follows:

# ```julia
# using TidierData
# import TidierDB as DB
# ```

# Alternatively, `using Tidier` will import TidierDB in the above manner for you, where TidierDB functions and macros will be available as `DB.@mutate()` and so on, and the TidierData equivalent would be `@mutate()`.

# ## Connecting
# To connect to a database, you can uset the `connect` function  as shown below, or establish your own connection through the respecitve libraries.

# For example
# Connecting to MySQL
# ```julia
# conn = DB.connect(DB.mysql(); host="localhost", user="root", password="password", db="mydb")
# ```
# versus connecting to DuckDB
# ```julia
# conn = DB.connect(DB.duckdb())
# ```

# You can also use establish a connection through an alternate method that you preferred, and use that as your connection as well. 

# ## Package Extensions 
# The following backends utilize package extensions. To use one of backends listed below, you will need to write `using Library`

# - ClickHouse: `import ClickHouse`
# - MySQL and MariaDB: `using MySQL`
# - MSSQL: `using ODBC` 
# - Postgres: `using LibPQ`
# - SQLite: `using SQLite`
# - Athena: `using AWS`
# - Oracle: `using ODBC` 
# - Google BigQuery: `using GoogleCloud`

# ## `db_table`
# What does `db_table` do? 

# `db_table` starts the underlying SQL query struct, in addition to pulling the table metadata and storing it there. Storing metadata is what enables a lazy interface that also supports tidy selection.  
# - `db_table` has two required arguments: `connection` and `table`
# - `table` can be a table name on a database or a path/url to file to read.  When passing `db_table` a path or url, the table is not copied into memory.
# - With DuckDB and ClickHouse, if you have a folder of multiple files to read, you can use `*` read in all files matching the pattern. 
# - For example, the below would read all files that end in `.csv` in the given folder.
# ```julia
# db_table(db, "folder/path/*.csv")
# ``` 
# `db_table` also supports iceberg, delta, and S3 file paths via DuckDB.

# ## Minimizing Compute Costs
# If you are working with a backend where compute cost is important, it will be important to minimize using `db_table` as this will requery for metadata each time. 
# Compute costs are relevant to backends such as AWS, databricks and Snowflake. 

# To do this, save the results of `db_table` and use them with `from_query`. Using `from_query` pulls the relevant information (metadata, con, etc) from the mutable SQLquery struct, allowing you to repeatedly query and collect the table without requerying for the metadata each time
# ```julia
# table = DB.db_table(con, "path")
# @chain DB.from_query(table) begin
#     ## data wrangling here 
# end 
# ```
