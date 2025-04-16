# If you plan to use TidierDB.jl with TidierData.jl or Tidier.jl, it is most convenenient to load the packages as follows:

# ```julia
# using TidierData
# import TidierDB as DB
# ```

# Alternatively, `using Tidier` will import TidierDB in the above manner for you, where TidierDB functions and macros will be available as `DB.@mutate()` and so on, and the TidierData equivalent would be `@mutate()`.

# ## Connecting
# To use TidierDB, a connection must first be established. To connect to a database, you can uset the `connect` function  as shown below, or establish your own connection through the respecitve libraries.

# For example
# Connecting to DuckDB
# ```julia
# conn = DB.connect(DB.duckdb())
# ```
# versus connecting to MySQL
# ```julia
# conn = DB.connect(DB.mysql(); host="localhost", user="root", password="password", db="mydb")
# ```
# The [`connect` docstring](https://tidierorg.github.io/TidierDB.jl/latest/reference/#TidierDB.connect-Tuple{duckdb}) has many examples for how to use the connect function to connect to various backends or to MotherDuck. 

# ## Connect to a local database file with DuckDB
# You can also connect to an existing database by passing the database file path as a string.
# ```julia
# db = DB.connect(DB.duckdb(), "path/to/mydb.duckdb")
# ```

# You can also establish any DuckDB connection through an alternate method that you prefer, and use that as your connection as well. 

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
# - Google Sheets via DuckDB: run the following with your db `ghseet_connect(db)`, copy key and paste back in terminal. Then paste the entire google sheets link as your table name in `db_table`

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
# Compute costs are relevant to backends such as AWS, databricks and Snowflake. It is best practice to save the results of `dt` or `db_table` and work w that object