# To use TidierDB.jl, you will have to set up a connection. TidierDB.jl gives you access to duckdb via `duckdb_open` and `duckdb_connect`. However, to use MySql, ClickHouse, MSSQL, Postgres, or SQLite, you will have to load those packages in first. 

# If you plan to use TidierDB.jl with TidierData.jl or Tidier.jl, it is most convenenient to load the packages as follows:

# ```julia
# using TidierData
# import TidierDB as DB
# ```

# Alternatively, `using Tidier` will import TidierDB in the above manner for you, where TidierDB functions and macros will be available as `DB.@mutate()` and so on, and the TidierData equivalent would be `@mutate()`.

# To connect to a database, you can uset the `connect` function  as shown below, or establish your own connection through the respecitve libraries.

# For example
# Connecting to MySQL
# ```julia
# conn = connect(mysql(); host="localhost", user="root", password="password", db="mydb")
# ```
# versus connecting to DuckDB
# ```julia
# conn = connect(duckdb())
# ```

# ## Package Extensions 
# The following backends utilize package extensions. To use one of backends listed below, you will need to write `using Library`

# - ClickHouse: `using ClickHouse`
# - MySQL and MariaDB: `using MySQL`
# - MSSQL: `using ODBC` 
# - Postgres: `using LibPQ`
# - SQLite: `using SQLite`
# - Athena: `using AWS`
# - Oracle: `using ODBC` 
# - Google BigQuery: `using GoogleCloud`

