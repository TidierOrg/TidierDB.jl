# To use TidierDB.jl, you will have to set up a connection. TidierDB.jl gives you access to duckdb via `duckdb_open` and `duckdb_connect`. However, to use MySql, ClickHouse, MSSQL, Postgres, or SQLite, you will have to load those packages in first. 

# If you plan to use TidierDB.jl with TidierData.jl or Tidier.jl, it is most convenenient to load the packages as follows:

# ```julia
# using TidierData
# import TidierDB as DB
# ```

# Alternatively, `using Tidier` will import TidierDB in the above manner for you, where TidierDB functions and macros will be available as `DB.@mutate()` and so on, and the TidierData equivalent would be `@mutate()`.

# There are two ways to connect to the database.  you can use `connect` without any need to load any additional packages.

# For example
# Connecting to MySQL
# ```julia
# conn = connect(:mysql; host="localhost", user="root", password="password", db="mydb")
# ```
# versus connecting to DuckDB
# ```julia
# conn = connect(:duckdb)
# ```

# Alternatively, you can use the packages outlined below to establish a connection through their respective methods.

# - ClickHouse: ClickHouse.jl
# - MySQL and MariaDB: MySQL.jl
# - MSSQL: ODBC.jl 
# - Postgres: LibPQ.jl
# - SQLite: SQLite.jl

# For DuckDB, SQLite, and MySQL, `copy_to()` lets you copy data to the database and query there. ClickHouse, MSSQL, and Postgres support for `copy_to()` has not been added yet.
