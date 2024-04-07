# To use TidierDB.jl, you will have to set up a connection. TidierDB.jl gives you access to duckdb via `duckdb_open` and `duckdb_connect`. However, to use MySql, ClickHouse, MSSQL, Postgres, or SQLite, you will have to load that packages in first. 

# If you plan to use TidierDB.jl with TidierData.jl or Tidier.jl, it is most convenenient to load the packages as follows:


# - using Tidier # or TidierData
# - import TidierDB as DB.

# Afterwards, all of the TidierDB macros will be available as DB.@mutate and so on, and the TidierData equivalent would be @mutate.

# The associated databased packages  used to set up connections are currently as follows

# - ClickHouse - ClickHouse.jl
# - MySQL - MySQL.jl
# - MSSQL - ODBC.jl 
# - Postgres - LibPQ.jl
# - SQLite - SQLite.jl

# For DuckDB, SQLite, and MySQL, `copy_to` lets you copy data to the database and query there. ClickHouse, MSSQL, and Postgres support for `copy_to` has not been added yet.
