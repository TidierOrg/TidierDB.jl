# To use TidierDB.jl, you will have to set up a connection. TidierDB.jl gives you access to duckdb via `duckdb_open` and `duckdb_connect`. However, to use MySql, ClickHouse, MSSQL, Postgres, or SQLite, you will have to load that packages in first. 

# The associated databased packages used to set up connections are currently as follows

# - ClickHouse - ClickHouse.jl
# - MySQL - MySQL.jl
# - MSSQL - ODBC.jl 
# - Postgres - LibPQ.jl
# - SQLite - SQLite.jl

# For DuckDB, SQLite, and MySQL, `copy_to` lets you copy data to the database and query there. ClickHouse, MSSQL, and Postgres support for `copy_to` has not been added yet.

# After the connection is set up and the data is available, TidierDB.jl will take care of the rest of your querying needs.
