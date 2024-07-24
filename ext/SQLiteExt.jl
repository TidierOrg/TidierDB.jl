module SQLiteExt

using TidierDB
using DataFrames
using SQLite
__init__() = println("Extension was loaded!")

function TidierDB.connect(backend::Symbol; kwargs...)
    if backend == :SQLite || backend == :lite
        db_path = get(kwargs, :db, ":memory:") 
        set_sql_mode(:lite)
        return SQLite.DB(db_path)
    elseif backend == :DuckDB || backend == :duckdb
        set_sql_mode(:duckdb)
        db = DBInterface.connect(DuckDB.DB, ":memory:")
        DBInterface.execute(db, "SET autoinstall_known_extensions=1;")
        DBInterface.execute(db, "SET autoload_known_extensions=1;")
    
        # Install and load the httpfs extension
        DBInterface.execute(db, "INSTALL httpfs;")
        DBInterface.execute(db, "LOAD httpfs;")
        return db
    else
        throw(ArgumentError("Unsupported backend: $backend"))
    end
end



function TidierDB.get_table_metadata(db::SQLite.DB, table_name::String)
    query = "PRAGMA table_info($table_name);"
    result = SQLite.DBInterface.execute(db, query) |> DataFrame
    result[!, :current_selxn] .= 1
    resize!(result.current_selxn, nrow(result))
    result[!, :table_name] .= table_name
    # Adjust the select statement to include the new table_name column
    return DataFrames.select(result, 2 => :name, 3 => :type, :current_selxn, :table_name)
end

function TidierDB.copy_to(conn::SQLite.DB, df::DataFrame, name::String)
    SQLite.load!(df, conn, name)
end


# In SQLiteExt.jl
function TidierDB.final_collect(sqlquery::TidierDB.SQLQuery)
    if TidierDB.current_sql_mode[] == :duckdb || TidierDB.current_sql_mode[] == :lite || TidierDB.current_sql_mode[] == :postgres || TidierDB.current_sql_mode[] == :mysql || TidierDB.current_sql_mode[] == :mssql 
        final_query = TidierDB.finalize_query(sqlquery)
        result = DBInterface.execute(sqlquery.db, final_query)
        return DataFrame(result)
    elseif TidierDB.current_sql_mode[] == :snowflake
        final_query = TidierDB.finalize_query(sqlquery)
        result = TidierDB.execute_snowflake(sqlquery.db, final_query)
        return DataFrame(result)
    elseif TidierDB.current_sql_mode[] == :databricks
        final_query = TidierDB.finalize_query(sqlquery)
        result = TidierDB.execute_databricks(sqlquery.db, final_query)
        return DataFrame(result)
    end
end

# In DuckDBExt.jl




end
