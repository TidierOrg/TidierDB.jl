module MySQLExt

using TidierDB
using DataFrames
using MySQL
__init__() = println("Extension was loaded!")

function TidierDB.connect(backend::Symbol; kwargs...)
    if backend == :MySQL || backend == :mysql 
        set_sql_mode(:mysql)

        # Required parameters by MySQL.jl: host and user
        host = get(kwargs, :host, "localhost")
        user = get(kwargs, :user, "")          
        password = get(kwargs, :password, "")  
        # Extract other optional parameters
        db = get(kwargs, :db, nothing)  
        port = get(kwargs, :port, nothing)     
        return DBInterface.connect(MySQL.Connection, host, user, password; db=db, port=port)

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


# MySQL
function TidierDB.get_table_metadata(conn::MySQL.Connection, table_name::String)
    # Query to get column names and types from INFORMATION_SCHEMA
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '$table_name'
    AND TABLE_SCHEMA = '$(conn.db)'
    ORDER BY ordinal_position;
    """

    result = DBInterface.execute(conn, query) |> DataFrame
    result[!, 2] = map(x -> String(x), result[!, 2])
    result[!, :current_selxn] .= 1
    result[!, :table_name] .= table_name
    # Adjust the select statement to include the new table_name column
    return DataFrames.select(result, :1 => :name, 2 => :type, :current_selxn, :table_name)
end


function TidierDB.final_collect(sqlquery::TidierDB.SQLQuery)
    if TidierDB.current_sql_mode[] == :duckdb || TidierDB.current_sql_mode[] == :lite || TidierDB.current_sql_mode[] == :postgres || TidierDB.current_sql_mode[] == :mysql || TidierDB.current_sql_mode[] == :mssql  || TidierDB.current_sql_mode[] == :mariadb 
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



end
