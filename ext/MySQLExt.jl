module MySQLExt

using TidierDB
using DataFrames
using MySQL
__init__() = println("Extension was loaded!")

function TidierDB.connect(::mysql; kwargs...)
        set_sql_mode(mysql())
        # Required parameters by MySQL.jl: host and user
        host = get(kwargs, :host, "localhost")
        user = get(kwargs, :user, "")          
        password = get(kwargs, :password, "")  
        # Extract other optional parameters
        db = get(kwargs, :db, nothing)  
        port = get(kwargs, :port, nothing)     
        return DBInterface.connect(MySQL.Connection, host, user, password; db=db, port=port)
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


function TidierDB.final_collect(sqlquery::SQLQuery, ::Type{<:mysql})
    final_query = TidierDB.finalize_query(sqlquery)
    result = DBInterface.execute(sqlquery.db, final_query)
    return DataFrame(result)
end

function TidierDB.show_tables(conn::MySQL.Connection)
    return DataFrame(DBInterface.execute(conn, "SHOW TABLES"))
end

end
