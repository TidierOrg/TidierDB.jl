module ODBCExt

using TidierDB
using DataFrames
using ODBC
__init__() = println("Extension was loaded!")


# MSSQL
function TidierDB.get_table_metadata(conn::ODBC.Connection, table_name::String)
    if TidierDB.current_sql_mode[] == oracle()
        set_sql_mode(oracle());
        table_name = uppercase(table_name)
        query = """
        SELECT column_name, data_type
        FROM all_tab_columns
        WHERE table_name = '$table_name'
        ORDER BY column_id
        """
    else
        set_sql_mode(mssql());
        query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '$table_name'
        ORDER BY ordinal_position;
        """
    end

    result = DBInterface.execute(conn, query) |> DataFrame
    result[!, :current_selxn] .= 1
    result[!, :table_name] .= table_name
    # Adjust the select statement to include the new table_name column
    return select(result, :column_name => :name, :data_type => :type, :current_selxn, :table_name)
end


function TidierDB.final_collect(sqlquery::SQLQuery, ::Type{<:mssql})
    final_query = TidierDB.finalize_query(sqlquery)
    result = DBInterface.execute(sqlquery.db, final_query)
    return DataFrame(result)
end

function TidierDB.final_collect(sqlquery::SQLQuery, ::Type{<:oracle})
    final_query = TidierDB.finalize_query(sqlquery)
    result = DBInterface.execute(sqlquery.db, final_query)
    return DataFrame(result)
end

function TidierDB.show_tables(con::ODBC.Connection)
    return DataFrame(DBInterface.execute(con, "SELECT name 
                                               FROM sys.tables;"))
end

function TidierDB.connect(::mssql, connection_string::String)
    set_sql_mode(mssql())
    return ODBC.Connection(connection_string)
end

end