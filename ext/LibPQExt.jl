module LibPQExt

using TidierDB
using DataFrames
using LibPQ
__init__() = println("Extension was loaded!")

function TidierDB.connect(::postgres; kwargs...)
        set_sql_mode(postgres())
        conn_str = join(["$(k)=$(v)" for (k, v) in kwargs], " ")
        return LibPQ.Connection(conn_str)
end


function TidierDB.get_table_metadata(conn::LibPQ.Connection, table_name::String)
    if occursin(".", table_name)
        table_name = split(table_name, ".")[2]
    end
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '$table_name'
    ORDER BY ordinal_position;
    """
    result = LibPQ.execute(conn, query) |> DataFrame
    result[!, :current_selxn] .= 1
    result[!, :table_name] .= table_name
    # Adjust the select statement to include the new table_name column
    return select(result, 1 => :name, 2 => :type, :current_selxn, :table_name)
end


function TidierDB.final_collect(sqlquery::SQLQuery, ::Type{<:postgres})
    final_query = TidierDB.finalize_query(sqlquery)
    result = LibPQ.execute(sqlquery.db, final_query)
    return DataFrame(result)
end

function TidierDB.show_tables(con::LibPQ.Connection)
    return DataFrame(LibPQ.execute(con, "SHOW TABLES"))
end

end
