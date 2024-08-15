module SQLiteExt

using TidierDB
using DataFrames
using SQLite
__init__() = println("Extension was loaded!")

function TidierDB.connect(::sqlite; kwargs...)
        db_path = get(kwargs, :db, ":memory:") 
        set_sql_mode(sqlite())
        return SQLite.DB(db_path)
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
function TidierDB.final_collect(sqlquery::SQLQuery, ::Type{<:sqlite})
    final_query = TidierDB.finalize_query(sqlquery)
    result = DBInterface.execute(sqlquery.db, final_query)
    return DataFrame(result)
end

function TidierDB.show_tables(con::SQLite.DB)
    return DataFrame(DBInterface.execute(con, "SELECT name 
                                                FROM sqlite_master 
                                                WHERE type = 'table' 
                                                ORDER BY name;
                                                "))
end

end
