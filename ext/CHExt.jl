module CHExt

using TidierDB
using DataFrames
import ClickHouse
__init__() = println("Extension was loaded!")

function TidierDB.connect(::clickhouse; kwargs...)
        set_sql_mode(clickhouse())
        if haskey(kwargs, :host) && haskey(kwargs, :port)
            kwargs_filtered = Dict{Symbol, Any}()
            for (k, v) in kwargs
                if k == :user
                    kwargs_filtered[:username] = v
                elseif k ∉ [:host, :port]
                    kwargs_filtered[k] = v
                end
            end
            return ClickHouse.connect(kwargs[:host], kwargs[:port]; kwargs_filtered...)
        else
            throw(ArgumentError("Missing required positional arguments 'host' and 'port' for ClickHouse."))
        end
end


 # ClickHouse
 function TidierDB.get_table_metadata(conn::ClickHouse.ClickHouseSock, table_name::String)
    # Query to get column names and types from INFORMATION_SCHEMA
    query = """
    SELECT
        name AS column_name,
        type AS data_type
    FROM system.columns
    WHERE table = '$table_name' AND database = 'default'
    """
    result = ClickHouse.select_df(conn,query)

    result[!, :current_selxn] .= 1
    result[!, :table_name] .= table_name
    # Adjust the select statement to include the new table_name column
    return select(result, 1 => :name, 2 => :type, :current_selxn, :table_name)
end



function TidierDB.final_collect(sqlquery, ::Type{<:clickhouse})
    final_query = TidierDB.finalize_query(sqlquery)
    df_result = ClickHouse.select_df(sqlquery.db, final_query)
    selected_columns_order = sqlquery.metadata[sqlquery.metadata.current_selxn .== 1, :name]
    df_result = df_result[:, selected_columns_order]
    return df_result
end

end
