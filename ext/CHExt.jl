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
    if occursin("/", table_name) || occursin("http", table_name)

     
            query = "DESCRIBE url($table_name)
            SETTINGS enable_url_encoding=0, 
             max_http_get_redirects=10
            "
           # println(query)
            column_info = ClickHouse.select_df(conn, query)
            column_info = select(column_info, :name, :type)
    
        # Prepare the column_info DataFrame
        
        # Add the table name and selection marker
        column_info[!, :current_selxn] .= 1
        table_name = if occursin(r"[:/]", table_name)
            split(basename(table_name), '.')[1]
           #"'$table_name'"
       else
           table_name
       end
        column_info[!, :table_name] .= table_name
        
    else
        # Standard case: Querying from system.columns
        query = """
        SELECT
            name AS column_name,
            type AS data_type
        FROM system.columns
        WHERE table = '$table_name' AND database = 'default'
        """
        column_info = ClickHouse.select_df(conn, query)
        
        # Add the table name and selection marker
        column_info[!, :current_selxn] .= 1
        column_info[!, :table_name] .= table_name
    end
    # Return the result with relevant columns
    return select(column_info, 1 => :name, 2 => :type, :current_selxn, :table_name)
end



function TidierDB.final_collect(sqlquery, ::Type{<:clickhouse})
    final_query = TidierDB.finalize_query(sqlquery)
    df_result = ClickHouse.select_df(sqlquery.db, final_query)
    selected_columns_order = sqlquery.metadata[sqlquery.metadata.current_selxn .== 1, :name]
    df_result = df_result[:, selected_columns_order]
    return df_result
end

end