module CHExt

using TidierDB
using DataFrames
import ClickHouse
__init__() = println("Extension was loaded!")

function TidierDB.connect(backend::Symbol; kwargs...)
    if backend == :Clickhouse || backend == :clickhouse 
        set_sql_mode(:clickhouse)
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



function TidierDB.final_collect(sqlquery::TidierDB.SQLQuery)
    if TidierDB.current_sql_mode[] == :duckdb || TidierDB.current_sql_mode[] == :lite || TidierDB.current_sql_mode[] == :postgres || TidierDB.current_sql_mode[] == :mysql
        final_query = TidierDB.finalize_query(sqlquery)
        result = DBInterface.execute(sqlquery.db, final_query)
        return DataFrame(result)
    elseif TidierDB.current_sql_mode[] == :clickhouse
        final_query = TidierDB.finalize_query(sqlquery)
        df_result = ClickHouse.select_df(sqlquery.db, final_query)
        selected_columns_order = sqlquery.metadata[sqlquery.metadata.current_selxn .== 1, :name]
        df_result = df_result[:, selected_columns_order]
        return df_result
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
