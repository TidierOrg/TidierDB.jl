mutable struct DatabricksConnection
    workspace_id::String
    auth_token::String
    database::String
    schema::String
    warehouse_id::String
    api_url::String
end


function databricks_api_request(conn::DatabricksConnection, method::String="POST", body=nothing)
    headers = [
        "Authorization" => "Bearer $(conn.auth_token)",
        "Content-Type" => "application/json"
    ]
    
    if method == "GET"
        response = HTTP.get(conn.api_url, headers)
    elseif method == "POST"
        response = HTTP.post(conn.api_url, headers, JSON3.write(body))
    # Add other methods as needed
    end
    
    return JSON3.read(String(response.body))
end


function execute_databricks(conn::DatabricksConnection, query::String)
    body = Dict(
        "warehouse_id" => conn.warehouse_id,
        "statement" => query,
        "catalog" => conn.database,
        "schema" => conn.schema
    )
    result = databricks_api_request(conn, "POST", body)

    column_names = [col.name for col in result.manifest.schema.columns]
    data = result.result.data_array
    transposed_data = permutedims(hcat(data...))
    df = DataFrame(transposed_data, column_names)
    transform!(df, names(df) .=> ByRow(x -> isnothing(x) ? missing : x), renamecols=false)
    return df
end

function get_table_metadata(conn::DatabricksConnection, table_name::String)
    query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM $(conn.database).INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '$(conn.schema)'
            AND TABLE_NAME = '$table_name'
            ORDER BY ORDINAL_POSITION;
            """
    result = execute_databricks(conn, query)
    result[!, :current_selxn] .= 1
    result[!, :table_name] .= table_name
    # Adjust the select statement to include the new table_name column
    return select(result, 1 => :name, 2 => :type, :current_selxn, :table_name)
end
