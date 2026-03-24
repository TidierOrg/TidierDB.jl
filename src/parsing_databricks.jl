# COV_EXCL_START
mutable struct DatabricksConnection
    workspace_id::String
    auth_token::String
    database::String
    schema::String
    warehouse_id::String
    api_url::String
end

DATABRICKS_SQL_API_PATH = "/api/2.0/sql/statements"

function databricks_api_request(conn::DatabricksConnection, method::String="POST"; 
    body=nothing, statement_id::Union{Nothing, String}=nothing, next_chunk_link::Union{Nothing, String}=nothing)
    headers = [
        "Authorization" => "Bearer $(conn.auth_token)",
        "Content-Type" => "application/json"
    ]
    if method == "GET"
        if !isnothing(next_chunk_link)
            # remove DATABRICKS_SQL_API_PATH from api_url to use next_chunk_link
            api_url_without_statements = replace(conn.api_url, DATABRICKS_SQL_API_PATH => "")
            req_url = "$(api_url_without_statements)$(next_chunk_link)"
        elseif !isnothing(statement_id)
            req_url = "$(conn.api_url)/$(statement_id)"
        else
            req_url = conn.api_url
        end
        response = HTTP.get(req_url, headers)
    elseif method == "POST"
        response = HTTP.post(conn.api_url, headers, JSON3.write(body))
    # Add other methods as needed
    end
    return JSON3.read(String(response.body))
end

function fetch_external_link(url::String)
    # For external links, do NOT include Authorization header (presigned URL is self-contained)
    response = HTTP.get(url)
    return String(response.body)
end


function get_dataframe_from_result(data, manifest)
    column_names = [col.name for col in manifest.schema.columns]
    transposed_data = permutedims(hcat(data...))
    df = DataFrame(transposed_data, column_names)
    transform!(df, names(df) .=> ByRow(x -> isnothing(x) ? missing : x), renamecols=false)
    for col in names(df)
        if all(x -> ismissing(x) || can_convert_numeric(x), df[!, col])
            df[!, col] = [ismissing(x) ? missing : try_parse_numeric(x) for x in df[!, col]]
        end
    end
    return df
end

function get_dataframe_from_external_links(external_links, manifest)
    # external_links is an array of objects with "url" and "expiration" fields
    # Fetch and parse JSON_ARRAY data from each link
    df = nothing
    
    for (_, link_obj) in enumerate(external_links)
        url = link_obj.external_link        
        # Fetch the JSON_ARRAY data from the presigned URL
        json_string = fetch_external_link(url)
        data_array = JSON3.read(json_string, Vector)
        
        # Create DataFrame from this chunk
        chunk_df = get_dataframe_from_result(data_array, manifest)
        
        if isnothing(df)
            df = chunk_df
        else
            append!(df, chunk_df)
        end
    end
    
    return df
end

function execute_databricks(conn::DatabricksConnection, query::String)
    body = Dict(
        "warehouse_id" => conn.warehouse_id,
        "statement" => query,
        "catalog" => conn.database,
        "schema" => conn.schema,
        "disposition" => "INLINE",
        "format" => "JSON_ARRAY"
    )
    result = databricks_api_request(conn, "POST", body=body)
    if (result.status.state == "PENDING")
        sleep(1)
        statement_id = result.statement_id
        while true
            result = databricks_api_request(conn, "GET", statement_id=statement_id)
            if result.status.state != "PENDING"
            #if result.status.state in ["SUCCEEDED", "FAILED", "CANCELED"]
                break
            else
                sleep(1)
            end
        end
    end
    
    # Handle FAILED state by retrying with EXTERNAL_LINKS disposition
    if result.status.state == "FAILED"
        body["disposition"] = "EXTERNAL_LINKS"
        result = databricks_api_request(conn, "POST", body=body)
        if (result.status.state == "PENDING")
            sleep(1)
            statement_id = result.statement_id
            while true
                result = databricks_api_request(conn, "GET", statement_id=statement_id)
                if result.status.state != "PENDING"
                    break
                else
                    sleep(1)
                end
            end
        end
    end
    
    if result.status.state != "SUCCEEDED"
        error("Query failed with status: $(result.status)")
    end
    
    # Check if we have external_links or inline result
    if haskey(result, :result) && haskey(result.result, :data_array)
        # INLINE disposition result
        df = get_dataframe_from_result(result.result.data_array, result.manifest)
        # Handle pagination if necessary
        if haskey(result.result, "next_chunk_internal_link")
            println("Response was broken in additional chunks...")
            next_chunk_link = result.result.next_chunk_internal_link
            chunk_index = 1
            while !isnothing(next_chunk_link)
                println("Fetching chunk index: $chunk_index")
                chunk_result = databricks_api_request(conn, "GET", next_chunk_link=next_chunk_link)
                # Reuse the original manifest, which is not included in chunk results
                chunk_df = get_dataframe_from_result(chunk_result.data_array, result.manifest)
                append!(df, chunk_df)
                next_chunk_link = haskey(chunk_result, "next_chunk_internal_link") ? chunk_result.next_chunk_internal_link : nothing
                chunk_index += 1
            end
        end
    elseif haskey(result, :result) && haskey(result.result, :external_links)
        # EXTERNAL_LINKS disposition result
        df = get_dataframe_from_external_links(result.result.external_links, result.manifest)
    else
        error("Unexpected response format: no result data or external_links found")
    end
    
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


function show_tables(con::DatabricksConnection)
    result = execute_databricks(con, "SHOW TABLES IN $(con.schema)")
    return DataFrame(result)
end

# COV_EXCL_START