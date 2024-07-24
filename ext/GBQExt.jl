module GBQExt

using TidierDB
using DataFrames
using GoogleCloud, HTTP, JSON3
__init__() = println("Extension was loaded!")

mutable struct GBQ 
    projectname::String
    session::GoogleSession
    bigquery_resource
    bigquery_method
end

function TidierDB.connect(type::Symbol, json_key_path::String, project_id::String) 
    # Expand the user's path to the JSON key
    creds_path = expanduser(json_key_path)
    set_sql_mode(:gbq)
    # Create credentials and session for Google Cloud
    creds = JSONCredentials(creds_path)
    session = GoogleSession(creds, ["https://www.googleapis.com/auth/bigquery"])

    # Define the API method for BigQuery
    bigquery_method = GoogleCloud.api.APIMethod(
        :POST, 
        "https://bigquery.googleapis.com/bigquery/v2/projects/$(project_id)/queries",
        "Run query", 
        Dict{Symbol, Any}();
        transform=(x, t) -> x
    )

    # Define the API resource for BigQuery
    bigquery_resource = GoogleCloud.api.APIResource(
        "https://bigquery.googleapis.com/bigquery/v2",
        ;query=bigquery_method  # Pass the method as a named argument
    )

    # Store all data in a global GBQ instance
    global gbq_instance = GBQ(project_id, session, bigquery_resource, bigquery_method)

    # Return only the session
    return session
end

function collect_gbq(conn, query)
    query_data = Dict(
    "query" => query,
    "useLegacySql" => false,
    "location" => "US")
    
    response = GoogleCloud.api.execute(
        conn, 
        gbq_instance.bigquery_resource,  # Use the resource from GBQ
        gbq_instance.bigquery_method, 
        data=query_data
    ) 
    response_string = String(response)
    response_data = JSON3.read(response_string)
    rows = get(response_data, "rows", [])
    
    # Convert rows to DataFrame
    # First, extract column names from the schema
    column_names = [field["name"] for field in response_data["schema"]["fields"]]
    column_types = [field["type"] for field in response_data["schema"]["fields"]]
    # Then, convert each row's data (currently nested inside dicts with key "v") into arrays of dicts
    if !isempty(rows)
        # Return an empty DataFrame with the correct columns but 0 rows
        data = [get(row["f"][i], "v", missing) for row in rows, i in 1:length(column_names)]
        df = DataFrame(data, Symbol.(column_names))
        df = TidierDB.parse_gbq_df(df, column_types)
        return df
    else
        # Convert each row's data (nested inside dicts with key "v") into arrays of dicts
        df =DataFrame([Vector{Union{Missing, Any}}(undef, 0) for _ in column_names], Symbol.(column_names))
        df = TidierDB.parse_gbq_df(df, column_types)
        return df
    end

    return df
end
function TidierDB.get_table_metadata(conn::GoogleSession{JSONCredentials}, table_name::String)
    query = " SELECT * FROM
    $table_name LIMIT 0
   ;"
    query_data = Dict(
    "query" => query,
    "useLegacySql" => false,
    "location" => "US")
    # Define the API resource

    response = GoogleCloud.api.execute(
        conn, 
        gbq_instance.bigquery_resource, 
        gbq_instance.bigquery_method, 
        data=query_data
    ) 
    response_string = String(response)
    response_data = JSON3.read(response_string)
    column_names = [field["name"] for field in response_data["schema"]["fields"]]
    column_types = [field["type"] for field in response_data["schema"]["fields"]]
    result = DataFrame(name = column_names, type = column_types)
    result[!, :current_selxn] .= 1
    result[!, :table_name] .= table_name

    return select(result, 1 => :name, 2 => :type, :current_selxn, :table_name)
end


function TidierDB.final_collect(sqlquery::TidierDB.SQLQuery)
    if TidierDB.current_sql_mode[] == :duckdb || TidierDB.current_sql_mode[] == :lite || TidierDB.current_sql_mode[] == :postgres || TidierDB.current_sql_mode[] == :mysql
        final_query = TidierDB.finalize_query(sqlquery)
        result = DBInterface.execute(sqlquery.db, final_query)
        return DataFrame(result)
    elseif TidierDB.current_sql_mode[] == :gbq
        final_query = TidierDB.finalize_query(sqlquery)
        return collect_gbq(sqlquery.db, final_query)
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
