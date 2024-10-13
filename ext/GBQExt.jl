module GBQExt

using TidierDB
using DataFrames
using GoogleCloud, HTTP, JSON3, Dates
__init__() = println("Extension was loaded!")

include("GBQ_to_DF.jl")

mutable struct GBQ 
    projectname::String
    session::GoogleSession
    bigquery_resource
    bigquery_method
    location::String
end

function TidierDB.connect(::gbq, json_key_path::String, location::String) 
    # Expand the user's path to the JSON key
    creds_path = expanduser(json_key_path)
    set_sql_mode(gbq())
    # Create credentials and session for Google Cloud
    creds = JSONCredentials(creds_path)
    project_id = JSONCredentials(creds_path).project_id
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
    global gbq_instance = GBQ(project_id, session, bigquery_resource, bigquery_method, location)

    # Return only the session
    return session
end

function collect_gbq(conn, query)
    set_sql_mode(gbq());
    query_data = Dict(
    "query" => query,
    "useLegacySql" => false,
    "location" => gbq_instance.location)
    
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

    if !isempty(rows)
        # Return an empty DataFrame with the correct columns but 0 rows
        data = [get(row["f"][i], "v", missing) for row in rows, i in 1:length(column_names)]
        df = DataFrame(data, Symbol.(column_names))
        convert_df_types!(df, column_names, column_types)

        return df
    else
        # Convert each row's data (nested inside dicts with key "v") into arrays of dicts
        df = DataFrame([Vector{Union{Missing, Any}}(undef, 0) for _ in column_names], Symbol.(column_names))
      #  df = TidierDB.parse_gbq_df(df, column_types)
         convert_df_types!(df, column_names, column_types)
        return df
    end

    return df
end

function TidierDB.get_table_metadata(conn::GoogleSession{JSONCredentials}, table_name::String)
    set_sql_mode(gbq());
    query = " SELECT * FROM
    $table_name LIMIT 0
   ;"
    query_data = Dict(
    "query" => query,
    "useLegacySql" => false,
    "location" => gbq_instance.location)
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
    result[!, :table_name] .= split(table_name, ".")[2] 
    return select(result, 1 => :name, 2 => :type, :current_selxn, :table_name)
end



function TidierDB.final_collect(sqlquery::SQLQuery, ::Type{<:gbq})
    final_query = TidierDB.finalize_query(sqlquery)
    return collect_gbq(sqlquery.db, final_query)
end

function TidierDB.final_compute(sqlquery::SQLQuery, ::Type{<:gbq}, sql_cr_or_relace)
    final_query = TidierDB.finalize_query(sqlquery)
    
    final_query = sql_cr_or_relace * final_query
    query_data = Dict(
        "query" => final_query,
        "useLegacySql" => false,
        "location" => gbq_instance.location)
        
    GoogleCloud.api.execute(
        sqlquery.db, 
        gbq_instance.bigquery_resource,  # Use the resource from GBQ
        gbq_instance.bigquery_method, 
        data=query_data
    ) 
    
end

function TidierDB.show_tables(con::GoogleSession{JSONCredentials}, datasetname)
    project_id = gbq_instance.projectname
    query = """
    SELECT table_name
    FROM `$project_id.$datasetname.INFORMATION_SCHEMA.TABLES`
    """
    return collect_gbq(con, query)
end

end
