module GBQExt

using TidierDB
using DataFrames
using GoogleCloud, HTTP, JSON3, Dates
__init__() = println("Extension was loaded!")

mutable struct GBQ 
    projectname::String
    session::GoogleSession
    bigquery_resource
    bigquery_method
    location::String
end


function apply_type_conversion_gbq(df, col_index, col_type)

end

function parse_gbq_df(df, column_types)
    for (i, col_type) in enumerate(column_types)
        # Check if column index is within bounds of DataFrame columns
        if i <= size(df, 2)
            try
                apply_type_conversion_gbq(df, i, col_type)
            catch e
               # @warn "Failed to convert column $(i) to $(col_type): $e"
            end
        else
           # @warn "Column index $(i) is out of bounds for the current DataFrame."
        end
    end;
    return df
end

type_map = Dict(
    "STRING"  => String,
    "FLOAT"   => Float64,
    "INTEGER" => Int64,
    "DATE"    => Date,
    "DATETIME" => DateTime,
    "ARRAY" => Array,
    "STRUCT" => Struct    
)

function convert_df_types!(df::DataFrame, new_names::Vector{String}, new_types::Vector{String})
    for (name, type_str) in zip(new_names, new_types)
        if haskey(type_map, type_str)
            # Get the corresponding Julia type
            target_type = type_map[type_str]
            
            # Check if the DataFrame has the column
            if hasproperty(df, name)
                # Convert the column to the target type
                if target_type == Float64
                    df[!, name] = [x === nothing || ismissing(x) ? missing : parse(Float64, x) for x in df[!, name]]
                elseif target_type == Int64
                    df[!, name] = [x === nothing || ismissing(x) ? missing : parse(Int64, x) for x in df[!, name]]
                elseif target_type == Date
                    df[!, name] = [x === nothing || ismissing(x) ? missing : Date(x) for x in df[!, name]]
                else
                    df[!, name] = convert.(target_type, df[!, name])
                end
            else
                println("Warning: Column $name not found in DataFrame.")
            end
        else
            println("Warning: Type $type_str is not recognized.")
        end
    end
    return df
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
   # println(column_names)
    column_types = [field["type"] for field in response_data["schema"]["fields"]]
   # println(column_types)
    # Then, convert each row's data (currently nested inside dicts with key "v") into arrays of dicts
    if !isempty(rows)
        # Return an empty DataFrame with the correct columns but 0 rows
        data = [get(row["f"][i], "v", missing) for row in rows, i in 1:length(column_names)]
        df = DataFrame(data, Symbol.(column_names))
     #   df = TidierDB.parse_gbq_df(df, column_types)
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

function TidierDB.show_tables(con::GoogleSession{JSONCredentials}, project_id, datasetname)
    query = """
    SELECT table_name
    FROM `$project_id.$datasetname.INFORMATION_SCHEMA.TABLES`
    """
    return collect_gbq(con, query)
end

end
