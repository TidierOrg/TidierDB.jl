module AWSExt

using TidierDB
using DataFrames
using AWS, HTTP, JSON3
__init__() = println("Extension was loaded!")

function collect_athena(result, has_header = true)
    # Extract column names and types from the result set metadata
    column_metadata = result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
    column_names = [col["Label"] for col in column_metadata]
    column_types = [col["Type"] for col in column_metadata]
    num_columns = length(column_names)

    # Process data rows, starting from the second row to skip header information
    start = has_header ? 2 : 1
    data_rows = result["ResultSet"]["Rows"][start:end]

    if isempty(data_rows)
        df = DataFrame([name => String[] for name in Symbol.(column_names)])
        return TidierDB.parse_athena_df(df, column_types)
    end

    # Extract data from each row and handle missing values
    data_matrix = Matrix{Union{String, Missing}}(undef, length(data_rows), num_columns)

    for (row_idx, row) in enumerate(data_rows)
        row_data = row["Data"]
        for col_idx in 1:num_columns
            data_matrix[row_idx, col_idx] = get(row_data[col_idx], "VarCharValue", missing)
        end
    end

    # Create the DataFrame
    df = DataFrame(data_matrix, Symbol.(column_names))
    # Return the DataFrame
    return TidierDB.parse_athena_df(df, column_types)
end

@service Athena

function TidierDB.get_table_metadata(AWS_GLOBAL_CONFIG, table_name::String; athena_params)
    schema, table = split(table_name, '.')  # Ensure this correctly parses your input
    query = """SELECT * FROM $schema.$table limit 0;"""
    exe_query = Athena.start_query_execution(query, athena_params; aws_config = AWS_GLOBAL_CONFIG)

    # Poll Athena to check if the query has completed
    wait_time = 1.0
    status = "RUNNING"
    while status in ["RUNNING", "QUEUED"]
        sleep(round(wait_time))  # Wait for wait_time second before checking the status again to avoid flooding the API
        query_status = Athena.get_query_execution(exe_query["QueryExecutionId"], athena_params; aws_config = AWS_GLOBAL_CONFIG)
        status = query_status["QueryExecution"]["Status"]["State"]
        if status == "FAILED"
            error("Query failed: ", query_status["QueryExecution"]["Status"]["StateChangeReason"])
        elseif status == "CANCELLED"
            error("Query was cancelled.")
        end
        wait_time = min(wait_time * 1.2, 10.0)  # Exponential backoff, max wait time of 10 seconds
    end

    # Fetch the results once the query completes
    result = Athena.get_query_results(exe_query["QueryExecutionId"], athena_params; aws_config = AWS_GLOBAL_CONFIG)

    column_names = [col["Label"] for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    column_types = [col["Type"] for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    df = DataFrame(name = column_names, type = column_types)
    df[!, :current_selxn] .= 1
    df[!, :table_name] .= split(table_name, ".")[2]

    return select(df, 1 => :name, 2 => :type, :current_selxn, :table_name)
end

function TidierDB.final_collect(sqlquery::SQLQuery, ::Type{<:athena})
    final_query = TidierDB.finalize_query(sqlquery)
    exe_query = Athena.start_query_execution(final_query, sqlquery.athena_params; aws_config = sqlquery.db)
    wait_time = 1.0
    status = "RUNNING"
    while status in ["RUNNING", "QUEUED"]
        sleep(round(wait_time))  # Wait for wait_time seconds before checking the status again to avoid flooding the API
        query_status = Athena.get_query_execution(exe_query["QueryExecutionId"], sqlquery.athena_params; aws_config = sqlquery.db)
        status = query_status["QueryExecution"]["Status"]["State"]
        if status == "FAILED"
            error("Query failed: ", query_status["QueryExecution"]["Status"]["StateChangeReason"])
        elseif status == "CANCELLED"
            error("Query was cancelled.")
        end
        wait_time = min(wait_time * 1.2, 10.0)  # Exponential backoff, max wait time of 10 seconds
    end
    dfs = []
    next = true
    params = sqlquery.athena_params
    while next
        result = Athena.get_query_results(exe_query["QueryExecutionId"], params; aws_config = sqlquery.db)
        next = haskey(result, "NextToken")
        params = Dict{String, Any}(mergewith(_merge, next ? Dict("NextToken" => result["NextToken"]) : Dict(), sqlquery.athena_params))
        push!(dfs, collect_athena(result, isempty(dfs)))
    end
    return vcat(dfs...)
end

end
