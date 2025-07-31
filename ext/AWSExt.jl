module AWSExt

using TidierDB
using DataFrames
using AWS, HTTP, JSON3
__init__() = println("Extension was loaded!")

function collect_athena(result, has_header = true)
    # Extract column names and types from the result set metadata
    column_names = [col["Label"] for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    column_types = [col["Type"] for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]

    # Process data rows, starting from the second row to skip header information
    data_rows = result["ResultSet"]["Rows"]
    filtered_column_names = filter(c -> !isempty(c), column_names)
    num_columns = length(filtered_column_names)

    has_header ? start = 2 : start = 1
    data_for_df = [
        [get(col, "VarCharValue", missing) for col in row["Data"]] for row in data_rows[start:end]
    ]

    # Ensure each row has the correct number of elements
    adjusted_data_for_df = [
        length(row) == num_columns ? row : resize!(copy(row), num_columns) for row in data_for_df
    ]

    # Pad rows with missing values if they are shorter than expected
    for row in adjusted_data_for_df
        if length(row) < num_columns
            append!(row, fill(missing, num_columns - length(row)))
        end
    end

    # Transpose the data to match DataFrame constructor requirements
    data_transposed = permutedims(hcat(adjusted_data_for_df...))

    # Create the DataFrame
    df = DataFrame(data_transposed, Symbol.(filtered_column_names))
    TidierDB.parse_athena_df(df, column_types)
    # Return the DataFrame
    return df
end

@service Athena

function TidierDB.get_table_metadata(AWS_GLOBAL_CONFIG, table_name::String; athena_params)
    schema, table = split(table_name, '.')  # Ensure this correctly parses your input
    query = """SELECT * FROM $schema.$table limit 0;"""
  #  println(query)
  #  try
        exe_query = Athena.start_query_execution(query, athena_params; aws_config = AWS_GLOBAL_CONFIG)

        # Poll Athena to check if the query has completed
        status = "RUNNING"
        while status in ["RUNNING", "QUEUED"]
            sleep(1)  # Wait for 1 second before checking the status again to avoid flooding the API
            query_status = Athena.get_query_execution(exe_query["QueryExecutionId"], athena_params; aws_config = AWS_GLOBAL_CONFIG)
            status = query_status["QueryExecution"]["Status"]["State"]
            if status == "FAILED"
                error("Query failed: ", query_status["QueryExecution"]["Status"]["StateChangeReason"])
            elseif status == "CANCELLED"
                error("Query was cancelled.")
            end
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
    status = "RUNNING"
    while status in ["RUNNING", "QUEUED"]
        sleep(1)  # Wait for 1 second before checking the status again to avoid flooding the API
        query_status = Athena.get_query_execution(exe_query["QueryExecutionId"], sqlquery.athena_params; aws_config = sqlquery.db)
        status = query_status["QueryExecution"]["Status"]["State"]
        if status == "FAILED"
            error("Query failed: ", query_status["QueryExecution"]["Status"]["StateChangeReason"])
        elseif status == "CANCELLED"
            error("Query was cancelled.")
        end
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
