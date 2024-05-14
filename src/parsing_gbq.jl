
mutable struct GBQ 
    projectname::String
    session::GoogleSession
    bigquery_resource
    bigquery_method
end

function connect(type::Symbol, json_key_path::String, project_id::String) 
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
        df = parse_gbq_df(df, column_types)
        return df
    else
        # Convert each row's data (nested inside dicts with key "v") into arrays of dicts
        df =DataFrame([Vector{Union{Missing, Any}}(undef, 0) for _ in column_names], Symbol.(column_names))
        df = parse_gbq_df(df, column_types)
        return df
    end

    return df
end


function apply_type_conversion_gbq(df, col_index, col_type)
    if col_type == "FLOAT"
        df[!, col_index] = [ismissing(x) ? missing : parse(Float64, x) for x in df[!, col_index]]
    elseif col_type == "INTEGER"
        df[!, col_index] = [ismissing(x) ? missing : parse(Int, x) for x in df[!, col_index]]
    elseif col_type == "STRING"
        # Assuming varchar needs to stay as String, no conversion needed
    end
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


function expr_to_sql_gbq(expr, sq; from_summarize::Bool)
    expr = parse_char_matching(expr)
    expr = exc_capture_bug(expr, names_to_modify)
    MacroTools.postwalk(expr) do x
        # Handle basic arithmetic and functions
        if @capture(x, a_ + b_)
            return :($a + $b)
        elseif @capture(x, a_ - b_)
            return :($a - $b)
        elseif @capture(x, a_ * b_)
            return :($a * $b)
        elseif @capture(x, a_ / b_)
            return :($a / $b)
        elseif @capture(x, a_ ^ b_)
            return :(POWER($a, $b))
        elseif @capture(x, round(a_))
            return :(ROUND($a))
        elseif @capture(x, round(a_, b_))
            return :(ROUND($a, $b))
        elseif @capture(x, mean(a_))
            if from_summarize
                return :(AVG($a))
            else
                window_clause = construct_window_clause(sq)
                return  "AVG($(string(a))) $(window_clause)"
            end
        elseif @capture(x, minimum(a_))
            if from_summarize
                return :(MIN($a))
            else
                window_clause = construct_window_clause(sq)
                return  "MIN($(string(a))) $(window_clause)"
            end
        elseif @capture(x, maximum(a_))
            if from_summarize
                return :(MAX($a))
            else
                window_clause = construct_window_clause(sq)
                return  "MAX($(string(a))) $(window_clause)"
            end
        elseif @capture(x, sum(a_))
            if from_summarize
                return :(SUM($a))
            else
                window_clause = construct_window_clause(sq)
                return  "SUM($(string(a))) $(window_clause)"
            end
        elseif @capture(x, cumsum(a_))
            if from_summarize
                error("cumsum is only available through a windowed @mutate")
            else
               # sq.windowFrame = "ROWS UNBOUNDED PRECEDING "
               window_clause = construct_window_clause(sq, from_cumsum = true)
               return  "SUM($(string(a))) $(window_clause)"
            end
        #stats agg
        elseif @capture(x, std(a_))
            if from_summarize
                return :(STDDEV_SAMP($a))
            else
                window_clause = construct_window_clause(sq, )
                return  "STDDEV_SAMP($(string(a))) $(window_clause)"
            end
        elseif @capture(x, cor(a_, b_))
            if from_summarize
                return :(CORR($a))
            else
                window_clause = construct_window_clause(sq)
                return  "CORR($(string(a))) $(window_clause)"
            end
        elseif @capture(x, cov(a_, b_))
            if from_summarize
                return :(COVAR_SAMP($a))
            else
                window_clause = construct_window_clause(sq)
                return  "COVAR_SAMP($(string(a))) $(window_clause)"
            end
        elseif @capture(x, var(a_))
            if from_summarize
                return :(VAR_SAMP($a))
            else
                window_clause = construct_window_clause(sq)
                return  "VAR_SAMP($(string(a))) $(window_clause)"
            end
        #elseif @capture(x, sql_agg(str_))
        #    if from_summarize
        #        return  error("sql_agg is only needed with aggregate functions in @mutate")
        #    else
        #        window_clause = construct_window_clause(sq)
        #        return "$(str) $(window_clause)"
        #    end
        #stringr functions, have to use function that removes _ so capture can capture name
        elseif @capture(x, strreplaceall(str_, pattern_, replace_))
            return :(REGEXP_REPLACE($str, $pattern, $replace, 'g'))
        elseif @capture(x, strreplace(str_, pattern_, replace_))
            return :(REGEXP_REPLACE($str, $pattern, $replace))
        elseif @capture(x, strremoveall(str_, pattern_))
            return :(REGEXP_REPLACE($str, $pattern, "", "g"))
        elseif @capture(x, strremove(str_, pattern_))
            return :(REGEXP_REPLACE($str, $pattern, ""))
        elseif @capture(x, ismissing(a_))
            return  "($(string(a)) IS NULL)"
        # Date extraction functions
        elseif @capture(x, year(a_))
            return "EXTRACT(YEAR FROM " * string(a) * ")"
        elseif @capture(x, month(a_))
            return "EXTRACT(MONTH FROM " * string(a) * ")"
        elseif @capture(x, day(a_))
            return "EXTRACT(DAY FROM " * string(a) * ")"
        elseif @capture(x, hour(a_))
            return "EXTRACT(HOUR FROM " * string(a) * ")"
        elseif @capture(x, minute(a_))
            return "EXTRACT(MINUTE FROM " * string(a) * ")"
        elseif @capture(x, second(a_))
            return "EXTRACT(SECOND FROM " * string(a) * ")"
        elseif @capture(x, floordate(time_column_, unit_))
            return :(DATE_TRUNC($unit, $time_column))
        elseif @capture(x, difftime(endtime_, starttime_, unit_))
            return :(date_diff($unit, $starttime, $endtime))
        elseif @capture(x, replacemissing(column_, replacement_value_))
            return :(COALESCE($column, $replacement_value))
        elseif @capture(x, missingif(column_, value_to_replace_))
                return :(NULLIF($column, $value_to_replace))   
        elseif isa(x, Expr) && x.head == :call
            if x.args[1] == :if_else && length(x.args) == 4
                return parse_if_else(x)
            elseif x.args[1] == :as_float && length(x.args) == 2
                column = x.args[2]
                return "CAST(" * string(column) * " AS DECIMAL)"
            elseif x.args[1] == :as_integer && length(x.args) == 2
                column = x.args[2]
                return "CAST(" * string(column) * " AS INT)"
            elseif x.args[1] == :as_string && length(x.args) == 2
                column = x.args[2]
                return "CAST(" * string(column) * " AS STRING)"
            elseif x.args[1] == :case_when
                return parse_case_when(x)
        elseif isa(x, Expr) && x.head == :call && x.args[1] == :!  && x.args[1] != :!= && length(x.args) == 2
            inner_expr = expr_to_sql_gbq(x.args[2], sq, from_summarize = false)  # Recursively transform the inner expression
            return string("NOT (", inner_expr, ")")
        elseif x.args[1] == :str_detect && length(x.args) == 3
            column, pattern = x.args[2], x.args[3]
            return string(column, " LIKE \'%", pattern, "%'")
        elseif isa(x, Expr) && x.head == :call && x.args[1] == :n && length(x.args) == 1
            return "COUNT(*)"
            end
        end
        return x
    end
end
