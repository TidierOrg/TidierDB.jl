## snowflake connection and execution begins around 150
function expr_to_sql_snowflake(expr, sq; from_summarize::Bool)
   # expr = parse_char_matching(expr)
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
        elseif isa(x, Expr) && x.head == :call && x.args[1] == :agg
            args = x.args[2:end]       # Capture all arguments to agg
            if from_summarize
                return error("agg is only needed with aggregate functions in @mutate")
            else
                window_clause = construct_window_clause(sq)
                # Create the SQL string representation of the agg function call
                arg_str = join(map(string, args), ", ")
                str = "$(arg_str)"
                return "$(str) $(window_clause)"
            end
        elseif !isempty(sq.window_order) && isa(x, Expr) && x.head == :call
            function_name = x.args[1]  # This will be `lead`
            args = x.args[2:end]       # Capture all arguments from the second position onward
            window_clause = construct_window_clause(sq)
        
            # Create the SQL string representation of the function call
            arg_str = join(map(string, args), ", ")  # Join arguments into a string
            str = "$(function_name)($(arg_str))"      # Construct the function call string
            return "$(str) $(window_clause)"
        #stringr functions, have to use function that removes _ so capture can capture name
        elseif @capture(x, strreplaceall(str_, pattern_, replace_))
            return :(REGEXP_REPLACE($str, $pattern, $replace, 'g'))
        elseif @capture(x, strreplace(str_, pattern_, replace_))
            return :(REGEXP_REPLACE($str, $pattern, $replace))
        elseif @capture(x, strremoveall(str_, pattern_))
            return :(REGEXP_REPLACE($str, $pattern, "g"))
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
        elseif @capture(x, ymd(time_))
            return :(TO_DATE($time, "YYYY-MM-DD"))
        elseif @capture(x, mdy(time_))
            return :(TO_DATE($time, "MM-DD-YYYY"))
        elseif @capture(x, dmy(time_))
            return :(TO_DATE($time, "DD-MM-YYYY"))
        elseif @capture(x, replacemissing(column_, replacement_value_))
            return :(COALESCE($column, $replacement_value))
        elseif @capture(x, missingif(column_, value_to_replace_))
                return :(NULLIF($column, $value_to_replace))   
        elseif isa(x, Expr) && x.head == :call
            if x.args[1] == :if_else
                return parse_if_else(x)
            elseif x.args[1] == :as_float && length(x.args) == 2
                column = x.args[2]
                return Expr(:call, Symbol("CAST"), column, Symbol("AS DECIMAL"))
            elseif x.args[1] == :as_integer && length(x.args) == 2
                column = x.args[2]
                return Expr(:call, Symbol("CAST"), column, Symbol("AS INT"))
            elseif x.args[1] == :as_string && length(x.args) == 2
                column = x.args[2]
                return Expr(:call, Symbol("CAST"), column, Symbol("AS STRING"))
            elseif x.args[1] == :case_when
                return parse_case_when(x)
        elseif isa(x, Expr) && x.head == :call && x.args[1] == :!  && x.args[1] != :!= && length(x.args) == 2
            inner_expr = expr_to_sql_snowflake(x.args[2], sq, from_summarize = false)  # Recursively transform the inner expression
            return string("NOT (", inner_expr, ")")
        elseif x.args[1] == :str_detect && length(x.args) == 3
            column, pattern = x.args[2], x.args[3]
            if pattern isa String
                return string(column, " LIKE \'%", pattern, "%'")
            elseif pattern isa Expr
                pattern_str = string(pattern)[2:end]
                return string("REGEXP_LIKE", column, ", '", pattern_str, "')")
            end
        elseif isa(x, Expr) && x.head == :call && x.args[1] == :n && length(x.args) == 1
            if from_summarize
                return "COUNT(*)"
            else
                window_clause = construct_window_clause(sq)
                return "COUNT(*) $(window_clause)"
            end
            end
        elseif isa(x, SQLQuery)
            return "(__(" * finalize_query(x) * ")__("
        end
        return x
    end
end

mutable struct SnowflakeConnection
    account_identifier::String
    auth_token::String
    database::String
    schema::String
    warehouse::String
    api_url::String
end

function execute_snowflake(conn::SnowflakeConnection, sql_query::String)
    json_body = JSON3.write(Dict(
        "statement" => sql_query,
        "database" => conn.database,
        "schema" => conn.schema,
        "warehouse" => conn.warehouse,
        "role" => ""
    ))
    headers = Dict(
        "Authorization" => "Bearer $(conn.auth_token)",
        "Content-Type" => "application/json"
    )

    response = nothing  # Initialize response outside the try block
    try
        response = HTTP.post(conn.api_url, headers, json_body)
    catch e
        if isa(e, HTTP.StatusError)
            # Extract and print the error message from the HTTP response
            println("HTTP request failed with status $(e.status): $(String(e.response.body))")
            return
        else
            rethrow(e)
        end
    end

    if response === nothing
        println("No response was obtained from the server.")
        return
    end

    if response.status == 200
        content_encoding = ""
        for header in response.headers
            if header.first == "Content-Encoding"
                content_encoding = header.second
                break
            end
        end

        decompressed_body = ""
        if content_encoding == "gzip"
            try
                temp_file = "temp_response.gz"
                open(temp_file, "w") do file
                    write(file, response.body)
                end
                GZip.open(temp_file, "r") do file
                    decompressed_body = read(file, String)
                end
                rm(temp_file)
            catch e
                println("Decompression error: ", e)
            end
        else
            decompressed_body = String(response.body)
        end

        json_data = JSON3.read(decompressed_body)
        column_names = Symbol[]
        for col in json_data.resultSetMetaData.rowType
            push!(column_names, Symbol(col.name))
        end
        data_matrix = [json_data.data[i] for i in 1:length(json_data.data)]
        transposed_data = permutedims(hcat(data_matrix...))
        df = DataFrame(transposed_data, column_names)
        transform!(df, names(df) .=> ByRow(x -> isnothing(x) ? missing : x), renamecols=false)
        
        for col in names(df)
            if all(x -> ismissing(x) || can_convert_numeric(x), df[!, col])
                df[!, col] = [ismissing(x) ? missing : try_parse_numeric(x) for x in df[!, col]]
            end
        end 
        return df
    else
        println("Failed to execute query, status: ", response.status)
        println("Response: ", String(response.body))
    end
end

function get_table_metadata(conn::SnowflakeConnection, table_name::String)
    table_name = uppercase(table_name)
    query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM $(conn.database).INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '$(conn.schema)'
            AND TABLE_NAME = '$table_name'
            ORDER BY ORDINAL_POSITION;
            """
    result = execute_snowflake(conn, query)
    result[!, :current_selxn] .= 1
    result[!, :table_name] .= table_name
    # Adjust the select statement to include the new table_name column
    return select(result, 1 => :name, 2 => :type, :current_selxn, :table_name)
end



function try_parse_numeric(x)
    try
        parse(Int, x)
    catch
        try
            parse(Float64, x)
        catch
            x 
        end
    end
end

function can_convert_numeric(x)
    try
        parse(Int, x)
        return true
    catch
        try
            parse(Float64, x)
            return true
        catch
            return false
        end
    end
end

function update_con(sqlquery::SQLQuery, new_token::String)
    sqlquery.db.auth_token = new_token
    return sqlquery
end

function update_con(con::SnowflakeConnection, new_token::String)
    con.auth_token = new_token
end


function show_tables(con::SnowflakeConnection)
    result = execute_snowflake(con, "SHOW TABLES in SCHEMA $(con.schema)")
    return DataFrame(result)
end
