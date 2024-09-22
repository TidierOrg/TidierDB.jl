function expr_to_sql_trino(expr, sq; from_summarize::Bool)
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
       elseif @capture(x, agg(str_))
            if from_summarize
                return  error("agg is only needed with aggregate functions in @mutate")
            else
                window_clause = construct_window_clause(sq)
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
            inner_expr = expr_to_sql_duckdb(x.args[2], sq, from_summarize = false)  # Recursively transform the inner expression
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


function apply_type_conversion(df, col_index, col_type)
    if col_type == "double"
        df[!, col_index] = [ismissing(x) ? missing : parse(Float64, x) for x in df[!, col_index]]
    elseif col_type == "bigint"
        df[!, col_index] = [ismissing(x) ? missing : parse(Int, x) for x in df[!, col_index]]
    elseif col_type == "varchar"
        # Assuming varchar needs to stay as String, no conversion needed
    end
end

function parse_athena_df(df, column_types)
    for (i, col_type) in enumerate(column_types)
        # Check if column index is within bounds of DataFrame columns
        if i <= size(df, 2)
            try
                apply_type_conversion(df, i, col_type)
            catch e
               # @warn "Failed to convert column $(i) to $(col_type): $e"
            end
        else
           # @warn "Column index $(i) is out of bounds for the current DataFrame."
        end
    end;
    return df
end


