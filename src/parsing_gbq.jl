# COV_EXCL_START
function expr_to_sql_gbq(expr, sq; from_summarize::Bool)
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
        elseif @capture(x, column_[key_])
            # Convert variables to strings if necessary
            column_str = string(column)
            key_str = string(key)
            if uppercase(column_str) == "ARRAY"
                return x
            else
                return """ CASE WHEN $column_str IS NULL THEN NULL ELSE COALESCE(  LIST_EXTRACT( CASE  WHEN '$key_str' IS NULL THEN NULL ELSE ELEMENT_AT($column_str, '$key_str') END, 1 ), NULL) END ***"""
            end        # Date extraction functions
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
        elseif @capture(x, ymd(time_))
            return :(PARSE_DATE("%Y-%m-%d", $time))
        elseif @capture(x, mdy(time_))
            return :(PARSE_DATE("%m-%d-%Y", $time))
        elseif @capture(x, dmy(time_))
            return :(PARSE_DATE("%d-%m-%Y", $time))
        elseif @capture(x, floordate(time_column_, unit_))
            return :(DATE_TRUNC($unit, $time_column))
        elseif @capture(x, difftime(endtime_, starttime_, unit_))
            return :(date_diff($unit, $starttime, $endtime))
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
                inner_expr = expr_to_sql_gbq(x.args[2], sq, from_summarize = false)  # Recursively transform the inner expression
                return string("NOT (", inner_expr, ")")
            elseif x.args[1] == :str_detect && length(x.args) == 3
                column, pattern = x.args[2], x.args[3]
                if pattern isa String
                    return string(column, " LIKE \'%", pattern, "%'")
                elseif pattern isa Expr
                    pattern_str = string(pattern)[2:end]
                    return string("REGEXP_CONTAINS(", column, ", '", pattern_str, "')")
                end
            elseif x.args[1] == :n && length(x.args) == 1
                return from_summarize ? "COUNT(*)" : "COUNT(*) $(construct_window_clause(sq))"
                        elseif string(x.args[1]) in String.(window_agg_fxns)
                    if from_summarize
                        return x
                    else
                        args = x.args[2:end]       
                        window_clause = construct_window_clause(sq)
                        arg_str = join(map(string, args), ", ")        
                        str_representation = "$(string(x.args[1]))($(arg_str))"  
                        return "$(str_representation) $(window_clause)"
                    end
                end   
        elseif isa(x, SQLQuery)
            return "(__(" * finalize_query(x) * ")__("
        end
        return x
    end
end
# COV_EXCL_STOP