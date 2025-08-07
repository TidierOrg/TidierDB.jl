# COV_EXCL_START
function expr_to_sql_lite(expr, sq; from_summarize::Bool)
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
                return  "***AVG($(string(a))) $(window_clause)***"
            end
        elseif @capture(x, minimum(a_))
            if from_summarize
                return :(MIN($a))
            else
                window_clause = construct_window_clause(sq)
                return  "***MIN($(string(a))) $(window_clause)***"
            end
        elseif @capture(x, maximum(a_))
            if from_summarize
                return :(MAX($a))
            else
                window_clause = construct_window_clause(sq)
                return  "***MAX($(string(a))) $(window_clause)***"
            end
        elseif @capture(x, sum(a_))
            if from_summarize
                return :(SUM($a))
            else
                window_clause = construct_window_clause(sq)
                return  "***SUM($(string(a))) $(window_clause)***"
            end
        elseif @capture(x, cumsum(a_))
            if from_summarize
                error("cumsum is only available through a windowed @mutate")
            else
               # sq.windowFrame = "ROWS UNBOUNDED PRECEDING "
                window_clause = construct_window_clause(sq, from_cumsum = true)
                return  "SUM($(string(a))) $(window_clause)"
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
        elseif isa(x, Expr) && x.head == :call && x.args[1] === :~
            # ~f(args)  ⇒  f(args) OVER (...)   (only valid in @mutate / window context)
            if from_summarize
                return error("~ is only needed with aggregate functions in @mutate")
            else
                inner = x.args[2]
                if !(inner isa Expr && inner.head == :call)
                    return error("Use ~ with a function call, e.g., ~mean(x)")
                end
                window_clause = construct_window_clause(sq)
                inner_sql = string(expr_to_sql(inner, sq; from_summarize=false))
                return "$(inner_sql) $(window_clause)"
            end
    # exc_capture_bug used above to allow proper _ function name capturing
        elseif @capture(x, replacemissing(column_, replacement_value_))
            return :(COALESCE($column, $replacement_value))
        elseif @capture(x, missingif(column_, value_to_replace_))
                return :(NULLIF($column, $value_to_replace)) 
        elseif @capture(x, ismissing(a_))
                return  "($(string(a)) IS NULL)"
        elseif isa(x, Expr) && x.head == :call
            if x.args[1] == :if_else
                return parse_if_else(x)
            elseif x.args[1] == :as_float && length(x.args) == 2
                column = x.args[2]
                # Return the SQL CAST statement directly as a string
                return Expr(:call, Symbol("CAST"), column, Symbol("AS DOUBLE"))
            elseif x.args[1] == :as_integer && length(x.args) == 2
                column = x.args[2]
                return Expr(:call, Symbol("CAST"), column, Symbol("AS INT"))
            elseif x.args[1] == :as_string && length(x.args) == 2
                column = x.args[2]
                return Expr(:call, Symbol("CAST"), column, Symbol("AS STRING"))
            elseif x.args[1] == :case_when
                return parse_case_when(x)
        elseif isa(x, Expr) && x.head == :call && x.args[1] == :! && length(x.args) == 2
            inner_expr = expr_to_sql_lite(x.args[2], sq, from_summarize = false)  # Recursively transform the inner expression
            return string("NOT (", inner_expr, ")")
        elseif x.args[1] == :str_detect && length(x.args) == 3
            column, pattern = x.args[2], x.args[3]
            return string(column, " LIKE \'%", pattern, "%'")
        elseif isa(x, Expr) && x.head == :call && x.args[1] == :n && length(x.args) == 1
            if from_summarize
                return "COUNT(*)"
            else
                window_clause = construct_window_clause(sq)
                return "COUNT(*) $(window_clause)"
            end
            end
        end
        return x
    end
end
# COV_EXCL_STOP