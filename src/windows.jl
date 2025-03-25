"""
$docstring_window_order
"""
macro window_order(sqlquery, order_by_expr...)

    return quote
        sq = $(esc(sqlquery))
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 

        if isa(sq, SQLQuery)
            # Convert order_by_expr to SQL order by string
            order_specs = String[]
            for expr in $(esc(order_by_expr))
                if isa(expr, Expr) && expr.head == :call && expr.args[1] == :desc #|| _frame
                    # Column specified with `desc()`, indicating descending order
                    push!(order_specs, string(expr.args[2]) * " DESC")
                elseif isa(expr, String) && startswith(expr, "DESC")
                    # String column starting with `DESC`, indicating descending order
                    push!(order_specs, string(replace(expr, "DESC" => "")) * " DESC")
                elseif isa(expr, String) || isa(expr, Symbol)
                    # Plain column symbol, indicating ascending order
                    push!(order_specs, string(expr) * " ASC")
                else
                    throw("Unsupported column specification in @window_order: $expr")
                end
            end
            order_by_sql = join(order_specs, ", ")
            
            # Update the window_order field of the SQLQuery instance
            sq.window_order = order_by_sql
            
            # If this is the first operation after an aggregation, wrap current state in a CTE
            if sq.post_aggregation
                sq.post_aggregation = false
                cte_name = "cte_" * string(sq.cte_count + 1)
                cte_sql = "SELECT * FROM " * sq.from
                
                if !isempty(sq.where)
                    cte_sql *= " WHERE " * sq.where
                end
                
                new_cte = CTE(name=cte_name, select=cte_sql, from=sq.from)
                up_cte_name(sq, cte_name)
                push!(sq.ctes, new_cte)
                sq.cte_count += 1
                
                # Reset the from to reference the new CTE
                sq.from = cte_name
            end
        else
            error("Expected sqlquery to be an instance of SQLQuery") # COV_EXCL_LINE
        end
        sq
    end
end

"""
$docstring_window_frame
"""
macro window_frame(sqlquery, args...)
 #   sqlquery_expr = esc(sqlquery)
    # Initialize expressions for from and to values
    frame_from_expr = nothing
    frame_to_expr = nothing

    # Process the arguments at macro expansion time
    for arg in args
        if isa(arg, Expr) && arg.head == :(=)
            # Named argument
            arg_name = arg.args[1]
            arg_value = arg.args[2]
            if arg_name == :from
                frame_from_expr = arg_value
            elseif arg_name == :to
                frame_to_expr = arg_value
            else
                error("Unknown keyword argument: $(arg_name)") # COV_EXCL_LINE
            end
        elseif isa(arg, Expr) && arg.head == :tuple
            if length(arg.args) != 2
                error("`_frame` must be a tuple with exactly two elements: (_frame = (from, to))") # COV_EXCL_LINE
            end
            frame_from_expr = arg.args[1]
            frame_to_expr = arg.args[2]
        else
            # Positional argument
            if frame_from_expr === nothing
                frame_from_expr = arg
            elseif frame_to_expr === nothing
                frame_to_expr = arg
            else
                error("Too many positional arguments") # COV_EXCL_LINE
            end
        end
    end

    # Now generate the code that computes the frame clauses at runtime
    return quote
        sq = $(esc(sqlquery))
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 

        if isa(sq, SQLQuery)
            # Evaluate frame_from_value and frame_to_value
            frame_from_value = $(frame_from_expr !== nothing ? esc(frame_from_expr) : :(nothing))
            frame_to_value = $(frame_to_expr !== nothing ? esc(frame_to_expr) : :(nothing))

            # Initialize frame_start_clause and frame_end_clause
            frame_start_clause = ""
            frame_end_clause = ""

            if frame_from_value !== nothing && frame_to_value === nothing
                # Only from is specified
                frame_start_value = frame_from_value
                frame_start_clause = if frame_start_value == 0
                    "CURRENT ROW"
                elseif frame_start_value < 0
                    string(abs(frame_start_value), " PRECEDING")
                elseif frame_start_value > 0
                    string(abs(frame_start_value), " FOLLOWING")
                else
                    error("Invalid frame_from_value")
                end
                # Set frame_end_clause to "UNBOUNDED FOLLOWING"
                frame_end_clause = "UNBOUNDED FOLLOWING"
            elseif frame_from_value === nothing && frame_to_value !== nothing
                # Only to is specified
                frame_end_value = frame_to_value
                frame_end_clause = if frame_end_value == 0
                    "CURRENT ROW"
                elseif frame_end_value < 0
                    string(abs(frame_end_value), " PRECEDING")
                elseif frame_end_value > 0
                    string(abs(frame_end_value), " FOLLOWING")
                else
                    error("Invalid frame_to_value")
                end
                # Set frame_start_clause to "UNBOUNDED PRECEDING"
                frame_start_clause = "UNBOUNDED PRECEDING"
            elseif frame_from_value !== nothing && frame_to_value !== nothing
                # Both from and to are specified
                frame_start_value = frame_from_value
                frame_start_clause = if frame_start_value == 0
                    "CURRENT ROW"
                elseif frame_start_value < 0
                    string(abs(frame_start_value), " PRECEDING")
                elseif frame_start_value > 0
                    string(abs(frame_start_value), " FOLLOWING")
                else
                    error("Invalid frame_from_value")
                end

                frame_end_value = frame_to_value
                frame_end_clause = if frame_end_value == 0
                    "CURRENT ROW"
                elseif frame_end_value < 0
                    string(abs(frame_end_value), " PRECEDING")
                elseif frame_end_value > 0
                    string(abs(frame_end_value), " FOLLOWING")
                else
                    error("Invalid frame_to_value")
                end
            else
                # Neither from nor to is specified
                frame_start_clause = "UNBOUNDED PRECEDING"
                frame_end_clause = "UNBOUNDED FOLLOWING"

            end

            # Construct the window frame clause
            frame_clause = "ROWS BETWEEN $(frame_start_clause) AND $(frame_end_clause)"

            # Update the windowFrame field of the SQLQuery instance
            sq.windowFrame = frame_clause
        else
            error("Expected sqlquery to be an instance of SQLQuery") # COV_EXCL_LINE
        end
        sq
    end
end
