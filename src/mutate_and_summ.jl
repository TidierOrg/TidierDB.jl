function process_mutate_expression(expr, sq, select_expressions, cte_name)
    if isa(expr, Expr) && expr.head == :(=) && isa(expr.args[1], Symbol)
        # Extract column name and convert to string
        col_name = string(expr.args[1])
        if current_sql_mode[] == snowflake()
            col_name = uppercase(col_name)
        end
        
        # Convert the expression to a SQL expression
        col_expr = expr_to_sql(expr.args[2], sq)
        col_expr = string(col_expr)
        
        # Check if the column already exists in the metadata
        if col_name in sq.metadata[!, "name"]
            # Find the index of the existing column in select_expressions
            select_expr_index = findfirst(==(col_name), select_expressions)
            if !isnothing(select_expr_index)
                # Replace the existing column expression with the new mutation
                select_expressions[select_expr_index] = col_expr * " AS " * col_name
            else
                # If not found in select_expressions, append the new expression
                push!(select_expressions, col_expr * " AS " * col_name)
            end
            # Update the existing metadata entry instead of adding a new one
            metadata_index = findfirst(==(col_name), sq.metadata[!, "name"])
            if !isnothing(metadata_index)
                sq.metadata[metadata_index, "type"] = "UNKNOWN"
                sq.metadata[metadata_index, "current_selxn"] = 1
                sq.metadata[metadata_index, "table_name"] = cte_name
            end
        else
            # Append the mutation as a new column expression
            push!(select_expressions, col_expr * " AS " * col_name)
            # Update metadata to include this new column
            push!(sq.metadata, Dict("name" => col_name, "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => cte_name))
        end
    else
        throw("Unsupported expression format in @mutate: $(expr)")
    end
end

"""
$docstring_mutate
"""
macro mutate(sqlquery, mutations...)
    mutations = parse_blocks(mutations...)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            cte_name = "cte_" * string(sq.cte_count + 1)

            if sq.post_aggregation
                # Reset post_aggregation as we're now handling it
                sq.post_aggregation = false
               # sq.cte_count += 1
                select_expressions = !isempty(sq.select) ? [sq.select] : ["*"]
        
                cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
                if sq.is_aggregated && !isempty(sq.groupBy)
                    cte_sql *= " " * sq.groupBy
                    sq.groupBy = ""
                end
                if !isempty(sq.where)
                    cte_sql *= " WHERE " * sq.where
                    sq.where = " "
                end
                if !isempty(sq.having)
                    cte_sql *= "  " * sq.having
                    sq.having = " "
                end
    
                # Create and add the new CTE
                new_cte = CTE(name=string(cte_name), select=cte_sql)
                push!(sq.ctes, new_cte)
                sq.cte_count += 1
                sq.from = string(cte_name)
                
            else
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
            end
            cte_name = "cte_" * string(sq.cte_count + 1)
            sq.cte_count += 1
            # Prepare select expressions, starting with existing selections if any
            #select_expressions =  ["*"]
            #if sq.is_aggregated == true
            select_expressions =  ["*"]
            #else
            #select_expressions = !isempty(sq.select) && sq.select != "*" ? [sq.select] : ["*"]
            #end
            all_columns = [
                (row[:current_selxn] == 1 ? row[:name] : row[:table_name] * "." * row[:name])
                for row in eachrow(sq.metadata) if row[:current_selxn] != 0
            ]            
            select_expressions = [col for col in all_columns]  # Start with all currently selected columns

            for expr in $mutations
                # Transform 'across' expressions first
                if isa(expr, Expr) && expr.head == :call && expr.args[1] == :across
                    expr = parse_across(expr, $(esc(sqlquery)).metadata)  # Assume expr_to_sql can handle 'across' and returns a tuple of expressions
                end
                if isa(expr, Expr) && expr.head == :tuple
                    for subexpr in expr.args
                        process_mutate_expression(subexpr, sq, select_expressions, cte_name)
                    end
                else
                    process_mutate_expression(expr, sq, select_expressions, cte_name)
                end
            end
            cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from

            # Construct CTE SQL, handling aggregated queries differently
            cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
            if sq.is_aggregated # && !isempty(sq.groupBy)
                cte_sql *= " " * sq.groupBy
                sq.is_aggregated = false

            end
            if !isempty(sq.where)
                cte_sql *= " WHERE " * sq.where
            end

            # Create and add the new CTE
            new_cte = CTE(name=string(cte_name), select=cte_sql)
            push!(sq.ctes, new_cte)

            # Update sq.from to the latest CTE, reset sq.select for final query
            sq.from = string(cte_name)
            
            sq.select = "*"  # This selects everything from the CTE without duplicating transformations
            if _warning_[]
                if sq.groupBy != "" || sq.window_order !=""  || sq.windowFrame !=""
                @warn "After applying all mutations, @mutate removed grouping and window clauses."
                end
            end
            sq.groupBy =""
            sq.windowFrame = ""
            sq.window_order = ""
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

function process_summary_expression(expr, sq, summary_str)
    if isa(expr, Expr) && expr.head == :(=) && isa(expr.args[1], Symbol)
        summary_operation = expr_to_sql(expr.args[2], sq, from_summarize = true)
        summary_operation = string(summary_operation)
        summary_column = expr_to_sql(expr.args[1], sq, from_summarize = true)
        summary_column = string(summary_column)
        if current_sql_mode[] == snowflake()
            summary_column = uppercase(summary_column)
        end
        push!(sq.metadata, Dict("name" => summary_column, "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => sq.from))
    
        push!(summary_str, summary_operation * " AS " * summary_column)
    else
        throw("Unsupported expression format in @summarize: $(expr)")
    end
end

"""
$docstring_summarize
"""
macro summarize(sqlquery, expressions...)
    expressions = parse_blocks(expressions...)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            summary_str = String[]
            sq.metadata.current_selxn .= 0
            groupby_columns = split(replace(sq.groupBy, "GROUP BY " => ""), ", ")
            groupby_columns = strip.(groupby_columns)
            for groupby_column in groupby_columns
                for i in 1:size(sq.metadata, 1)
                    if sq.metadata[i, :name] == groupby_column
                        sq.metadata[i, :current_selxn] = 1
                        break 
                    end
                end
            end
            
            for expr in $expressions
                # Transform 'across' expressions first
                if isa(expr, Expr) && expr.head == :call && expr.args[1] == :across
                    expr = parse_across(expr, $(esc(sqlquery)).metadata)  # Assume expr_to_sql can handle 'across' and returns a tuple of expressions
                end
                if isa(expr, Expr) && expr.head == :tuple
                    for subexpr in expr.args
                        process_summary_expression(subexpr, sq, summary_str)
                    end
                else
                    process_summary_expression(expr, sq, summary_str)
                end
            end
        
            summary_clause = join(summary_str, ", ")
            existing_select = sq.select
            # Check if there's already a SELECT clause and append, otherwise create new
            if startswith(existing_select, "SELECT")
                sq.select = existing_select * ", " * summary_clause
            elseif  isempty(summary_clause)
                sq.select = "SUMMARIZE "
            else
                sq.select = "SELECT " * summary_clause
            end
            sq.is_aggregated = true  # Mark the query as aggregated
            sq.post_aggregation = true  # Indicate ready for post-aggregation operations
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_summarise
"""
macro summarise(df, expressions...)
    :(@summarize($(esc(df)), $(expressions...)))
end