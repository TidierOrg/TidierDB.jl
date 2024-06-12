"""
$docstring_select
"""
macro select(sqlquery, exprs...)
    exprs = parse_blocks(exprs...)
    exprs_str = parse_interpolation2.(exprs)

    return quote
        exprs_str = map(expr -> isa(expr, Symbol) ? string(expr) : expr, $exprs)

        let columns = parse_tidy_db($exprs_str, $(esc(sqlquery)).metadata)
            columns_str = join(["SELECT ", join([string(column) for column in columns], ", ")])
            $(esc(sqlquery)).select = columns_str
            $(esc(sqlquery)).metadata.current_selxn .= 0
            for col in columns
                if occursin(".", col)
                    table_col_split = split(col, ".")
                    table_name, col_name = table_col_split[1], table_col_split[2]

                    # Iterate and update current_selxn based on matches
                    for idx in eachindex($(esc(sqlquery)).metadata.current_selxn)
                        if $(esc(sqlquery)).metadata.table_name[idx] == table_name && 
                           $(esc(sqlquery)).metadata.name[idx] == col_name
                            $(esc(sqlquery)).metadata.current_selxn[idx] = 2
                        end
                    end
                else
                    # Direct matching for columns without 'table.' prefix
                    matching_indices = findall($(esc(sqlquery)).metadata.name .== col)
                    $(esc(sqlquery)).metadata.current_selxn[matching_indices] .= 1
                end
            end
        end
        
        $(esc(sqlquery))
    end
end

"""
$docstring_filter
"""
macro filter(sqlquery, conditions...)
    conditions = parse_blocks(conditions...)
    conditions = parse_interpolation2.(conditions)
    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            # Early handling for non-aggregated context
            if !sq.is_aggregated
                cte_name = "cte_" * string(sq.cte_count + 1)
                combined_conditions = String[]
                for condition in $(esc(conditions))
                    condition_str = string(expr_to_sql(condition, sq))
                    condition_str = replace(condition_str, "'\"" => "'",  "'\"" => "'", "\"'" => "'", "[" => "(", "]" => ")")
                    push!(combined_conditions, condition_str)
                end
                combined_condition_str = join(combined_conditions, " AND ")
                new_cte = CTE(name=cte_name, select="*", from=(isempty(sq.ctes) ? sq.from : last(sq.ctes).name), where=combined_condition_str)
                push!(sq.ctes, new_cte)
                sq.from = cte_name
                sq.cte_count += 1
                
            else
            aggregated_columns = Set{String}()
            
            # Check SELECT clause of the main query and all CTEs for aggregation functions
            if !isempty(sq.select)
                for part in split(sq.select, ", ")
                    if occursin(" AS ", part)
                        # Extract the alias used after 'AS' which represents an aggregated column
                        aggregated_column = strip(split(part, " AS ")[2])
                        push!(aggregated_columns, aggregated_column)
                    end
                end
            end
            
            non_aggregated_conditions = String[]
            groupby_columns = split(replace(sq.groupBy, "GROUP BY " => ""), ", ")
            groupby_columns = strip.(groupby_columns)
            
            # Process each condition
            for condition in $(esc(conditions))
                condition_str = string(expr_to_sql(condition, sq)) # Convert condition to SQL string
                condition_str = replace(condition_str, "'\"" => "'", "\"'" => "'", "[" => "(", "]" => ")")
                condition_involves_aggregated_column = any(col -> occursin(Regex("\\b$col\\b"), condition_str), aggregated_columns)
                sq.where = ""

                if !condition_involves_aggregated_column && any(col -> occursin(Regex("\\b$col\\b"), condition_str), groupby_columns)
                    # Condition involves an aggregated/grouping column; use HAVING clause
                    main_query_having = !isempty(sq.having) ? sq.having * " AND " * condition_str : "HAVING " * condition_str
                    sq.having = main_query_having
                    sq.where = ""  # Clearing sq.where to prevent carrying over conditions

                else
                   push!(non_aggregated_conditions, condition_str)
                end
            end
            if !isempty(non_aggregated_conditions)
                combined_conditions = join(non_aggregated_conditions, " AND ")
                cte_name = "cte_" * string(sq.cte_count + 1)
                new_cte = CTE(name=cte_name, select=sq.select, from=(isempty(sq.ctes) ? sq.from : last(sq.ctes).name), groupBy = sq.groupBy, having=sq.having)
                push!(sq.ctes, new_cte)
                sq.select = "*"
                sq.groupBy = ""
                sq.having = ""
                
                sq.where = "WHERE " * join(non_aggregated_conditions, " AND ")
                sq.from = cte_name
                sq.cte_count += 1
            end
        end
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


desc(col::Symbol) = (col, :desc)
"""
$docstring_arrange
"""
macro arrange(sqlquery, columns...)
    columns = parse_interpolation2.(columns)

    # Initialize a string to hold column order specifications
    order_specs = String[]

    # Process each column argument
    for col in columns
        if isa(col, Expr) && col.head == :call && col.args[1] == :desc
            # Column specified with `desc()`, indicating descending order
            push!(order_specs, string(col.args[2]) * " DESC")
        elseif isa(col, Symbol)
            # Plain column symbol, indicating ascending order
            push!(order_specs, string(col) * " ASC")
        else
            throw("Unsupported column specification in @arrange: $col")
        end
    end

    # Construct the ORDER BY clause
    order_clause = join(order_specs, ", ")

    # Modify the SQLQuery object's orderBy field
    return quote
        if $(esc(sqlquery)) isa SQLQuery
            $(esc(sqlquery)).orderBy = " ORDER BY " * $order_clause
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        $(esc(sqlquery))
    end
end



function process_mutate_expression(expr, sq, select_expressions, cte_name)
    if isa(expr, Expr) && expr.head == :(=) && isa(expr.args[1], Symbol)
        col_name = string(expr.args[1])
        col_expr = expr_to_sql(expr.args[2], sq)  # Convert to SQL expression

        # Determine whether the column already exists or needs to be added
        if col_name in [col for col in sq.metadata[!, "name"]]
            # Replace the existing column expression with the mutation
            select_expr_index = findfirst(==(col_name), select_expressions)
            select_expressions[select_expr_index] = string(col_expr, " AS ", col_name)
        else
            # Append the mutation as a new column expression
            push!(select_expressions, string(col_expr, " AS ", col_name))
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
    mutations = parse_interpolation2.(mutations)
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
         #   if !isempty(sq.groupBy)
          #      println("@mutate removed grouping after applying mutations.")
          #  end
            sq.groupBy =""
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

function quote_col_name(col_name::String)
    "$col_name"
end

"""
$docstring_group_by
"""
macro group_by(sqlquery, columns...)
    columns = parse_blocks(columns...)
    columns = parse_interpolation2.(columns)

    return quote
        columns_str = map(col -> isa(col, Symbol) ? string(col) : col, $columns)
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)

            let group_columns = parse_tidy_db(columns_str, sq.metadata)
                group_clause = "GROUP BY " * join(group_columns, ", ")
                
                sq.groupBy = group_clause

               # if isempty(sq.select) || sq.select == "SELECT "
               #     sq.select = "SELECT " * join(group_columns, ", ")
               # else
               #     for col in group_columns
               #         if !contains(sq.select, col)
               #             sq.select = sq.select * ", " * col
               #         end
               #     end
               # end

                current_group_columns = group_columns
                summarized_columns = split(sq.select, ", ")[2:end]  # Exclude the initial SELECT
                all_columns = unique(vcat(current_group_columns, summarized_columns))
                sq.select = "SELECT " * join(all_columns, ", ")
            end
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_distinct
"""
macro distinct(sqlquery, distinct_columns...)
    distinct_columns = parse_interpolation2.(distinct_columns)
    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            # Convert expressions to strings for parsing
            exprs_str = map(expr -> isa(expr, Symbol) ? string(expr) : expr, $(distinct_columns))
            
            # Use parse_tidy_db to determine the columns involved, based on metadata
            let columns = parse_tidy_db(exprs_str, sq.metadata)
                # Generate the SELECT part of the CTE based on the distinct columns
                distinct_cols_str = join([string(column) for column in columns], ", ")
                # Always increment cte_count to ensure a unique CTE name
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                
                # Construct the SELECT part of the CTE using distinct columns or all columns if none are specified
                cte_select = !isempty(distinct_cols_str) ? " DISTINCT " * distinct_cols_str : " DISTINCT *"
                cte_select *= " FROM " * sq.from
                
                # Create the CTE instance
                cte = CTE(name=cte_name, select=cte_select)
                
                # Add the CTE to the SQLQuery's CTEs vector
                push!(sq.ctes, cte)
                
                # Adjust the main query to select from the newly created CTE
                sq.from = cte_name
                
                # Reset sq.select to ensure the final SELECT * operates correctly
                sq.select = "*"
            end
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
    expressions = parse_interpolation2.(expressions)

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

"""
$docstring_count
"""
macro count(sqlquery, group_by_columns...)
    # Convert the group_by_columns to a string representation
    group_by_columns = parse_interpolation2.(group_by_columns)
    group_by_cols_str = [string(col) for col in group_by_columns]
    group_clause = join(group_by_cols_str, ", ")

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            # Interpolate `group_clause` directly into the quoted code to avoid scope issues
            if !isempty($group_clause)
                sq.select = "SELECT " * $group_clause * ", COUNT(*) AS count"
                sq.groupBy = "GROUP BY " * $group_clause
            else
                # If no grouping columns are specified, just count all records
                sq.select = "SELECT COUNT(*) AS count"
            end
            
            # Adjustments for previously set GROUP BY or ORDER BY clauses might be needed here
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_rename
"""
macro rename(sqlquery, renamings...)
    renamings = parse_blocks(renamings...)
    renamings = parse_interpolation2.(renamings)
    return quote
        # Prepare the renaming rules from the macro arguments
        renamings_dict = Dict{String, String}()
        for renaming in $(esc(renamings))
            if isa(renaming, Expr) && renaming.head == :(=) && isa(renaming.args[1], Symbol)
                # Map original column names to new names for renaming
                renamings_dict[string(renaming.args[2])] = string(renaming.args[1])
            else
                throw("Unsupported renaming format in @rename: $(renaming)")
            end
        end

        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            # Generate a new CTE name
            new_cte_name = "cte_" * string(sq.cte_count + 1)
            sq.cte_count += 1
     
            # Determine the select clause for the new CTE
            select_clause = if isempty(sq.select) || sq.select == "SELECT *"
                # If select is *, list all columns with renaming applied
                all_columns = sq.metadata[!, :name]
                join([haskey(renamings_dict, col) ? col * " AS " * renamings_dict[col] : col for col in all_columns], ", ")
            else
                
                select_parts = split(sq.select[8:end], ", ")
                updated_parts = map(select_parts) do part
                    # Identify the base column name for potential renaming
                    col = strip(split(part, " AS ")[1])
                    if haskey(renamings_dict, col)
                        # Apply renaming to the base column name
                        string(renamings_dict[col]) * " AS " * col
                    else
                        # No renaming needed; keep the original part
                        part
                    end
                end
                sq.select = " " * join(updated_parts, ", ")

            end
            for (old_name, new_name) in renamings_dict
                sq.metadata[!, :name] = replace.(sq.metadata[!, :name], old_name => new_name)
            end

            if isempty(sq.select) 
                 sq.select == "SELECT *" 
            end

            # Create the new CTE with the select clause
            new_cte = CTE(name=new_cte_name, select=select_clause, from=sq.from)
            push!(sq.ctes, new_cte)

            # Update the from clause of the SQLQuery to the new CTE
            sq.from = new_cte_name
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_window_order
"""
macro window_order(sqlquery, order_by_expr...)
    order_by_expr = parse_interpolation2.(order_by_expr)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            # Convert order_by_expr to SQL order by string
            order_by_sql = join([expr_to_sql(expr, sq) for expr in $(esc(order_by_expr))], ", ")
            
            # Update the orderBy field of the SQLQuery instance
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
                push!(sq.ctes, new_cte)
                sq.cte_count += 1
                
                # Reset the from to reference the new CTE
                sq.from = cte_name
            end
            
            # Note: Actual window functions would be applied in subsequent @mutate calls or similar,
            # potentially using the orderBy set here for their OVER() clauses.
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_window_frame
"""
macro window_frame(sqlquery, frame_start::Int, frame_end::Int)
    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            # Validate frame_start and frame_end
            if $frame_end < $frame_start
                error("frame_end must be greater than or equal to frame_start")
            end

            # Calculate absolute values for frame_start and frame_end
            abs_frame_start = abs($frame_start)
            abs_frame_end = abs($frame_end)

            # Determine the direction and clause for frame_start
            frame_start_clause = if $frame_start < 0
                string(abs_frame_start, " PRECEDING")
            else
                string(abs_frame_start, " FOLLOWING")
            end

            # Determine the direction and clause for frame_end
            frame_end_clause = if $frame_end < 0
                string(abs_frame_end, " PRECEDING")
            else
                string(abs_frame_end, " FOLLOWING")
            end

            # Construct the window frame clause
            frame_clause = string("ROWS BETWEEN ", frame_start_clause, " AND ", frame_end_clause)

            # Update the windowFrame field of the SQLQuery instance
            sq.windowFrame = frame_clause
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end




macro show_query(sqlquery)
    return quote
        # Generate the final query string
        final_query = finalize_query($(esc(sqlquery)))
        
        # Apply formatting for readability, including JOIN clauses
        formatted_query = replace(final_query, r"(?<=\)), " => ",\n") # New line after each CTE definition
        formatted_query = replace(formatted_query, "SELECT " => "\nSELECT ")
        formatted_query = replace(formatted_query, "AS (SELECT " => "AS ( \n\tSELECT ")
        formatted_query = replace(formatted_query, " FROM " => "\n\tFROM ")
        formatted_query = replace(formatted_query, " WHERE " => "\n\tWHERE ")
        formatted_query = replace(formatted_query, " GROUP BY " => "\n\tGROUP BY ")
        formatted_query = replace(formatted_query, " ORDER BY " => "\n\tORDER BY ")
        formatted_query = replace(formatted_query, " HAVING " => "\n\tHAVING ")
        formatted_query = replace(formatted_query, " LEFT JOIN " => "\n\tLEFT JOIN ")
        formatted_query = replace(formatted_query, " RIGHT JOIN " => "\n\tRIGHT JOIN ")
        formatted_query = replace(formatted_query, " INNER JOIN " => "\n\tINNER JOIN ")
        formatted_query = replace(formatted_query, " OUTER JOIN " => "\n\tOUTER JOIN ")
    #    formatted_query = replace(formatted_query, " JOIN " => "\n\tJOIN ") # General JOIN clause
        
        # Print the formatted query
        println(formatted_query)
    end
end

macro collect(sqlquery)
    return quote
        # Extract the database connection from the SQLQuery object
        db = $(esc(sqlquery)).db
        sq = $(esc(sqlquery))
        # Finalize the query to get the SQL string
        final_query = finalize_query($(esc(sqlquery)))
        df_result = DataFrame()
        # Determine the type of db and execute the query accordingly
        if db isa SQLite.DB || db isa LibPQ.Connection || db isa DuckDB.Connection || db isa MySQL.Connection || db isa ODBC.Connection
            result = DBInterface.execute(db, final_query)
            df_result = DataFrame(result)
        elseif current_sql_mode[] == :clickhouse
            df_result = ClickHouse.select_df(db, final_query)
            selected_columns_order = sq.metadata[sq.metadata.current_selxn .== 1, :name]
            df_result = df_result[:, selected_columns_order]
        elseif db isa GoogleSession{JSONCredentials}
                df_result = collect_gbq(sq.db, final_query)
        elseif db isa Oracle.Connection
            result = Oracle.query(db, final_query)
            df_result = DataFrame(result)
        elseif current_sql_mode[] == :athena
            exe_query = Athena.start_query_execution(final_query, sq.athena_params; aws_config = db)
                status = "RUNNING"
        while status in ["RUNNING", "QUEUED"]
            sleep(1)  # Wait for 1 second before checking the status again to avoid flooding the API
            query_status = Athena.get_query_execution(exe_query["QueryExecutionId"], sq.athena_params; aws_config = db)
            status = query_status["QueryExecution"]["Status"]["State"]
            if status == "FAILED"
                error("Query failed: ", query_status["QueryExecution"]["Status"]["StateChangeReason"])
            elseif status == "CANCELLED"
                error("Query was cancelled.")
            end
        end
        
        # Fetch the results once the query completes
        result = Athena.get_query_results(exe_query["QueryExecutionId"], sq.athena_params; aws_config = db)
            df_result = collect_athena(result)
        else
            error("Unsupported database type: $(typeof(db))")
        end
        df_result
    end
end

