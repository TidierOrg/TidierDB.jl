names_to_modify = ["str_replace", "str_replace_all", "str_remove", "str_remove_all", "replace_missing", "missing_if", 
                            "floor_date", "is_missing"]
#this function allows for @capture to capture names that would have an underscore, ie str_replace
function exc_capture_bug(expr, names_to_modify::Vector{String})
    names_set = Set(names_to_modify)

    function modify_expr(e)
        if isa(e, Expr)
            if e.head == :call && string(e.args[1]) in names_set
                e.args[1] = Symbol(replace(string(e.args[1]), "_" => ""))
            end
            for i in 2:length(e.args)
                e.args[i] = modify_expr(e.args[i])  # Apply modify_expr recursively
            end
        end
        
        return e
    end
    
    return modify_expr(expr)
end

function parse_tidy_db(exprs, metadata::DataFrame)
    all_columns = metadata[!, "name"]
    included_columns = String[]  # Start with an empty list for explicit inclusions
    excluded_columns = String[]

    # Convert tuple to vector if necessary
    exprs_iterable = isa(exprs, Tuple) || isa(exprs, AbstractVector) ? exprs : [exprs]

    for expr in exprs_iterable
        # First, check for exclusion
        is_excluded = isa(expr, Expr) && expr.head == :call && expr.args[1] == :(!)
        actual_expr = is_excluded ? expr.args[2] : expr

        if occursin(".", string(actual_expr))
            if is_excluded
                push!(excluded_columns, string(actual_expr))
            else
                push!(included_columns, string(actual_expr))
            end
            continue
        end

        if isa(actual_expr, Expr) && actual_expr.head == :call
            if actual_expr.args[1] == :(:)
                # Handle range expression
                start_col = string(actual_expr.args[2])
                if current_sql_mode[] == snowflake()
                    start_col = uppercase(start_col)
                end
                end_col = string(actual_expr.args[3])
                if current_sql_mode[] == snowflake()
                    end_col = uppercase(end_col)
                end
                start_idx = findfirst(==(start_col), all_columns)
                end_idx = findfirst(==(end_col), all_columns)
                if isnothing(start_idx) || isnothing(end_idx) || start_idx > end_idx
                    error("Column range not found or invalid: $start_col to $end_col")
                end
                range_columns = all_columns[start_idx:end_idx]
                if is_excluded
                    excluded_columns = union(excluded_columns, range_columns)
                else
                    append!(included_columns, range_columns)
                end
            elseif actual_expr.args[1] == :starts_with || actual_expr.args[1] == :ends_with || actual_expr.args[1] == :contains
                # Handle starts_with, ends_with, and contains
                substring = actual_expr.args[2]
                if current_sql_mode[] == snowflake()
                    substring = uppercase(substring)
                end
                match_columns = filter(col -> 
                    (actual_expr.args[1] == :starts_with && startswith(col, substring)) ||
                    (actual_expr.args[1] == :ends_with && endswith(col, substring)) ||
                    (actual_expr.args[1] == :contains && occursin(substring, col)),
                    all_columns)
                if is_excluded
                    excluded_columns = union(excluded_columns, match_columns)
                else
                    append!(included_columns, match_columns)
                end
            else
                error("Unsupported function call: $(actual_expr.args[1])")
            end
        elseif isa(actual_expr, Symbol) || isa(actual_expr, String)
            # Handle single column name
            if occursin(".", string(actual_expr))
                is_excluded ? push!(excluded_columns, string(actual_expr)) : push!(included_columns, string(actual_expr))
                continue
            end

            col_name = isa(actual_expr, Symbol) ? string(actual_expr) : actual_expr
            col_name = current_sql_mode[] == snowflake() ? uppercase(col_name) : col_name
            if is_excluded
                push!(excluded_columns, col_name)
            else
                push!(included_columns, col_name)
            end
        elseif isa(actual_expr, Expr) && actual_expr.head == :vect
            for item in actual_expr.args
                
                col_name = string(item)[2:end]
                if current_sql_mode[] == snowflake()
                    col_name = uppercase(col_name)
                end
                if is_excluded
                    push!(excluded_columns, col_name)
                else
                    push!(included_columns, col_name)
                end
            end


     
# COV_EXCL_START
    elseif isa(actual_expr, AbstractVector)
        for item in actual_expr
            col_name = string(item)
            col_name = current_sql_mode[] == snowflake() ? uppercase(col_name) : col_name

            if is_excluded
                push!(excluded_columns, col_name)
            else
                push!(included_columns, col_name)
            end
        end
        elseif isa(actual_expr, Tuple) && all(isa.(actual_expr, Vector{Symbol}))
            for vec in actual_expr
                for item in vec
                    col_name = string(item)[2:end]
                    col_name = current_sql_mode[] == snowflake() ? uppercase(col_name) : col_name
                    is_excluded ? push!(excluded_columns, col_name) : push!(included_columns, col_name)
                end
            end
       
        else
            error("Unsupported expression type: $expr")
        end

    end
# COV_EXCL_STOP
    # Loop through excluded columns and update current_selxn to 0 in the metadata DataFrame
    for col_name in excluded_columns
        if occursin(".", col_name)
            # Split into table and column
            table_col_split = split(col_name, ".")
            table_name, col_name = table_col_split[1], table_col_split[2]
            # Find indices where both table_name and column name match
            col_indices = findall((metadata.table_name .== table_name) .& (metadata.name .== col_name))
        else
            # Exclude all columns with the matching name regardless of table
            col_indices = findall(metadata.name .== col_name)
        end
        if !isempty(col_indices)
            metadata.current_selxn[col_indices] .= 0
        end
    end

    # If no columns are explicitly included, default to all columns (with current_selxn == 1) minus any exclusions
    if isempty(included_columns)
        included_columns = metadata.name[metadata.current_selxn .== 1]
        included_columns = setdiff(included_columns, excluded_columns)
    else
        included_columns = setdiff(included_columns, excluded_columns)
    end

    return included_columns
end


function parse_if_else(expr)
    transformed_expr = MacroTools.postwalk(expr) do x
        # Check if the expression is a call to if_else
        if isa(x, Expr) && x.head == :call && x.args[1] == :if_else
            args_length = length(x.args)

            # Handle 4-argument if_else
            if args_length == 4
                condition = x.args[2]
                true_case = x.args[3]
                false_case = x.args[4]

                # Format true_case
                true_case_formatted = string(true_case) == "missing" ? "NULL" :
                                      isa(true_case, String) ? "'$true_case'" : true_case

                # Format false_case
                false_case_formatted = string(false_case) == "missing" ? "NULL" :
                                       isa(false_case, String) ? "'$false_case'" : false_case

                # Construct SQL CASE WHEN statement
                sql_case = "CASE WHEN $(condition) THEN $(true_case_formatted) ELSE $(false_case_formatted) END"

                return sql_case

            # Handle 5-argument if_else
            elseif args_length == 5
                condition = x.args[2]
                true_case = x.args[3]
                false_case = x.args[4]
                missing_case = x.args[5]

                # Format true_case
                true_case_formatted = string(true_case) == "missing" ? "NULL" :
                                      isa(true_case, String) ? "'$true_case'" : true_case

                # Format false_case
                false_case_formatted = string(false_case) == "missing" ? "NULL" :
                                       isa(false_case, String) ? "'$false_case'" : false_case

                # Format missing_case
                missing_case_formatted = string(missing_case) == "missing" ? "NULL" :
                                         isa(missing_case, String) ? "'$missing_case'" : missing_case

                # Construct SQL CASE WHEN statement
                sql_case = "CASE WHEN $(condition) THEN $(true_case_formatted) ELSE $(false_case_formatted) END"

                # Wrap the CASE statement to handle the missing_case
                # This ensures that if the result of CASE is NULL, it remains NULL
                sql_case_with_missing = "CASE WHEN ($sql_case) IS NULL THEN $(missing_case_formatted) ELSE ($sql_case) END"

                return sql_case_with_missing

            else
                # Unsupported number of arguments; return as is
                return x
            end
        else
            # Not an if_else call; return as is
            return x
        end
    end
    return transformed_expr
end



function parse_case_when(expr)
    MacroTools.postwalk(expr) do x
        if isa(x, Expr) && x.head == :call && x.args[1] == :case_when
            sql_parts = ["CASE"]
            args      = x.args[2:end]

            expanded = Any[]           
            default  = nothing


            i = 1
            while i ≤ length(args)
                arg = args[i]

                if isa(arg, Expr) && arg.head == :call && arg.args[1] == :(=>)
                    push!(expanded, arg.args[2], arg.args[3])
                    i += 1

                elseif isa(arg, Pair)
                    push!(expanded, arg.first, arg.second)
                    i += 1

                else
                    if i == length(args)          
                        default = arg
                        i += 1
                    else
                        push!(expanded, arg, args[i + 1])
                        i += 2
                    end
                end
            end

            for j in 1:2:length(expanded)
                cond   = expanded[j]
                result = expanded[j + 1]

                res_sql = result === :missing ? "NULL" :
                          isa(result, String) ? "'$result'" : result
                push!(sql_parts, "WHEN $(cond) THEN $(res_sql)")
            end

            if default !== nothing
                def_sql = default === :missing ? "NULL" :
                          isa(default, String) ? "'$default'" : default
                push!(sql_parts, "ELSE $(def_sql)")
            end

            push!(sql_parts, "END")
            return join(sql_parts, " ")
        end
        return x
    end
end



#this fxn is not being tested, bc its only in backends. - i might be able to get rid of it entirely as well
# COV_EXCL_START
function parse_char_matching(expr) 
    MacroTools.postwalk(expr) do x
        if isa(x, Expr) && x.head == :call
            if x.args[1] == :! && length(x.args) == 2 && isa(x.args[2], Expr) && x.args[2].head == :call
                # Handle negation case
                inner_func = x.args[2].args[1]
                if inner_func in (:starts_with, :ends_with, :contains)
                    column = x.args[2].args[2]
                    pattern = x.args[2].args[3]
                    if current_sql_mode[] == clickhouse()
                        like_expr = if inner_func == :starts_with 
                            "NOT startsWith($(column), '$(pattern)')"
                        elseif inner_func == :ends_with
                            "NOT endsWith($(column), '$(pattern)')"
                        elseif inner_func == :contains
                            "NOT position($(column), '$(pattern)') > 0"
                        end
                    else
                        like_expr = if inner_func == :starts_with 
                            "$(column) NOT LIKE '$(pattern)%'"
                        elseif inner_func == :ends_with
                            "$(column) NOT LIKE '%$(pattern)'"
                        elseif inner_func == :contains
                            "$(column) NOT LIKE '%$(pattern)%'"
                        end
                    end
                    return like_expr
                end
            elseif x.args[1] in (:starts_with, :ends_with, :contains)
                # Handle positive case
                column = x.args[2]
                pattern = x.args[3]
                if current_sql_mode[] == clickhouse()
                    like_expr = if x.args[1] == :starts_with 
                        "startsWith($(column), '$(pattern)')"
                    elseif x.args[1] == :ends_with
                        "endsWith($(column), '$(pattern)')"
                    elseif x.args[1] == :contains
                        "position($(column), '$(pattern)') > 0"
                    end
                else
                    like_expr = if x.args[1] == :starts_with 
                        "$(column) LIKE '$(pattern)%'"
                    elseif x.args[1] == :ends_with
                        "$(column) LIKE '%$(pattern)'"
                    elseif x.args[1] == :contains
                        "$(column) LIKE '%$(pattern)%'"
                    end
                end
                return like_expr
            end
        end
        return x  # Return the expression unchanged if no specific handling applies
    end
end
# COV_EXCL_STOP


function parse_across(expr, metadata)
    columns_expr, funcs_expr = expr.args[2], expr.args[3]
    
    # Existing column selection logic remains unchanged
    if isa(columns_expr, String)
        columns_exprs = map(Symbol, split(strip(columns_expr), ","))
    elseif isa(columns_expr, Expr) && columns_expr.head == :tuple
        columns_exprs = columns_expr.args
    else
        columns_exprs = [columns_expr]
    end
   # metadata = metadata[metadata.current_selxn .>= 1, :]
    resolved_columns = parse_tidy_db(columns_exprs, metadata)
   # println(resolved_columns)
  #  filtered_names = metadata.name[metadata.current_selxn .>= 1]
  #  resolved_columns = intersect(resolved_columns, filtered_names)
  #  println(resolved_columns)

    funcs = isa(funcs_expr, Expr) && funcs_expr.head == :tuple ? funcs_expr.args : [funcs_expr]
    result_exprs = []

    for func in funcs
        for col_name in resolved_columns
            col_symbol = Meta.parse(col_name)  # Convert string back to symbol
            func_filled = insert_col_into_func(func, col_symbol)
            # Specify "agg" to be skipped in the result name
            func_name_str = generate_func_name(func, ["agg"])
            result_name = Symbol(col_name, "_", func_name_str)
            new_expr = Expr(:(=), result_name, func_filled)
            push!(result_exprs, new_expr)
        end
    end

    combined_expr = Expr(:tuple, result_exprs...)
    return combined_expr
end

function insert_col_into_func(func_expr, col_symbol)
    if isa(func_expr, Symbol)
        # Simple function name; create a call with the column symbol
        return Expr(:call, func_expr, col_symbol)
    elseif isa(func_expr, Expr) && func_expr.head == :call
        # Function call; recursively insert the column symbol into arguments
        func_name = func_expr.args[1]
        args = func_expr.args[2:end]
        new_args = [insert_col_into_func(arg, col_symbol) for arg in args]
        return Expr(:call, func_name, new_args...)
    else
        # Other expressions; return as-is
        return func_expr
    end
end
function generate_func_name(func_expr, skip_funcs=String[])
    if isa(func_expr, Symbol)
        return string(func_expr)
    elseif isa(func_expr, Expr) && func_expr.head == :call
        func_name_expr = func_expr.args[1]
        if isa(func_name_expr, Symbol)
            func_name = string(func_name_expr)
        else
            func_name = generate_func_name(func_name_expr, skip_funcs)
        end
        # Process nested function names
        nested_names = [generate_func_name(arg, skip_funcs) for arg in func_expr.args[2:end]]
        # Exclude function names in skip_funcs
        if func_name in skip_funcs
            # Skip adding this function name
            return join(nested_names, "_")
        else
            # Remove empty strings from nested_names
            nested_names = filter(n -> n != "", nested_names)
            return join([func_name; nested_names], "_")
        end
    else
        return ""
    end
end

function parse_blocks(exprs...)
    if length(exprs) == 1 && hasproperty(exprs[1], :head) && exprs[1].head == :block
      return (MacroTools.rmlines(exprs[1]).args...,)
    end
    return exprs
end


function construct_window_clause(sq::SQLQuery ; from_cumsum::Bool = false)
    # Construct the partition clause, considering both groupBy and window_order
    partition_clause = !isempty(sq.groupBy) ? "PARTITION BY $(sq.groupBy)" : ""
    if !isempty(sq.window_order)
        # If there's already a partition clause, append the order clause; otherwise, start with ORDER BY
        order_clause = !isempty(partition_clause) ? " ORDER BY $(sq.window_order)" : "ORDER BY $(sq.window_order)"
    else
        order_clause = ""
    end
    if from_cumsum == true
        frame_clause = "ROWS UNBOUNDED PRECEDING "
    else 
        frame_clause = !isempty(sq.windowFrame) ? sq.windowFrame : ""
    end
    # Combine partition, order, and frame clauses for the complete window function clause
    # Ensure to include space only when needed to avoid syntax issues
    partition_and_order_clause = partition_clause * (!isempty(order_clause) ? " " * order_clause : "")
    window_clause = (!isempty(partition_clause) || !isempty(order_clause) || !isempty(frame_clause))  ? "OVER ($partition_and_order_clause $frame_clause)" : "OVER ()"

    return window_clause
end

function parse_join_expression(expr)
    # We’ll store everything we find in these:
    lhs_cols  = String[]
    rhs_cols  = String[]
    operators = String[]
    closests  = String[]
    as_of     = ""

    # Check for `closest(...)`
    if isa(expr, Expr) && expr.head == :call && expr.args[1] == :closest
        as_of = " ASOF "

        # The inside of closest(...) is expr.args[2], e.g. :(sale_date <= promo_date)
        inside_expr = expr.args[2]
        if isa(inside_expr, Expr) && inside_expr.head == :call && (
            inside_expr.args[1] in (Symbol("=="), Symbol(">="), Symbol("<="), 
                                    Symbol("!="), Symbol(">"), Symbol("<"))
        )
            push!(operators, string(inside_expr.args[1]))
            push!(lhs_cols, string(inside_expr.args[2]))
            push!(rhs_cols, string(inside_expr.args[3]))
            push!(closests, string(expr))  # Optional: Record closest() for debugging
        else
            error("closest(...) must wrap an operator expression like sale_date <= promo_date")
        end

        return lhs_cols, rhs_cols, operators, closests, as_of
    end

    # Handle single operator-based expressions (e.g., sale_date <= promo_date)
    if isa(expr, Expr) && expr.head == :call && expr.args[1] in (
        Symbol("=="), Symbol(">="), Symbol("<="), Symbol("!="), Symbol(">"), Symbol("<"), Symbol("=")
    ) 
        push!(operators, string(expr.args[1]))
        push!(lhs_cols, string(expr.args[2]))
        push!(rhs_cols, string(expr.args[3]))
        return lhs_cols, rhs_cols, operators, closests, as_of
    elseif isa(expr, Expr) && expr.head == :(=)
            # The user wrote: id = id2
            # Typically we'd interpret that as an SQL "id = id2"
            push!(operators, "==")
            push!(lhs_cols, string(expr.args[1]))
            push!(rhs_cols, string(expr.args[2]))
            return lhs_cols, rhs_cols, operators, closests, as_of
        
    end

    # Handle single bare column (e.g., id)
    if isa(expr, Symbol)
        return [string(expr)], [string(expr)], ["=="], closests, as_of
    end
    
        error("Unsupported join expression: $expr")
end


# This function allows for tidy selection in relocate, and prob could be used in the _by argument as well
# COV_EXCL_START
function filter_columns_by_expr(actual_expr, metadata::DataFrame)
    # Filter metadata by current_selxn != 0
    selected_df = metadata[metadata.current_selxn .!= 0, :]
    all_columns = selected_df.name
    function maybe_uppercase(s)
        if current_sql_mode[] == snowflake()
            return uppercase(s)
        else
            return s
        end
    end

    # If actual_expr is a symbol that looks like ends_with("d"), etc., try parsing it as an expression
    if isa(actual_expr, Symbol)
        sym_str = string(actual_expr)
        parsed = Meta.parse(sym_str)
        if isa(parsed, Expr) && parsed.head == :call
            actual_expr = parsed
        end
    end

    # If actual_expr is a vector, process each element individually
    if isa(actual_expr, AbstractVector)
        final_columns = String[]
        for elem in actual_expr
            if isa(elem, AbstractVector)
                # elem is directly a vector of symbols like [:groups, :value]
                col_strs = string.(elem)
                local_cols = all_columns
                if current_sql_mode[] == snowflake()
                    col_strs = uppercase.(col_strs)
                    local_cols = uppercase.(local_cols)
                end
                # Check if all requested columns exist
                missing_cols = setdiff(col_strs, intersect(col_strs, local_cols))
                if !isempty(missing_cols)
                    error("The following columns were not found: $(missing_cols)")
                end
                append!(final_columns, col_strs)
            elseif isa(elem, Symbol)
                # elem is a single symbol, try parsing
                elem_str = string(elem)
                parsed = Meta.parse(elem_str)
                if isa(parsed, Expr) && parsed.head == :vect
                    # It's a vector expression like [groups, value]
                    col_syms = parsed.args
                    col_strs = string.(col_syms)
                    local_cols = all_columns
                    if current_sql_mode[] == snowflake()
                        col_strs = uppercase.(col_strs)
                        local_cols = uppercase.(local_cols)
                    end
                    col_strs = replace.(col_strs, ":"=> "")
                    missing_cols = setdiff(col_strs, intersect(col_strs, local_cols))
                    if !isempty(missing_cols)
                        error("The following columns were not found: $(missing_cols)")
                    end
                    append!(final_columns, col_strs)
                elseif isa(parsed, Expr) && parsed.head == :call
                    func = parsed.args[1]
                    if func == :(:)
                        # Handle range expression like id:groups
                        start_col = string(parsed.args[2])
                        end_col = string(parsed.args[3])
                        if current_sql_mode[] == snowflake()
                            start_col = uppercase(start_col)
                            end_col = uppercase(end_col)
                            all_columns = uppercase.(all_columns)
                        end
                        start_idx = findfirst(==(start_col), all_columns)
                        end_idx = findfirst(==(end_col), all_columns)
                        if isnothing(start_idx) || isnothing(end_idx) || start_idx > end_idx
                            error("Column range not found or invalid: $start_col to $end_col")
                        end
                        range_columns = all_columns[start_idx:end_idx]
                        append!(final_columns, range_columns)
                    elseif isa(parsed, Expr) && parsed.head == :call
                        # It's a function call expression like ends_with("d")
                        func = parsed.args[1]
                        if func == :starts_with || func == :ends_with || func == :contains
                            substring = string(parsed.args[2])
                            substring = maybe_uppercase(substring)
                            local_cols = current_sql_mode[] == snowflake() ? uppercase.(all_columns) : all_columns
                            match_columns = filter(col ->
                                (func == :starts_with && startswith(col, substring)) ||
                                (func == :ends_with && endswith(col, substring)) ||
                                (func == :contains && occursin(substring, col)),
                                local_cols)
                            append!(final_columns, match_columns)
                        else
                            error("Unsupported function call: $(func)")
                        end
                    end
                else
                    # Treat as a direct column reference
                    local_cols = all_columns
                    if current_sql_mode[] == snowflake()
                        elem_str = uppercase(elem_str)
                        local_cols = uppercase.(local_cols)
                    end
                    elem_str = replace(elem_str, ":" => "")
                    if elem_str in local_cols
                        push!(final_columns, elem_str)
                    else
                        error("The following columns were not found: [$elem_str]")
                    end
                end
            elseif isa(elem, String)
                # If the string starts with function call syntax, process it as a function call
                if startswith(elem, "starts_with(") || startswith(elem, "ends_with(") || startswith(elem, "contains(")
                    parsed = Meta.parse(elem)
                    if isa(parsed, Expr) && parsed.head == :call
                        func = parsed.args[1]
                        if func == :starts_with || func == :ends_with || func == :contains
                            substring = string(parsed.args[2])
                            substring = maybe_uppercase(substring)
                            local_cols = current_sql_mode[] == snowflake() ? uppercase.(all_columns) : all_columns
                            match_columns = filter(col ->
                                (func == :starts_with && startswith(col, substring)) ||
                                (func == :ends_with && endswith(col, substring)) ||
                                (func == :contains && occursin(substring, col)),
                                local_cols)
                            append!(final_columns, match_columns)
                        else
                            error("Unsupported function call: $(func)")
                        end
                    else
                        error("Invalid function expression: $elem")
                    end
                   elseif startswith(elem, ":") 
                        col = replace(elem, ":" => "")
                        local_cols = all_columns
                        if current_sql_mode[] == snowflake()
                            col = uppercase(col)
                            local_cols = uppercase.(local_cols)
                        end
                        idx = findfirst(==(col), local_cols)
                        if isnothing(idx)
                            error("Column not found: $col")
                        end
                        push!(final_columns, local_cols[idx])
                    elseif occursin(":", elem)
                        parts = split(elem, ":")
                        start_col, end_col = parts[1], parts[2]
                        local_cols = all_columns
                        if current_sql_mode[] == snowflake()
                            start_col = uppercase(start_col)
                            end_col = uppercase(end_col)
                            local_cols = uppercase.(local_cols)
                        end
                        start_idx = findfirst(==(start_col), local_cols)
                        end_idx = findfirst(==(end_col), local_cols)
                        if isnothing(start_idx) || isnothing(end_idx) || start_idx > end_idx
                            error("Column range not found or invalid: $start_col to $end_col")
                        end
                        range_columns = local_cols[start_idx:end_idx]
                        append!(final_columns, range_columns)
            
                else
                    # Treat as a direct column reference
                    local_cols = all_columns
                    if current_sql_mode[] == snowflake()
                        elem_upper = uppercase(elem)
                        local_cols = uppercase.(local_cols)
                    else
                        elem_upper = elem
                    end
                    if elem_upper in local_cols
                        push!(final_columns, elem_upper)
                    else
                        error("The following column was not found: [$elem]")
                    end
                end
            else
                println(final_columns)
            end
        end
        return final_columns
    end
end

# COV_EXCL_STOP