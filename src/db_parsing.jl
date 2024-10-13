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
    exprs_iterable = isa(exprs, Tuple) ? collect(exprs) : exprs

    for expr in exprs_iterable
        if occursin(".", string(expr))
            push!(included_columns, string(expr))
            continue
        end
        is_excluded = isa(expr, Expr) && expr.head == :call && expr.args[1] == :(!)
        actual_expr = is_excluded ? expr.args[2] : expr

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
            if isa(actual_expr, Tuple) && length(actual_expr) == 1 && isa(actual_expr[1], Vector{Symbol})
            for item in actual_expr[1]
                col_name = string(item)
                if current_sql_mode[] == snowflake()
                    col_name = uppercase(col_name)
                end
                if is_excluded
                    push!(excluded_columns, col_name)
                else
                    push!(included_columns, col_name)
                end
            end

        end
    elseif isa(actual_expr, AbstractVector)
        for item in actual_expr
            col_name = string(item)
            if current_sql_mode[] == snowflake()
                col_name = uppercase(col_name)
            end
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
                    if current_sql_mode[] == snowflake()
                        col_name = uppercase(col_name)
                    end
                    if is_excluded
                        push!(excluded_columns, col_name)
                    else
                        push!(included_columns, col_name)
                    end
                end
            end
        elseif isa(actual_expr, Symbol) || isa(actual_expr, String)
            if occursin(".", string(actual_expr))
                push!(included_columns, string(actual_expr))
                continue
            end

            col_name = isa(actual_expr, Symbol) ? string(actual_expr) : actual_expr
            if current_sql_mode[] == snowflake()
                col_name = uppercase(col_name)
            end
            if is_excluded
                push!(excluded_columns, col_name)
            else
                push!(included_columns, col_name)
            end
        else
            error("Unsupported expression type: $expr")
        end
    end

    # Loop through excluded columns and update current_selxn to 0 in the metadata DataFrame
    for col_name in excluded_columns
        col_idx = findfirst(isequal(col_name), metadata.name)
        if !isnothing(col_idx)
            metadata.current_selxn[col_idx] = 0
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
        # Ensure we're dealing with an Expr object and it's a call to if_else
        if isa(x, Expr) && x.head == :call && x.args[1] == :if_else && length(x.args) == 4
            # Extract condition, true_case, and false_case from the arguments
            condition = x.args[2]
            true_case = x.args[3]
            false_case = x.args[4]

            # Check and handle `missing` cases and formatting for string literals
            true_case_formatted = (string(true_case) == "missing") ? "NULL" : (isa(true_case, String) ? "'$true_case'" : true_case)
            false_case_formatted = (string(false_case) == "missing") ? "NULL" : (isa(false_case, String) ? "'$false_case'" : false_case)

            # Construct the SQL CASE statement as a string
            sql_case = "CASE WHEN $(condition) THEN $(true_case_formatted) ELSE $(false_case_formatted) END"
            
            # Return just the string
            return sql_case
        else
            # Return the unmodified object if it's not an Expr or not an if_else call
            return x
        end
    end
    return transformed_expr
end


function parse_case_when(expr)
    MacroTools.postwalk(expr) do x
        # Ensure we're dealing with an Expr object
        if isa(x, Expr)
            # Check for a case_when expression
            if x.head == :call && x.args[1] == :case_when
                # Initialize components for building a SQL CASE expression
                sql_case_parts = ["CASE"]
                
                # Iterate through the arguments of the case_when call, skipping the function name
                for i in 2:2:length(x.args)-1
                    # Ensure we're only adding valid expressions
                    cond = x.args[i]
                    result = x.args[i + 1]

                    # Handle `missing` by converting it to `NULL`
                    result_formatted = if result === :missing
                        "NULL"
                    elseif isa(result, String)
                        "'$result'"
                    else
                        result
                    end

                    # Append the WHEN-THEN part to the SQL CASE expression
                    push!(sql_case_parts, "WHEN $(cond) THEN $(result_formatted)")
                end
                
                # Handle the default case, the last argument
                default_result = x.args[end]
                default_result_formatted = if default_result === :missing
                    "NULL"
                elseif isa(default_result, String)
                    "'$default_result'"
                else
                    default_result
                end

                # Append the ELSE part and the END
                push!(sql_case_parts, "ELSE $(default_result_formatted) END")
                
                # Combine into a complete SQL CASE statement
                sql_case = join(sql_case_parts, " ")
                
                # Directly return the SQL CASE statement string
                return sql_case
            end
        end
        # Return the unmodified object if it's not an Expr or not a case_when call
        return x
    end
end


#hacky, but only way i could figure out how to get
#the right syntax for starts_with, ends_with, contains
#this is different then the tidy_selection starts_with, ends_with, contains, 
#as that relies on matching column names from the metadata dataframe. 

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

    resolved_columns = parse_tidy_db(columns_exprs, metadata)
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


function parse_interpolation2(expr)
    MacroTools.postwalk(expr) do x
        if @capture(x, !!variable_Symbol)
            #variable_value = eval(variable)  # Evaluate to get the symbol or direct value
            #to avoid use of eval, this is a temp fix to enable Interpolation
            variable_value = haskey(GLOBAL_CONTEXT.variables, variable) ? GLOBAL_CONTEXT.variables[variable] : missing
            if isa(variable_value, AbstractVector) #&& all(isa(v, Symbol) for v in variable_value)
                if all(isa(v, String) for v in variable_value)
                    # Quote each string and join with commas for SQL IN clause
                    quoted_strings = map(v -> "'" * v * "'", variable_value)
                    return join(quoted_strings, ", ")
                
                else
                    column_names = map(v -> string(v), variable_value)  # This line is the critical change
                    column_names = map(v -> isa(v, Symbol) ? string(v) : v, variable_value)
                    return join(column_names, ", ")
                end
            elseif isa(variable_value, Symbol)
                return variable_value  
            elseif isa(variable_value, Number)
                return variable_value
            elseif isa(variable_value, AbstractVector)

                column_names = map(v -> isa(v, Symbol) ? string(v) : v, variable_value)
                return join(column_names, ", ")
            else
                return Symbol(variable_value)  # Convert other cases directly to Symbol
            end
        else
            return x
        end
    end
end
#my_var = "gear"
#my_var = :gear
#my_val = 3.7
#my_var = [:gear, :cyl]
#expr = :((!!my_var) * (!!my_val))
#parse_interpolation2(expr)

#expr = :((!!my_val) * (!!my_var))
#parse_interpolation2(expr)


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