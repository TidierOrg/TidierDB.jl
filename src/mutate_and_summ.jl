function parse_mutate(mutations)
    grouping_var  = nothing
    order_var     = nothing
    frame_var     = nothing
    new_mutations = Expr[]

    # treat “a = b” and keyword-style “a = b” the same
    _isassign(e) = isa(e, Expr) && e.head in (:(=), :kw)

    for expr in mutations
        # ------------------------------------------------------- _by
        if _isassign(expr) && expr.args[1] == :_by
            arg = expr.args[2]
            grouping_var = if isa(arg, Expr) && arg.head == :vect
                join(symbol_to_string.(arg.args), ", ")
            else
                symbol_to_string(arg)
            end

        # ---------------------------------------------------- _order
        elseif _isassign(expr) && expr.args[1] == :_order
            arg = expr.args[2]
            if isa(arg, Expr) && arg.head == :vect
                order_var = join(symbol_to_string.(arg.args), ", ")
            elseif isa(arg, Expr) && arg.head == :call && arg.args[1] == :desc
                inner = arg.args[2]
                order_var = "DESC " *
                            (isa(inner, Expr) && inner.head == :vect ?
                                join(symbol_to_string.(inner.args), ", ")
                              : symbol_to_string(inner))
            else
                order_var = symbol_to_string(arg)
            end

        # ---------------------------------------------------- _frame
        elseif _isassign(expr) && expr.args[1] == :_frame
            arg = expr.args[2]
            frame_var = if isa(arg, Expr) && arg.head == :vect
                join(symbol_to_string.(arg.args), ", ")
            else
                arg   # keep tuple / literal intact (avoids String–Int errors)
            end

        # ------------------------------------------ regular mutations
        else
            push!(new_mutations, expr)
        end
    end

    return grouping_var, new_mutations, order_var, frame_var
end

function symbol_to_string(s)
    if isa(s, Symbol)
        return string(s)
    elseif isa(s, String)
        return s # COV_EXCL_LINE
    elseif isa(s, QuoteNode) && isa(s.value, Symbol)
        return string(s.value)
    else
        return s
    end
end

function process_mutate_expression(expr, sq, select_expressions, cte_name; from_transmute::Bool = false)

    if isa(expr, Expr) && expr.head == :(=) && isa(expr.args[1], Symbol)
        # Extract column name and convert to string
        col_name = string(expr.args[1])
        if current_sql_mode[] == snowflake()
            col_name = uppercase(col_name) # COV_EXCL_LINE
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
    # COV_EXCL_START
    elseif from_transmute
        col_expr = expr_to_sql(expr, sq)
        push!(select_expressions, col_expr)
        meta = DataFrame(DBInterface.execute(sq.db,"""
          DESCRIBE  SELECT  u.* FROM (SELECT UNNEST(items) AS u FROM $(sq.from))"""))
        for n in 1:nrow(meta)
            push!(sq.metadata, Dict("name" => meta.column_name[n], "type" => meta.column_type[n] , "current_selxn" => 1, "table_name" => cte_name))
        end

     # COV_EXCL_END
    else
        throw("Unsupported expression format in @mutate: $(expr)") # COV_EXCL_LINE
    end
end

"""
$docstring_mutate
"""
macro mutate(sqlquery, mutations...)
    grouping_var, mutations, order_var, frame_var = parse_mutate(mutations)
    mutations = parse_blocks(mutations...)

    return quote
        sq = t($(esc(sqlquery)))
        sq.post_mutate = true
        if isa(sq, SQLQuery)
            cte_name = "cte_" * string(sq.cte_count + 1)

            if sq.post_aggregation || sq.post_unnest || sq.post_count#|| sq.post_join 
                if sq.post_aggregation
                    for row in eachrow(sq.metadata)
                        if row[:current_selxn] == 2
                            row[:current_selxn] = 1
                        end
                    end
                end
                sq.post_aggregation = false
               
                select_expressions = !isempty(sq.select) ? [sq.select] : ["*"]

                local cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
                if sq.is_aggregated && !isempty(sq.groupBy)
                    cte_sql *= " " * sq.groupBy
                    sq.groupBy = ""
                end
                if !isempty(sq.where)
                    cte_sql *= " WHERE " * sq.where
                    sq.where = ""
                end
                if !isempty(sq.having)
                    cte_sql *= "  " * sq.having
                    sq.having = ""
                end

                # Create and add the new CTE
                new_cte = CTE(name=string(cte_name), select=cte_sql)
                up_cte_name(sq, string(cte_name))
                
                push!(sq.ctes, new_cte)
                sq.cte_count += 1
                sq.from = string(cte_name)
                sq.post_count = false
                
            else
              #  sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                
            end
            cte_name = "cte_" * string(sq.cte_count + 1)
            sq.cte_count += 1

            select_expressions = ["*"]
            most_recent_source = "cte_" * string(sq.cte_count - 1) 
            if !isempty(sq.ctes) && most_recent_source != "cte_0"
                all_columns = [
                    (row[:current_selxn] == 1 ? row[:name] : most_recent_source * "." * row[:name])
                    for row in eachrow(sq.metadata) if row[:current_selxn] != 0
                ]      
                    else
                    all_columns = [
                        (row[:current_selxn] == 1 ? row[:name] : row[:table_name] * "." * row[:name])
                        for row in eachrow(sq.metadata) if row[:current_selxn] != 0
                    ]        
                end    

            select_expressions = [col for col in all_columns]  # Start with all currently selected columns

            if $(esc(grouping_var)) != nothing
                group_vars = $(esc(grouping_var))
                group_vars_sql = expr_to_sql(group_vars, sq)
                sq.groupBy = "GROUP BY " * string(group_vars_sql)
            end

            if $(esc(order_var)) != nothing
               TidierDB.@window_order(sq, ($order_var))
            end

            if $(esc(frame_var)) != nothing
                 TidierDB.@window_frame(sq, ($frame_var))
              end

            for expr in $mutations
                # Transform 'across' expressions first
                if isa(expr, Expr) && expr.head == :call && expr.args[1] == :across
                    expr = parse_across(expr, sq.metadata)
                end
                if isa(expr, Expr) && expr.head == :tuple
                    for subexpr in expr.args
                        process_mutate_expression(subexpr, sq, select_expressions, cte_name)
                    end
                else
                    process_mutate_expression(expr, sq, select_expressions, cte_name)
                end
            end
            sq.post_join = false

            if $(esc(grouping_var)) != nothing
                sq.groupBy = ""
            end
     
                # Construct CTE SQL, handling aggregated queries differently
            local cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
            if sq.groupBy_exprs
                paren_sql, aliases = extract_paren_aliases(sq.select)
                for (i, alias) in enumerate(aliases)
                    pattern = Regex(", " * alias * "\\b")
                    cte_sql = replace(cte_sql, pattern => ", " * paren_sql[i], count=1)
                end
                sq.groupBy_exprs = false
            end
            
            if sq.is_aggregated
                cte_sql *= " " * sq.groupBy
                sq.is_aggregated = false
            end
            if !isempty(sq.where)
                cte_sql *= " WHERE " * sq.where
                sq.where = ""
            end

            new_cte = CTE(name=string(cte_name), select=cte_sql)
            up_cte_name(sq, cte_name)
            push!(sq.ctes, new_cte)


            sq.from = string(cte_name)

            sq.select = "*"
            if _warning_[]
                if sq.groupBy != "" || sq.window_order != "" || sq.windowFrame != ""
                    @warn "After applying all mutations, @mutate removed grouping and window clauses."
                end
            end
            sq.groupBy = ""
            sq.windowFrame = ""
            sq.window_order = ""
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

### SUMMARIZE 
function parse_by(mutations)
    grouping_var = nothing
    new_mutations = []
    for expr in mutations
        if isa(expr, Expr) && expr.head == :(=) && expr.args[1] == :_by
            arg2 = expr.args[2]
            if isa(arg2, Expr) && arg2.head == :vect
                grouping_var = join([symbol_to_string(arg) for arg in arg2.args], ", ")
            else
                grouping_var = symbol_to_string(arg2)
            end
        else
            push!(new_mutations, expr)
        end
    end
    return grouping_var, new_mutations
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
        throw("Unsupported expression format in @summarize: $(expr)") # COV_EXCL_LINE
    end
end

"""
$docstring_summarize
"""
macro summarize(sqlquery, expressions...)
    # Extract the `by` argument
    grouping_var, expressions = parse_by(expressions)
    expressions = parse_blocks(expressions...)

    return quote
        sq = t($(esc(sqlquery)))
        if isa(sq, SQLQuery)
            summary_str = String[]
            sq.metadata.current_selxn .= 0
            if sq.is_aggregated 
                build_cte!(sq)
                sq.select = ""
            end
            # Set the grouping variable if `by` is provided
            if $(esc(grouping_var)) != nothing
                group_vars = $(esc(grouping_var))
                if isa(group_vars, String)
                    sq.groupBy = "GROUP BY " * group_vars
                elseif isa(group_vars, AbstractArray)
                    sq.groupBy = "GROUP BY " * join(group_vars, ", ")
                else
                    sq.groupBy = "GROUP BY " * string(group_vars)
                end
                sq.is_aggregated = true
            end

            # Update metadata for grouping columns
            if !isempty(sq.groupBy)
                
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
            end

            # Process the summary expressions
            for expr in $expressions
                # Transform 'across' expressions first
                if isa(expr, Expr) && expr.head == :call && expr.args[1] == :across
                    expr = parse_across(expr, sq.metadata)
                end
                if isa(expr, Expr) && expr.head == :tuple
                    for subexpr in expr.args
                        process_summary_expression(subexpr, sq, summary_str)
                    end
                else
                    process_summary_expression(expr, sq, summary_str)
                end
            end

                        # Construct the SELECT clause
            # Construct the SELECT clause
            summary_clause   = join(summary_str, ", ")
            existing_select  = strip(sq.select)

            # helper: split SELECT list on top-level commas
            function _split_top_level_commas_str(s::AbstractString)
                ss = String(s)
                parts = String[]
                buf = IOBuffer()
                depth = 0
                i = firstindex(ss)
                while i <= lastindex(ss)
                    c = ss[i]
                    if c == '('
                        depth += 1
                        write(buf, c)
                    elseif c == ')'
                        depth = max(depth - 1, 0)
                        write(buf, c)
                    elseif c == ',' && depth == 0
                        push!(parts, strip(String(take!(buf))))
                    else
                        write(buf, c)
                    end
                    i = nextind(ss, i)
                end
                push!(parts, strip(String(take!(buf))))
                return parts
            end

            # helper: ensure a column exists in metadata and is selected
            function _ensure_selected!(sq, colname::AbstractString)
                names = sq.metadata[!, :name]
                idx = findfirst(==(String(colname)), names)
                if idx === nothing
                    push!(sq.metadata, Dict("name" => String(colname),
                                            "type" => "UNKNOWN",
                                            "current_selxn" => 1,
                                            "table_name" => sq.from))
                else
                    sq.metadata[idx, :current_selxn] = 1
                end
            end

            # helper: after we set sq.select, mark any "... AS alias" as selected
            function _select_aliases_in_current_select!(sq)
                sel = strip(String(sq.select))
                if !startswith(uppercase(sel), "SELECT ")
                    return
                end
                body = sel[8:end]                # strip leading "SELECT "
                for item in _split_top_level_commas_str(body)
                    if (m = match(r"(?i)\bAS\s+([A-Za-z_][\w$]*)\s*$", item)) !== nothing
                        _ensure_selected!(sq, m.captures[1])
                    end
                end
            end

            if !isempty(existing_select) && startswith(uppercase(existing_select), "SELECT")
                # keep any existing projection (e.g., COALESCE(...) AS id, value2)
                if isempty(summary_clause)
                    sq.select = existing_select
                else
                    sq.select = existing_select * ", " * summary_clause
                end
                _select_aliases_in_current_select!(sq)

            elseif !isempty(sq.groupBy)
                # no existing SELECT (e.g., it was finalized into a CTE): project the GROUP BY expressions
                # and give them clean aliases where possible
                gb = replace(sq.groupBy, "GROUP BY " => "")

                # derive a safe alias when possible
                function _alias_for_group_expr(e::AbstractString)
                    s = strip(String(e))
                    # COALESCE(a.col1, b.col2)  -> alias "col1" (always use the left-hand column name)
                    m = match(r"(?i)^\s*COALESCE\s*\(\s*[A-Za-z_][\w$]*\.([A-Za-z_][\w$]*)\s*,\s*[A-Za-z_][\w$]*\.([A-Za-z_][\w$]*)\s*\)\s*$", s)
                    if m !== nothing
                        return m.captures[1]
                    end
                    # qualified name a.col -> alias "col"
                    m2 = match(r"^\s*[A-Za-z_][\w$]*\.([A-Za-z_][\w$]*)\s*$", s)
                    if m2 !== nothing
                        return m2.captures[1]
                    end
                    return ""  # no clean alias
                end


                items = _split_top_level_commas_str(gb)
                proj  = String[]
                for it in items
                    alias = _alias_for_group_expr(it)
                    if !isempty(alias)
                        push!(proj, string(it, " AS ", alias))
                    else
                        push!(proj, it)
                    end
                end

                if isempty(summary_clause)
                    sq.select = "SELECT " * join(proj, ", ")
                else
                    sq.select = "SELECT " * join(proj, ", ") * ", " * summary_clause
                end
                _select_aliases_in_current_select!(sq)   # marks "id" selected

            elseif sq.groupBy_exprs
                # expression-style group_by previously appended to sq.select
                if isempty(summary_clause)
                    # keep as-is
                else
                    sq.select *= ", " * summary_clause
                end
                _select_aliases_in_current_select!(sq)
            else
                # pure aggregation with no grouping
                sq.select = "SELECT " * summary_clause
                _select_aliases_in_current_select!(sq)
            end

            sq.is_aggregated = true        # Mark the query as aggregated
            sq.post_aggregation = true     # Indicate ready for post-aggregation operations


            sq.is_aggregated = true        # Mark the query as aggregated
            sq.post_aggregation = true     # Indicate ready for post-aggregation operations
            
        else
            error("Expected sqlquery to be an instance of SQLQuery") # COV_EXCL_LINE
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
$docstring_transmute
"""
macro transmute(sqlquery, mutations...)
    grouping_var, mutations, order_var, frame_var = parse_mutate(mutations)
    mutations = parse_blocks(mutations...)

    return quote
        sq = t($(esc(sqlquery)))
        if isa(sq, SQLQuery)
            cte_name = "cte_" * string(sq.cte_count + 1)

            if sq.post_aggregation || sq.post_unnest #|| sq.post_join 
                if sq.post_aggregation
                    for row in eachrow(sq.metadata)
                        if row[:current_selxn] == 2
                            row[:current_selxn] = 1
                        end
                    end
                end
                sq.post_aggregation = false
               
                select_expressions = !isempty(sq.select) ? [sq.select] : ["*"]

                local cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
                if sq.is_aggregated && !isempty(sq.groupBy)
                    cte_sql *= " " * sq.groupBy
                    sq.groupBy = ""
                end
                if !isempty(sq.where)
                    cte_sql *= " WHERE " * sq.where
                    sq.where = ""
                end
                if !isempty(sq.having)
                    cte_sql *= "  " * sq.having
                    sq.having = ""
                end

                # Create and add the new CTE
                new_cte = CTE(name=string(cte_name), select=cte_sql)
                up_cte_name(sq, string(cte_name))
                
                push!(sq.ctes, new_cte)
                sq.cte_count += 1
                sq.from = string(cte_name)
                
            else
              #  sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                
            end
            cte_name = "cte_" * string(sq.cte_count + 1)
            sq.cte_count += 1

            select_expressions = ["*"]
            most_recent_source = "cte_" * string(sq.cte_count - 1) 
            all_columns = []; gbv_vector = []
            if (!isnothing(sq.ctes) && !isempty(sq.ctes)) && most_recent_source != "cte_0" || 
                (!isnothing($(esc(grouping_var))) && !isempty($(esc(grouping_var)))) || 
                (!isnothing(sq.groupBy) && !isempty(sq.groupBy))
               gbv = if $(esc(grouping_var)) != nothing 
                             $(esc(grouping_var))
                      elseif sq.groupBy != ""
                        replace(sq.groupBy, "GROUP BY " => "")
                      else 
                        ""
                      end
                gbv_vector = strip.(split(gbv, ","))
                    all_columns = [
                        (row[:current_selxn] == 1 ? row[:name] : most_recent_source * "." * row[:name])
                        for row in eachrow(sq.metadata) if row[:current_selxn] != 0 && row[:name] in gbv_vector
                    ]      
                    else
                    all_columns = [
                        (row[:current_selxn] == 1 ? row[:name] : row[:table_name] * "." * row[:name])
                        for row in eachrow(sq.metadata) if row[:current_selxn] != 0 && row[:name] in gbv_vector
                    ]        
                    end  
                    for row in eachrow(sq.metadata)
                        if !(row[:name] in all_columns)
                            row[:current_selxn] = 0
                        end
                    end
               # end    

            select_expressions = [col for col in all_columns]  # Start with all currently selected columns
                
            if $(esc(grouping_var)) != nothing
                group_vars = $(esc(grouping_var))
                group_vars_sql = expr_to_sql(group_vars, sq)
                sq.groupBy = "GROUP BY " * string(group_vars_sql)
            end

            if $(esc(order_var)) != nothing
               TidierDB.@window_order(sq, ($order_var))
            end

            if $(esc(frame_var)) != nothing
                 TidierDB.@window_frame(sq, ($frame_var))
              end

            for expr in $mutations
                # Transform 'across' expressions first
                if isa(expr, Expr) && expr.head == :call && expr.args[1] == :across
                    expr = parse_across(expr, sq.metadata)
                end
                if isa(expr, Expr) && expr.head == :tuple
                    for subexpr in expr.args
                        process_mutate_expression(subexpr, sq, select_expressions, cte_name, from_transmute = true)
                    end
                else
                    process_mutate_expression(expr, sq, select_expressions, cte_name, from_transmute = true)
                end
            end
            sq.post_join = false

            if $(esc(grouping_var)) != nothing
                sq.groupBy = ""
            end
                # Construct CTE SQL, handling aggregated queries differently
            local cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
            if sq.is_aggregated
                cte_sql *= " " * sq.groupBy
                sq.is_aggregated = false
            end
            if !isempty(sq.where)
                cte_sql *= " WHERE " * sq.where
                sq.where = ""
            end

            new_cte = CTE(name=string(cte_name), select=cte_sql)
            up_cte_name(sq, cte_name)
            push!(sq.ctes, new_cte)


            sq.from = string(cte_name)

            sq.select = "*"
            if _warning_[]
                if sq.groupBy != "" || sq.window_order != "" || sq.windowFrame != ""
                    @warn "After applying all mutations, @mutate removed grouping and window clauses."
                end
            end
            sq.groupBy = ""
            sq.windowFrame = ""
            sq.window_order = ""
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_summary
"""
macro summary(sqlquery)
    return quote
        sq = t($(esc(sqlquery)))
        if isa(sq, SQLQuery)
            if (sq.cte_count > 0 || sq.select != "")
                throw("@summary can only be used on tables un")
            else
                sq.from
            end
            sq.select = "SUMMARIZE"
        else
            error("Expected sqlquery to be an instance of SQLQuery") # COV_EXCL_LINE
        end
        sq
    end
end

function extract_paren_aliases(sel::String)
    # drop the leading “SELECT ”
    body = replace(sel, r"(?i)^\s*SELECT\s+" => "")
    paren_sql = String[]
    aliases   = String[]
    for m in eachmatch(r"(\(.*?\)\s+AS\s+(\w+))", body)
        push!(paren_sql, m.captures[1])
        push!(aliases,   m.captures[2])
    end
    return paren_sql, aliases
end