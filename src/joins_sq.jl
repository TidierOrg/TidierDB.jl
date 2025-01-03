## Join macros are long, but all basically identical logic 
## just change the join keyword. 
function gbq_join_parse(input)
    input = string(input)
    parts = split(input, ".")
    if current_sql_mode[] == gbq() && length(parts) >=2
        return parts[end]
    elseif occursin(".", input)
            if  occursin(r"[:/]", input)
                return split(basename(input), '.')[1]
            else
                return split(input, '.')[end]
            end
    else 
        return input
    end
end

function get_join_columns(db, join_table, lhs_col_str)
    if current_sql_mode[] == mssql()
        cols = get_table_metadata(db, string(join_table))
        matching_indices = findall(cols.name .== lhs_col_str)
        cols.current_selxn[matching_indices] .= 0
        cols_names = cols.name[cols.current_selxn .>= 1] |> Vector
        return join([string(join_table, ".", col) for col in cols_names], ", ") * " FROM "
    else current_sql_mode[] == gbq()
        return string(gbq_join_parse(join_table)) * ".* FROM "
    end
end

function finalize_query_jq(sqlquery::SQLQuery, from_clause)

    select_already_present = occursin(r"^SELECT\s+", uppercase(sqlquery.select))
    select_part = if sqlquery.distinct && !select_already_present
        "SELECT DISTINCT " * (isempty(sqlquery.select) ? "*" : sqlquery.select)
    elseif !select_already_present
        "SELECT " * (isempty(sqlquery.select) ? "*" : sqlquery.select)
    else
        sqlquery.select
    end
    query_parts = [select_part, "FROM ", from_clause]
    # Initialize query_parts with the CTE part
    # Append other clauses if present
    if !isempty(sqlquery.where) push!(query_parts, " " * sqlquery.where) end
    if !isempty(sqlquery.groupBy) push!(query_parts, "" * sqlquery.groupBy) end
    if !isempty(sqlquery.having) push!(query_parts, " " * sqlquery.having) end
    if !isempty(sqlquery.orderBy) push!(query_parts, " " * sqlquery.orderBy) end
    if !isempty(sqlquery.limit) push!(query_parts, " LIMIT " * sqlquery.limit) end
    complete_query = join(filter(!isempty, query_parts), " ")
    return complete_query
end
function create_and_add_cte(sq, cte_name)
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
    if !isempty(sq.select)
        cte_sql *= "  "
        sq.select = " * "
    end
    # Create and add the new CTE
    new_cte = CTE(name=string(cte_name), select=cte_sql)
    push!(sq.ctes, new_cte)
    sq.cte_count += 1
    cte_name = "cte_" * string(sq.cte_count)
    most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from
    return most_recent_source, cte_name
end

function process_and_generate_columns(sq, vq, lhs, rhs, most_recent_source, join_table_name)
    # Initialize strings for the final result
    coalesce_exprs = String[]
    column_exprs = String[]
    
    # Process `sq`
    function filter_dataframe_by_columns(df, column_set, remove=true)
        # Identify rows to keep or remove based on column_set
        if remove
            filtered_df = filter(row -> !(row.name in column_set), df)
        else
            filtered_df = filter(row -> row.name in column_set, df)
        end
        return filtered_df
    end
    
    cols_sq = filter_dataframe_by_columns(sq.metadata, vq.metadata.name)
    matching_indices_sq = findall(cols_sq.name .== lhs .|| cols_sq.name .== rhs)
    cols_sq.current_selxn[matching_indices_sq] .= 0
    cols_names_sq = cols_sq.name[cols_sq.current_selxn .>= 1] |> Vector

    # Add `COALESCE(...)` for lhs
    for (lhs_col, rhs_col) in zip(lhs, rhs)
        push!(coalesce_exprs, "COALESCE($most_recent_source.$lhs_col, $join_table_name.$rhs_col) AS $lhs_col")
    end
    

    # Add remaining columns from `sq`
    for col in cols_names_sq
        push!(column_exprs, "$most_recent_source.$col")
    end

    # Process `vq`
    cols_vq = vq.metadata
    matching_indices_vq = findall(cols_vq.name .== lhs .|| cols_vq.name .== rhs)
    cols_vq.current_selxn[matching_indices_vq] .= 0
    cols_names_vq = cols_vq.name[cols_vq.current_selxn .>= 1 .&& cols_vq.table_name .!= join_table_name] |> Vector

    # Add remaining columns from `vq`
    for col in cols_names_vq
        push!(column_exprs, "$join_table_name.$col")
    end

    # Combine all expressions
    final_columns = join(vcat(coalesce_exprs, column_exprs), ", ")  
    
    return final_columns   #*gbq_join_parse(most_recent_source)


end

function sql_join_on(sq, join_table_name, lhs_cols::Vector{String}, rhs_cols::Vector{String}, operators::Vector{String}; closest_expr=String[])
    table_from = isa(sq, SQLQuery) ? sq.from : sq
    conditions = String[]
    for (lhs_col_str, rhs_col_str, symb_str) in zip(lhs_cols, rhs_cols, operators)
        condition = gbq_join_parse(table_from) * "." * lhs_col_str * " " * symb_str * " " *
                    gbq_join_parse(join_table_name) * "." * rhs_col_str
        push!(conditions, condition)
    end
    on = join(conditions, " AND ")
    if on == "" && closest_expr != []
        on *= join(closest_expr, " AND ")
    elseif closest_expr != []
       # on *=  " AND " * join(closest_expr, " AND ")
    end
    return on
end

function do_join(
    join_type::String,
    sq::SQLQuery,
    jq::Union{SQLQuery,String},
    lhs_col_str::Vector{String},
    rhs_col_str::Vector{String},
    operators::Vector{String},
    closest_expr::Vector{String},
    as_of::String
)

    needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)

    if needs_new_cte
        sq.cte_count += 1
        cte_name = "cte_" * string(sq.cte_count)
        most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

        # === handle `jq` (join_table) if it is another SQLQuery ===
        if isa(jq, SQLQuery)
            jq.cte_count += 1
            sq.join_count += 1
            needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)

            if needs_new_cte_jq
                joinc = "j" * string(sq.join_count)
                for cte in jq.ctes
                    cte.name = joinc * cte.name
                end
                cte_name_jq = joinc * "cte_" * string(jq.cte_count)
                most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                push!(jq.ctes, new_cte_jq)
                jq.from = cte_name_jq
            end

            sq.ctes = vcat(sq.ctes, jq.ctes)
            sq.metadata = vcat(sq.metadata, jq.metadata)
            join_table_name = jq.from

        else
            # === handle `jq` if it is just a string/table-name ===
            join_table_name = string(jq)
            if current_sql_mode[] != :athena
                new_metadata = get_table_metadata(sq.db, join_table_name)
            else
                new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
            end
            sq.metadata = vcat(sq.metadata, new_metadata)
        end

        # If grouping is present, finalize the previous partial query as a CTE
        if sq.groupBy != ""
            most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
        end


        join_sql = " " * most_recent_source * ".*, " *
                           get_join_columns(sq.db, join_table_name, lhs_col_str) * gbq_join_parse(most_recent_source) *
                           as_of * " " * join_type * " JOIN " * join_table_name * " ON " *
                        sql_join_on(most_recent_source, join_table_name, lhs_col_str, rhs_col_str, operators)

        # Create and add the new CTE
        new_cte = CTE(name=cte_name, select=join_sql)
        push!(sq.ctes, new_cte)
        sq.from = cte_name

    else
        # === no new CTE needed, proceed with direct join string building ===
        if isa(jq, SQLQuery)
            needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
            sq.join_count += 1
            if needs_new_cte_jq
                joinc = "j" * string(sq.join_count)
                for cte in jq.ctes
                    cte.name = joinc * cte.name
                end
                jq.cte_count += 1
                cte_name_jq = joinc * "cte_" * string(jq.cte_count)
                most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                push!(jq.ctes, new_cte_jq)
                jq.from = cte_name_jq
            end
            sq.ctes = vcat(sq.ctes, jq.ctes)
            sq.metadata = vcat(sq.metadata, jq.metadata)
            join_table_name = jq.from
        else
            join_table_name = string(jq)
            if current_sql_mode[] != :athena
                new_metadata = get_table_metadata(sq.db, join_table_name)
            else
                new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
            end
            sq.metadata = vcat(sq.metadata, new_metadata)
        end

        if sq.groupBy != ""
            most_recent_source, cte_name = create_and_add_cte(sq, "cte_" * string(sq.cte_count))
        end

       
        join_clause = as_of * " " * join_type * " JOIN " * join_table_name * " ON " *
                    sql_join_on(sq.from, join_table_name, lhs_col_str, rhs_col_str, operators)
               
        sq.from *= join_clause
    end

    return sq
end


"""
$docstring_left_join
"""
macro left_join(sqlquery, join_table, expr... )
    lhs_col_str = String[]
    rhs_col_str = String[]
    operators   = String[]
    closest_expr = String[]
    as_of = ""

    parsed = parse_join_expression.(expr)

    lhs_col_str  = vcat([p[1] for p in parsed]...)
    rhs_col_str  = vcat([p[2] for p in parsed]...)
    operators    = vcat([p[3] for p in parsed]...)
    closest_expr = vcat([p[4] for p in parsed]...)
    as_of        = join([p[5] for p in parsed], "") 

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))
        do_join(
            "LEFT",
            sq,
            jq,
            $lhs_col_str,
            $rhs_col_str,
            $operators,
            $closest_expr,
            $as_of
        )
    end
end



"""
$docstring_right_join
"""
macro right_join(sqlquery, join_table, expr... )
    lhs_col_str = String[]
    rhs_col_str = String[]
    operators   = String[]
    closest_expr = String[]
    as_of = ""

    parsed = parse_join_expression.(expr)

    lhs_col_str  = vcat([p[1] for p in parsed]...)
    rhs_col_str  = vcat([p[2] for p in parsed]...)
    operators    = vcat([p[3] for p in parsed]...)
    closest_expr = vcat([p[4] for p in parsed]...)
    as_of        = join([p[5] for p in parsed], "") 

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))
        do_join(
            "RIGHT",
            sq,
            jq,
            $lhs_col_str,
            $rhs_col_str,
            $operators,
            $closest_expr,
            $as_of
        )
    end
end


"""
$docstring_inner_join
"""
macro inner_join(sqlquery, join_table, expr... )
    lhs_col_str = String[]
    rhs_col_str = String[]
    operators   = String[]
    closest_expr = String[]
    as_of = ""

    parsed = parse_join_expression.(expr)

    lhs_col_str  = vcat([p[1] for p in parsed]...)
    rhs_col_str  = vcat([p[2] for p in parsed]...)
    operators    = vcat([p[3] for p in parsed]...)
    closest_expr = vcat([p[4] for p in parsed]...)
    as_of        = join([p[5] for p in parsed], "") 

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))
        do_join(
            "INNER",
            sq,
            jq,
            $lhs_col_str,
            $rhs_col_str,
            $operators,
            $closest_expr,
            $as_of
        )
    end
end




"""
$docstring_full_join
"""
macro full_join(sqlquery, join_table, expr... )
    lhs_col_str = String[]
    rhs_col_str = String[]
    operators   = String[]
    closest_expr = String[]
    as_of = ""

    parsed = parse_join_expression.(expr)

    lhs_col_str  = vcat([p[1] for p in parsed]...)
    rhs_col_str  = vcat([p[2] for p in parsed]...)
    operators    = vcat([p[3] for p in parsed]...)
    closest_expr = vcat([p[4] for p in parsed]...)
    as_of        = join([p[5] for p in parsed], "") 

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))
        do_join(
            "FULL",
            sq,
            jq,
            $lhs_col_str,
            $rhs_col_str,
            $operators,
            $closest_expr,
            $as_of
        )
    end
end


"""
$docstring_semi_join
"""
macro semi_join(sqlquery, join_table, expr... )
    lhs_col_str = String[]
    rhs_col_str = String[]
    operators   = String[]
    closest_expr = String[]
    as_of = ""

    parsed = parse_join_expression.(expr)

    lhs_col_str  = vcat([p[1] for p in parsed]...)
    rhs_col_str  = vcat([p[2] for p in parsed]...)
    operators    = vcat([p[3] for p in parsed]...)
    closest_expr = vcat([p[4] for p in parsed]...)
    as_of        = join([p[5] for p in parsed], "") 

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))
        do_join(
            "SEMI",
            sq,
            jq,
            $lhs_col_str,
            $rhs_col_str,
            $operators,
            $closest_expr,
            $as_of
        )
    end
end



"""
$docstring_anti_join
"""
macro anti_join(sqlquery, join_table, expr... )
    lhs_col_str = String[]
    rhs_col_str = String[]
    operators   = String[]
    closest_expr = String[]
    as_of = ""

    parsed = parse_join_expression.(expr)

    lhs_col_str  = vcat([p[1] for p in parsed]...)
    rhs_col_str  = vcat([p[2] for p in parsed]...)
    operators    = vcat([p[3] for p in parsed]...)
    closest_expr = vcat([p[4] for p in parsed]...)
    as_of        = join([p[5] for p in parsed], "") 

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))
        do_join(
            "ANTI",
            sq,
            jq,
            $lhs_col_str,
            $rhs_col_str,
            $operators,
            $closest_expr,
            $as_of
        )
    end
end



"""
$docstring_union
"""
macro union(sqlquery, union_query)
    return quote
        sq = $(esc(sqlquery))
        uq = $(esc(union_query))

        if isa(sq, SQLQuery)
            # Determine if sq needs a new CTE
            needs_new_cte_sq = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte_sq
                sq.cte_count += 1
                cte_name_sq = "cte_" * string(sq.cte_count)
                most_recent_source_sq = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from
                select_sql_sq = "SELECT * FROM " * most_recent_source_sq
                new_cte_sq = CTE(name=cte_name_sq, select=select_sql_sq)
                push!(sq.ctes, new_cte_sq)
                sq.from = cte_name_sq
            end

            # Prepare the union query
            if isa(uq, SQLQuery)
                # Determine if uq needs a new CTE
                needs_new_cte_uq = !isempty(uq.select) || !isempty(uq.where) || uq.is_aggregated || !isempty(uq.ctes)
                if needs_new_cte_uq
                    sq.join_count +=1
                    joinc = "j" * string(sq.join_count)
                    for cte in uq.ctes
                        cte.name = joinc * cte.name
                    end
                    uq.cte_count += 1
                    cte_name_uq = joinc * "cte_" * string(uq.cte_count)
                    most_recent_source_uq = !isempty(uq.ctes) ? joinc * "cte_" * string(uq.cte_count - 1) : uq.from
                    select_sql_uq = finalize_query_jq(uq, most_recent_source_uq)
                    new_cte_uq = CTE(name=cte_name_uq, select=select_sql_uq)
                    push!(uq.ctes, new_cte_uq)
                    uq.from = cte_name_uq
                end

                # Combine the queries using UNION
                union_sql = "SELECT * FROM " * sq.from * " UNION SELECT * FROM " * uq.from

                # Merge CTEs and metadata
                sq.ctes = vcat(sq.ctes, uq.ctes)
              #  sq.metadata = vcat(sq.metadata, uq.metadata)
            else
                # Treat uq as a table name
                union_sql = "SELECT * FROM " * sq.from * " UNION SELECT * FROM " * string(uq)
                # Update metadata
                if current_sql_mode[] != :athena
                    new_metadata = get_table_metadata(sq.db, string(uq))
                else
                    new_metadata = get_table_metadata_athena(sq.db, string(uq), sq.athena_params)
                end
              #  sq.metadata = vcat(sq.metadata, new_metadata)
            end

            # Create a new CTE for the union
            sq.cte_count += 1
            union_cte_name = "cte_" * string(sq.cte_count)
            union_cte = CTE(name=union_cte_name, select=union_sql)
            push!(sq.ctes, union_cte)
            sq.from = union_cte_name
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


"""
$docstring_union_all
"""
macro union_all(sqlquery, union_query)
    return quote
        sq = $(esc(sqlquery))
        uq = $(esc(union_query))

        if isa(sq, SQLQuery)
            # Determine if sq needs a new CTE
            needs_new_cte_sq = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte_sq
                sq.cte_count += 1
                cte_name_sq = "cte_" * string(sq.cte_count)
                most_recent_source_sq = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from
                select_sql_sq = "SELECT * FROM " * most_recent_source_sq
                new_cte_sq = CTE(name=cte_name_sq, select=select_sql_sq)
                push!(sq.ctes, new_cte_sq)
                sq.from = cte_name_sq
            end

            # Prepare the union query
            if isa(uq, SQLQuery)
                # Determine if uq needs a new CTE
                needs_new_cte_uq = !isempty(uq.select) || !isempty(uq.where) || uq.is_aggregated || !isempty(uq.ctes)
                if needs_new_cte_uq
                    sq.join_count +=1
                    joinc = "j" * string(sq.join_count)
                    for cte in uq.ctes
                        cte.name = joinc * cte.name
                    end
                    uq.cte_count += 1
                    cte_name_uq = joinc * "cte_" * string(uq.cte_count)
                    most_recent_source_uq = !isempty(uq.ctes) ? joinc * "cte_" * string(uq.cte_count - 1) : uq.from
                    select_sql_uq = finalize_query_jq(uq, most_recent_source_uq)
                    new_cte_uq = CTE(name=cte_name_uq, select=select_sql_uq)
                    push!(uq.ctes, new_cte_uq)
                    uq.from = cte_name_uq
                end

                # Combine the queries using UNION
                union_sql = "SELECT * FROM " * sq.from * " UNION ALL SELECT * FROM " * uq.from

                # Merge CTEs and metadata
                sq.ctes = vcat(sq.ctes, uq.ctes)
              #  sq.metadata = vcat(sq.metadata, uq.metadata)
            else
                # Treat uq as a table name
                union_sql = "SELECT * FROM " * sq.from * " UNION ALL SELECT * FROM " * string(uq)
                # Update metadata
                if current_sql_mode[] != :athena
                    new_metadata = get_table_metadata(sq.db, string(uq))
                else
                    new_metadata = get_table_metadata_athena(sq.db, string(uq), sq.athena_params)
                end
              #  sq.metadata = vcat(sq.metadata, new_metadata)
            end

            # Create a new CTE for the union
            sq.cte_count += 1
            union_cte_name = "cte_" * string(sq.cte_count)
            union_cte = CTE(name=union_cte_name, select=union_sql)
            push!(sq.ctes, union_cte)
            sq.from = union_cte_name
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end