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
        cols_names = cols.name[cols.current_selxn .== 1] |> Vector
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

"""
$docstring_left_join
"""
macro left_join(sqlquery, join_table, expr)
      lhs_col_str, rhs_col_str = parse_join_expression(expr)
    
    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))  # Evaluate join_table

        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                if isa(jq, SQLQuery)
                    jq.cte_count += 1                    # Handle when join_table is an SQLQuery
                    sq.join_count += 1
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        
                        cte_name_jq = joinc* "cte_" * string(jq.cte_count)
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq   
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                      most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
                end
                
                join_sql = " " * most_recent_source * ".*, " *
                           get_join_columns(sq.db, join_table_name, $lhs_col_str) * gbq_join_parse(most_recent_source) *
                           " LEFT JOIN " * join_table_name * " ON " *
                           gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                           gbq_join_parse(most_recent_source) * "." * $rhs_col_str

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                # Update the FROM clause
                sq.from = cte_name
            else
                if isa(jq, SQLQuery)
                    # Handle when join_table is an SQLQuery
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    sq.join_count += 1
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        jq.cte_count += 1
                        cte_name_jq = joinc * "cte_" * string(jq.cte_count) #
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name

                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_clause = " LEFT JOIN " * join_table_name * " ON " *
                              gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                              gbq_join_parse(sq.from) * "." * $rhs_col_str
                sq.from *= join_clause
            end
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end



"""
$docstring_right_join
"""
macro right_join(sqlquery, join_table, expr)
    lhs_col_str, rhs_col_str = parse_join_expression(expr)

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))  # Evaluate join_table

        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                if isa(jq, SQLQuery)
                    jq.cte_count += 1                    # Handle when join_table is an SQLQuery
                    sq.join_count += 1
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        
                        cte_name_jq = joinc* "cte_" * string(jq.cte_count)
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq   
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_sql = " " * most_recent_source * ".*, " *
                           get_join_columns(sq.db, join_table_name, $lhs_col_str) * gbq_join_parse(most_recent_source) *
                           " RIGHT JOIN " * join_table_name * " ON " *
                           gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                           gbq_join_parse(most_recent_source) * "." * $rhs_col_str

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                # Update the FROM clause
                sq.from = cte_name
            else
                if isa(jq, SQLQuery)
                    # Handle when join_table is an SQLQuery
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    sq.join_count += 1
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        jq.cte_count += 1
                        cte_name_jq = joinc * "cte_" * string(jq.cte_count) #
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_clause = " RIGHT JOIN " * join_table_name * " ON " *
                              gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                              gbq_join_parse(sq.from) * "." * $rhs_col_str
                sq.from *= join_clause
            end
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


"""
$docstring_inner_join
"""
macro inner_join(sqlquery, join_table, expr)
    lhs_col_str, rhs_col_str = parse_join_expression(expr)


    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))  # Evaluate join_table

        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                if isa(jq, SQLQuery)
                    jq.cte_count += 1                    # Handle when join_table is an SQLQuery
                    sq.join_count += 1
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        
                        cte_name_jq = joinc* "cte_" * string(jq.cte_count)
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq   
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_sql = " " * most_recent_source * ".*, " *
                           get_join_columns(sq.db, join_table_name, $lhs_col_str) * gbq_join_parse(most_recent_source) *
                           " INNER JOIN " * join_table_name * " ON " *
                           gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                           gbq_join_parse(most_recent_source) * "." * $rhs_col_str

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                # Update the FROM clause
                sq.from = cte_name
            else
                if isa(jq, SQLQuery)
                    # Handle when join_table is an SQLQuery
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    sq.join_count += 1
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        jq.cte_count += 1
                        cte_name_jq = joinc * "cte_" * string(jq.cte_count) #
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_clause = " INNER JOIN " * join_table_name * " ON " *
                              gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                              gbq_join_parse(sq.from) * "." * $rhs_col_str
                sq.from *= join_clause
            end
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


"""
$docstring_full_join
"""
macro full_join(sqlquery, join_table, expr)
    lhs_col_str, rhs_col_str = parse_join_expression(expr)


    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))  # Evaluate join_table

        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                if isa(jq, SQLQuery)
                    jq.cte_count += 1                    # Handle when join_table is an SQLQuery
                    sq.join_count += 1
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        
                        cte_name_jq = joinc* "cte_" * string(jq.cte_count)
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq   
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                   
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_sql = " " * most_recent_source * ".*, " *
                           get_join_columns(sq.db, join_table_name, $lhs_col_str) * gbq_join_parse(most_recent_source) *
                           " FULL JOIN " * join_table_name * " ON " *
                           gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                           gbq_join_parse(most_recent_source) * "." * $rhs_col_str

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                # Update the FROM clause
                sq.from = cte_name
            else
                if isa(jq, SQLQuery)
                    # Handle when join_table is an SQLQuery
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    sq.join_count += 1
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        jq.cte_count += 1
                        cte_name_jq = joinc * "cte_" * string(jq.cte_count) #
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_clause = " FULL JOIN " * join_table_name * " ON " *
                              gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                              gbq_join_parse(sq.from) * "." * $rhs_col_str
                sq.from *= join_clause
            end
            
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end



"""
$docstring_semi_join
"""
macro semi_join(sqlquery, join_table, expr)
    lhs_col_str, rhs_col_str = parse_join_expression(expr)

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))  # Evaluate join_table

        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                if isa(jq, SQLQuery)
                    jq.cte_count += 1                    # Handle when join_table is an SQLQuery
                    sq.join_count += 1
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                       # joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        
                        cte_name_jq = joinc* "cte_" * string(jq.cte_count)
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq   
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_sql = " " * most_recent_source * ".*, " *
                           get_join_columns(sq.db, join_table_name, $lhs_col_str) * gbq_join_parse(most_recent_source) *
                           " SEMI JOIN " * join_table_name * " ON " *
                           gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                           gbq_join_parse(most_recent_source) * "." * $rhs_col_str

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                # Update the FROM clause
                sq.from = cte_name
            else
                if isa(jq, SQLQuery)
                    # Handle when join_table is an SQLQuery
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    sq.join_count += 1
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        jq.cte_count += 1
                        cte_name_jq = joinc * "cte_" * string(jq.cte_count) #
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
               end
                join_clause = " SEMI JOIN " * join_table_name * " ON " *
                              gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                              gbq_join_parse(sq.from) * "." * $rhs_col_str
                sq.from *= join_clause
            end
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


"""
$docstring_anti_join
"""
macro anti_join(sqlquery, join_table, expr)
    lhs_col_str, rhs_col_str = parse_join_expression(expr)

    return quote
        sq = $(esc(sqlquery))
        jq = $(esc(join_table))  # Evaluate join_table

        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                if isa(jq, SQLQuery)
                    jq.cte_count += 1                    # Handle when join_table is an SQLQuery
                    sq.join_count += 1
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        
                        cte_name_jq = joinc* "cte_" * string(jq.cte_count)
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq   
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
                end
                join_sql = " " * most_recent_source * ".*, " *
                           get_join_columns(sq.db, join_table_name, $lhs_col_str) * gbq_join_parse(most_recent_source) *
                           " ANTI JOIN " * join_table_name * " ON " *
                           gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                           gbq_join_parse(most_recent_source) * "." * $rhs_col_str

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                # Update the FROM clause
                sq.from = cte_name
            else
                if isa(jq, SQLQuery)
                    # Handle when join_table is an SQLQuery
                    needs_new_cte_jq = !isempty(jq.select) || !isempty(jq.where) || jq.is_aggregated || !isempty(jq.ctes)
                    sq.join_count += 1
                    if needs_new_cte_jq
                        joinc = "j" * string(sq.join_count)
                        for cte in jq.ctes
                            cte.name = joinc * cte.name
                        end
                        jq.cte_count += 1
                        cte_name_jq = joinc * "cte_" * string(jq.cte_count) #
                        most_recent_source_jq = !isempty(jq.ctes) ? joinc * "cte_" * string(jq.cte_count - 1) : jq.from
                        select_sql_jq = finalize_query_jq(jq, most_recent_source_jq)
                        new_cte_jq = CTE(name=cte_name_jq, select=select_sql_jq)
                        push!(jq.ctes, new_cte_jq)
                        jq.from = cte_name_jq
                    end
                    # Combine CTEs and metadata
                    sq.ctes = vcat(sq.ctes, jq.ctes)
                    sq.metadata = vcat(sq.metadata, jq.metadata)
                    join_table_name = jq.from
                else
                    # When join_table is a table name
                    join_table_name = string(jq)
                    if current_sql_mode[] != :athena
                        new_metadata = get_table_metadata(sq.db, join_table_name)
                    else
                        new_metadata = get_table_metadata_athena(sq.db, join_table_name, sq.athena_params)
                    end
                    sq.metadata = vcat(sq.metadata, new_metadata)
                end
                if sq.groupBy != ""
                    most_recent_source, cte_name = create_and_add_cte(sq, cte_name)
                end
                join_clause = " ANTI JOIN " * join_table_name * " ON " *
                              gbq_join_parse(join_table_name) * "." * $lhs_col_str * " = " *
                              gbq_join_parse(sq.from) * "." * $rhs_col_str
                sq.from *= join_clause
            end
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
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
