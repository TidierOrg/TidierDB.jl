# A unified function that performs the set operation, preserving existing logic

function perform_set_operation(sq::SQLQuery, uq_or_table, op::String; all::Bool=false)

    if !isa(sq, SQLQuery)
        error("Expected sqlquery to be an instance of SQLQuery")
    end

    # 1) Possibly create a new CTE for the left query (sq)
    needs_new_cte_sq = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
    if needs_new_cte_sq
        sq.cte_count += 1
        cte_name_sq = "cte_" * string(sq.cte_count)
        most_recent_source_sq = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from
        select_sql_sq = "SELECT * FROM " * most_recent_source_sq
        new_cte_sq = CTE(name=cte_name_sq, select=select_sql_sq)
        up_cte_name(sq, cte_name_sq)
        
        push!(sq.ctes, new_cte_sq)
        sq.from = cte_name_sq
    end

    local op_clause
    if all
        op_clause = op * " ALL"
    else
        op_clause = op
    end

    local union_sql
    if isa(uq_or_table, SQLQuery)
        uq = uq_or_table
        # Possibly create a new CTE for the right query (uq)
        needs_new_cte_uq = !isempty(uq.select) || !isempty(uq.where) || uq.is_aggregated || !isempty(uq.ctes)
        if needs_new_cte_uq
            sq.join_count += 1
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

        # Combine
        union_sql = "SELECT * FROM " * sq.from * " " * op_clause * " SELECT * FROM " * uq.from

        sq.ctes = vcat(sq.ctes, uq.ctes)
        # sq.metadata = vcat(sq.metadata, uq.metadata)

    else
        # Treat uq_or_table as a table name
        tbl_name = string(uq_or_table)
        union_sql = "SELECT * FROM " * sq.from * " " * op_clause * " SELECT * FROM " * tbl_name

        # Update metadata (commented out as in original)
        if current_sql_mode[] != :athena
            new_metadata = get_table_metadata(sq.db, tbl_name)
        else
            new_metadata = get_table_metadata_athena(sq.db, tbl_name, sq.athena_params)
        end
        # sq.metadata = vcat(sq.metadata, new_metadata)
    end

    # 4) Create a new CTE for the combined result
    sq.cte_count += 1
    union_cte_name = "cte_" * string(sq.cte_count)
    union_cte = CTE(name=union_cte_name, select=union_sql)
    up_cte_name(sq, union_cte_name)
    push!(sq.ctes, union_cte)
    sq.from = union_cte_name

    return sq
end


"""
$docstring_union
"""
macro union(sqlquery, union_query, args...)
    # parse the `all` argument exactly as in the original logic
    all_flag = false
    for arg in args
        if isa(arg, Expr) && arg.head == :(=)
            if arg.args[1] == :all && arg.args[2] == true
                all_flag = true
            end
        end
    end
    return quote
        perform_set_operation(
            $(esc(sqlquery)),
            $(esc(union_query)),
            "UNION";
            all = $(all_flag)
        )
    end
end

"""
$docstring_union_all
"""
macro union_all(sqlquery, union_query)
    return quote
        perform_set_operation(
            $(esc(sqlquery)),
            $(esc(union_query)),
            "UNION";  # We'll let the function append " ALL"
            all = true
        )
    end
end

"""
$docstring_intersect
"""
macro intersect(sqlquery, union_query, args...)
    # parse the `all` argument exactly as in the original logic
    all_flag = false
    for arg in args
        if isa(arg, Expr) && arg.head == :(=)
            if arg.args[1] == :all && arg.args[2] == true
                all_flag = true
            end
        end
    end

    return quote
        perform_set_operation(
            $(esc(sqlquery)),
            $(esc(union_query)),
            "INTERSECT";
            all = $(all_flag)
        )
    end
end

"""
$docstring_setdiff
"""
macro setdiff(sqlquery, union_query, args...)
    # parse the `all` argument exactly as in the original logic
    all_flag = false
    for arg in args
        if isa(arg, Expr) && arg.head == :(=)
            if arg.args[1] == :all && arg.args[2] == true
                all_flag = true
            end
        end
    end

    return quote
        perform_set_operation(
            $(esc(sqlquery)),
            $(esc(union_query)),
            "EXCEPT";
            all = $(all_flag)
        )
    end
end
