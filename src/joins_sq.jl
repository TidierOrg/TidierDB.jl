function gbq_join_parse(input)
    input = string(input)
    parts = split(input, ".")
    if current_sql_mode[] == gbq() && length(parts) >=2
        return join(parts[2:end], ".")
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


"""
$docstring_left_join
"""
macro left_join(sqlquery, join_table, lhs_column, rhs_column)
    # Convert column references to string
    lhs_col_str = string(lhs_column)
    rhs_col_str = string(rhs_column)
    join_table = QuoteNode(join_table)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)
            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)

                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                join_sql = " " * most_recent_source * ".*, " * get_join_columns(sq.db, string($(esc(join_table))), $lhs_col_str) * gbq_join_parse(most_recent_source) *
                            " LEFT JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(most_recent_source), ".", $rhs_col_str)

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)

                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " LEFT JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(sq.from), ".", $rhs_col_str)
                sq.from *= join_clause
            end

            if current_sql_mode[] != :athena
                new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
            else
                new_metadata = get_table_metadata_athena(sq.db, string($(esc(join_table))), sq.athena_params)
            end
            sq.metadata = vcat(sq.metadata, new_metadata)
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_right_join
"""
macro right_join(sqlquery, join_table, lhs_column, rhs_column)
    # Convert column references to string
    lhs_col_str = string(lhs_column)
    rhs_col_str = string(rhs_column)
    join_table = QuoteNode(join_table)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)

            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)

                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                join_sql = " " * most_recent_source * ".*, " * get_join_columns(sq.db, string($(esc(join_table))), $lhs_col_str) * gbq_join_parse(most_recent_source) *
                            " RIGHT JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(most_recent_source), ".", $rhs_col_str)

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)

                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " RIGHT JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(sq.from), ".", $rhs_col_str)
                sq.from *= join_clause
            end

            if current_sql_mode[] != :athena
                new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
            else
                new_metadata = get_table_metadata_athena(sq.db, string($(esc(join_table))), sq.athena_params)
            end
            sq.metadata = vcat(sq.metadata, new_metadata)
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


"""
$docstring_inner_join
"""
macro inner_join(sqlquery, join_table, lhs_column, rhs_column)
    # Convert column references to string
    lhs_col_str = string(lhs_column)
    rhs_col_str = string(rhs_column)
    join_table = QuoteNode(join_table)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)

            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)

                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                join_sql = " " * most_recent_source * ".*, " * get_join_columns(sq.db, string($(esc(join_table))), $lhs_col_str) * gbq_join_parse(most_recent_source) *
                            " INNER JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(most_recent_source), ".", $rhs_col_str)

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)

                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " INNER JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(sq.from), ".", $rhs_col_str)
                sq.from *= join_clause
            end

            if current_sql_mode[] != :athena
                new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
            else
                new_metadata = get_table_metadata_athena(sq.db, string($(esc(join_table))), sq.athena_params)
            end
            sq.metadata = vcat(sq.metadata, new_metadata)
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


"""
$docstring_full_join
"""
macro full_join(sqlquery, join_table, lhs_column, rhs_column)
    # Convert column references to string
    lhs_col_str = string(lhs_column)
    rhs_col_str = string(rhs_column)
    join_table = QuoteNode(join_table)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)

            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)

                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                join_sql = " " * most_recent_source * ".*, " * get_join_columns(sq.db, string($(esc(join_table))), $lhs_col_str) * gbq_join_parse(most_recent_source) *
                            " FULL JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(most_recent_source), ".", $rhs_col_str)

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)

                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " FULL JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(sq.from), ".", $rhs_col_str)
                sq.from *= join_clause
            end

            if current_sql_mode[] != :athena
                new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
            else
                new_metadata = get_table_metadata_athena(sq.db, string($(esc(join_table))), sq.athena_params)
            end
            sq.metadata = vcat(sq.metadata, new_metadata)
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


"""
$docstring_semi_join
"""
macro semi_join(sqlquery, join_table, lhs_column, rhs_column)
    # Convert column references to string
    lhs_col_str = string(lhs_column)
    rhs_col_str = string(rhs_column)
    join_table = QuoteNode(join_table)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)

            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)

                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                join_sql = " " * most_recent_source * ".*, " * get_join_columns(sq.db, string($(esc(join_table))), $lhs_col_str) * gbq_join_parse(most_recent_source) *
                            " SEMI JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(most_recent_source), ".", $rhs_col_str)

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)

                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " SEMI JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(sq.from), ".", $rhs_col_str)
                sq.from *= join_clause
            end

            if current_sql_mode[] != :athena
                new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
            else
                new_metadata = get_table_metadata_athena(sq.db, string($(esc(join_table))), sq.athena_params)
            end
            sq.metadata = vcat(sq.metadata, new_metadata)
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end


"""
$docstring_anti_join
"""
macro anti_join(sqlquery, join_table, lhs_column, rhs_column)
    # Convert column references to string
    lhs_col_str = string(lhs_column)
    rhs_col_str = string(rhs_column)
    join_table = QuoteNode(join_table)

    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)

            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)

                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                join_sql = " " * most_recent_source * ".*, " * get_join_columns(sq.db, string($(esc(join_table))), $lhs_col_str) * gbq_join_parse(most_recent_source) *
                            " ANTI JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(most_recent_source), ".", $rhs_col_str)

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)

                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " ANTI JOIN " * string($(esc(join_table))) * " ON " * string(gbq_join_parse($(esc(join_table))), ".", $lhs_col_str, " = ", gbq_join_parse(sq.from), ".", $rhs_col_str)
                sq.from *= join_clause
            end

            if current_sql_mode[] != :athena
                new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
            else
                new_metadata = get_table_metadata_athena(sq.db, string($(esc(join_table))), sq.athena_params)
            end
            sq.metadata = vcat(sq.metadata, new_metadata)
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end
