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
            # Check if there are modifications necessitating a new CTE
            # This can be determined by checking if sq.select, sq.where, etc., have been modified
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)

            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                
                # Use the most recent CTE or base table as the FROM source
                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from
                
                # Construct the SQL for the join
                join_sql = " " * most_recent_source * ".*, " * string($(esc(join_table))) * ".* FROM " * most_recent_source * 
                           " LEFT JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", most_recent_source, ".", $rhs_col_str)
                
                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                
                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " LEFT JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", sq.from, ".", $rhs_col_str)
                sq.from *= join_clause

            end
            
            # Update metadata to include columns from the joined table
            new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
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
                
                join_sql = " " * most_recent_source * ".*, " * string($(esc(join_table))) * ".* FROM " * most_recent_source * 
                           " RIGHT JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", most_recent_source, ".", $rhs_col_str)
                
                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                
                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " RIGHT JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", sq.from, ".", $rhs_col_str)
                sq.from *= join_clause
            end
            
            new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
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
                
                join_sql = " " * most_recent_source * ".*, " * string($(esc(join_table))) * ".* FROM " * most_recent_source * 
                           " INNER JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", most_recent_source, ".", $rhs_col_str)
                
                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                
                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " INNER JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", sq.from, ".", $rhs_col_str)
                sq.from *= join_clause
            end
            
            new_metadata = get_table_metadata(sq.db, string($(esc(join_table))))
            sq.metadata = vcat(sq.metadata, new_metadata)
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end