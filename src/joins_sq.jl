"""
$docstring_left_join
"""
macro left_join(sqlquery, join_table, args...)
    # Initialize options
    as_of = false
    and_clause = nothing
    using_columns = []
    lhs_column = nothing
    rhs_column = nothing

    join_table = QuoteNode(join_table)
    positional_args = []
    # Parse dynamic arguments
    for arg in args
        if isa(arg, Expr) && arg.head == :(=)
            key = Symbol(arg.args[1])
            value = arg.args[2]
            if key == :as_of
                as_of = value
            elseif key == :and_clause
                and_clause = value
            elseif key == :using_columns
                # Ensure using_columns are processed as a list of symbols
                if isa(value, Expr) && value.head == :vect
                    using_columns = [Symbol(v) for v in value.args]  # Convert each element to a symbol
                elseif isa(value, Array)
                    using_columns = [isa(v, Symbol) ? v : Symbol(v) for v in value]
                else
                    using_columns = [Symbol(value)]
                end
            else
                # Collect unrecognized named arguments that might be positional
                push!(positional_args, value)
            end
        else
            # Collect all unnamed positional arguments
            push!(positional_args, arg)
        end
    end

    # Assign lhs_column and rhs_column from positional_args if no using_columns and exactly two are present
    if  length(positional_args) == 2
        lhs_column, rhs_column = QuoteNode(positional_args[1]), QuoteNode(positional_args[2])

    end

    # Decide what to do based on provided arguments
    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            # Determine if modifications necessitate a new CTE
            needs_new_cte = !isempty(sq.select) || !isempty(sq.where) || sq.is_aggregated || !isempty(sq.ctes)

            if needs_new_cte
                sq.cte_count += 1
                cte_name = "cte_" * string(sq.cte_count)
                most_recent_source = !isempty(sq.ctes) ? "cte_" * string(sq.cte_count - 1) : sq.from

                join_sql = "SELECT " * most_recent_source * ".*, " * string($(esc(join_table))) * ".* FROM " * most_recent_source
                join_sql *= $as_of ? " ASOF LEFT JOIN " : " LEFT JOIN "
                join_sql *= string($(esc(join_table)))

                if !isempty($using_columns)
                    join_sql *= " USING (" * join(map(string, $using_columns), ", ") * ")"
                elseif $lhs_column !== nothing && $rhs_column !== nothing
                    join_sql *= " ON " * string($(esc(join_table)), ".", $lhs_column, " = ", most_recent_source, ".", $rhs_column)
                else
                    error("Proper join specifications (USING or ON) must be provided.")
                end

                if $and_clause !== nothing
                    join_sql *= " AND " * $and_clause
                end

                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                sq.from = cte_name
            else
                join_clause = $as_of ? " ASOF LEFT JOIN " : " LEFT JOIN "
                join_clause *= string($(esc(join_table)))

                if !isempty($using_columns)
                    join_clause *= " USING (" * join(map(string, $using_columns), ", ") * ")"
                else#if $lhs_column !== nothing && $rhs_column !== nothing
                    join_clause *= " ON " * string($(esc(join_table)), ".", $lhs_column, " = ", sq.from, ".", $rhs_column)
                #else
                #    error("Proper join specifications (USING or ON) must be provided.")
                end

                if $and_clause !== nothing
                    join_clause *= " AND " * $and_clause
                end

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
                
                join_sql = " " * most_recent_source * ".*, " * string($(esc(join_table))) * ".* FROM " * most_recent_source * 
                           " FULL JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", most_recent_source, ".", $rhs_col_str)
                
                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                
                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " FULL JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", sq.from, ".", $rhs_col_str)
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
                
                join_sql = " " * most_recent_source * ".*, " * string($(esc(join_table))) * ".* FROM " * most_recent_source * 
                           " SEMI JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", most_recent_source, ".", $rhs_col_str)
                
                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                
                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " SEMI JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", sq.from, ".", $rhs_col_str)
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
                
                join_sql = " " * most_recent_source * ".*, " * string($(esc(join_table))) * ".* FROM " * most_recent_source * 
                           " ANTI JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", most_recent_source, ".", $rhs_col_str)
                
                # Create and add the new CTE
                new_cte = CTE(name=cte_name, select=join_sql)
                push!(sq.ctes, new_cte)
                
                # Update the FROM clause
                sq.from = cte_name
            else
                join_clause = " ANTI JOIN " * string($(esc(join_table))) * " ON " * string($(esc(join_table)), ".", $lhs_col_str, " = ", sq.from, ".", $rhs_col_str)
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