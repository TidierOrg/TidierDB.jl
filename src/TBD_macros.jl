"""
$docstring_select
"""
macro select(sqlquery, exprs...)
    exprs = parse_blocks(exprs...)

    return quote
        exprs_str = map(expr -> isa(expr, Symbol) ? string(expr) : expr, $exprs)
        let columns = parse_tidy_db(exprs_str, $(esc(sqlquery)).metadata)
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

"""
$docstring_group_by
"""
macro group_by(sqlquery, columns...)
    columns = parse_blocks(columns...)

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

"""
$docstring_count
"""
macro count(sqlquery, group_by_columns...)
    # Convert the group_by_columns to a string representation

    group_by_cols_str = [string(col) for col in group_by_columns]
    group_clause = join(group_by_cols_str, ", ")

    return quote

        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            # Interpolate `group_clause` directly into the quoted code to avoid scope issues
            if !isempty($group_clause)
                for col in $group_by_cols_str
                    $(esc(sqlquery)).metadata.current_selxn .= 0
                    matching_indices = findall($(esc(sqlquery)).metadata.name .== col)
                    $(esc(sqlquery)).metadata.current_selxn[matching_indices] .= 1
                 end
                sq.select = "SELECT " * $group_clause * ", COUNT(*) AS count"
                sq.groupBy = "GROUP BY " * $group_clause
                push!(sq.metadata, Dict("name" => "count", "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => sq.from))

            else
                # If no grouping columns are specified, just count all records
                $(esc(sqlquery)).metadata.current_selxn .= 0
                sq.select = "SELECT COUNT(*) AS count"
                push!(sq.metadata, Dict("name" => "count", "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => sq.from))

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



function final_collect(sqlquery::SQLQuery, ::Type{<:duckdb})
    final_query = finalize_query(sqlquery)
    result = DBInterface.execute(sqlquery.db, final_query)
    return DataFrame(result)
end

function final_collect(sqlquery::SQLQuery, ::Type{<:databricks})
    final_query = finalize_query(sqlquery)
    result = execute_databricks(sqlquery.db, final_query)
    return DataFrame(result)
end

function final_collect(sqlquery::SQLQuery, ::Type{<:snowflake})
    final_query = finalize_query(sqlquery)
    result = execute_snowflake(sqlquery.db, final_query)
    return DataFrame(result)
end
#using TidierDB
function stream_collect(sqlquery::SQLQuery)
    final_query = finalize_query(sqlquery)
    res = DBInterface.execute(sqlquery.db, final_query, DuckDB.StreamResult)

    # Helper function to get the non-Missing type from a Union{Missing, T}
    function non_missing_type(T)
        T === Missing && return Any
        T <: Union{Missing} ? non_missing_type(Base.typesplit(T)[2]) : T
    end

    # Initialize DataFrame with correct types
    df = DataFrame([name => Vector{non_missing_type(t)}() for (name, t) in zip(res.names, res.types)])

    while true
        chunk = DuckDB.nextDataChunk(res)
        chunk === missing && break  # All chunks processed

        for (col_idx, col_name) in enumerate(res.names)
            # Convert DuckDB data to Julia data
            duckdb_logical_type = DuckDB.LogicalType(DuckDB.duckdb_column_logical_type(res.handle, col_idx))
            duckdb_conversion_state = DuckDB.ColumnConversionData([chunk], col_idx, duckdb_logical_type, nothing)
            col_data = DuckDB.convert_column(duckdb_conversion_state)
            
            # Append the data to the DataFrame
            append!(df[!, col_name], col_data)
        end

        DuckDB.destroy_data_chunk(chunk)
    end

    return df
end


"""
$docstring_collect
"""
macro collect(sqlquery, stream = false)
    return quote
        backend = current_sql_mode[]
        if backend == duckdb()
            if $stream
                println("streaming")
                stream_collect($(esc(sqlquery)))
            else
                final_collect($(esc(sqlquery)), duckdb)
            end
        elseif backend == clickhouse()
            final_collect($(esc(sqlquery)), clickhouse)
        elseif backend == sqlite()
            final_collect($(esc(sqlquery)), sqlite)
        elseif backend == mysql()
            final_collect($(esc(sqlquery)), mysql)
        elseif backend == mssql()
            final_collect($(esc(sqlquery)), mssql)
        elseif backend == postgres()
            final_collect($(esc(sqlquery)), postgres)
        elseif backend == athena()
            final_collect($(esc(sqlquery)), athena)
        elseif backend == snowflake()
            final_collect($(esc(sqlquery)), snowflake)
        elseif backend == gbq()
            final_collect($(esc(sqlquery)), gbq)
        elseif backend == oracle()
            final_collect($(esc(sqlquery)), oracle)
        elseif backend == databricks()
            final_collect($(esc(sqlquery)), databricks)
        else
            throw(ArgumentError("Unsupported SQL mode: $backend"))
        end
    end
end


"""
$docstring_head
"""
macro head(sqlquery, value = 6)
    value = string(value)
    return quote
        sq = $(esc(sqlquery))
        if $value != ""
        sq.limit = $value
        end
        sq
    end
end

"""
$docstring_show_tables
"""
function show_tables(con::Union{DuckDB.DB, DuckDB.Connection})
    return DataFrame(DBInterface.execute(con, "SHOW ALL TABLES"))
end

