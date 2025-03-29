"""
$docstring_select
"""
macro select(sqlquery, exprs...)

    exprs = parse_blocks(exprs...)

    return quote
        exprs_str = map(expr -> isa(expr, Symbol) ? string(expr) : expr, $exprs)
        sq = $(esc(sqlquery))
        sq = sq.post_first ? (t($(esc(sqlquery)))) : sq
        sq.post_first = false; 

        if sq.select != "" build_cte!(sq)  end
        let columns = parse_tidy_db(exprs_str, sq.metadata)
            columns_str = join(["SELECT ", join([string(column) for column in columns], ", ")])
            sq.select = columns_str
            sq.metadata.current_selxn .= 0
            for col in columns
                if occursin(".", col)
                    table_col_split = split(col, ".")
                    table_name, col_name = table_col_split[1], table_col_split[2]

                    # Iterate and update current_selxn based on matches
                    for idx in eachindex(sq.metadata.current_selxn)
                        if sq.metadata.table_name[idx] == table_name && 
                           sq.metadata.name[idx] == col_name
                            sq.metadata.current_selxn[idx] = 2
                        end
                    end
                else
                    # Direct matching for columns without 'table.' prefix
                    matching_indices = findall(sq.metadata.name .== col)
                    sq.metadata.current_selxn[matching_indices] .= 1
                end
            end
        end
        
        sq
    end
end

"""
$docstring_filter
"""
macro filter(sqlquery, conditions...)
    conditions = parse_blocks(conditions...)

    return quote
        sq = $(esc(sqlquery))
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 

        if isa(sq, SQLQuery)
            if !sq.is_aggregated
                if sq.post_join
                    combined_conditions = String[]
                    for condition in $(esc(conditions))
                        condition_str = string(expr_to_sql(condition, sq))
                        condition_str = replace(condition_str, "'\"" => "'",  "'\"" => "'", "\"'" => "'", "[" => "(", "]" => ")")
                        push!(combined_conditions, condition_str)
                    end
                    combined_condition_str = join(combined_conditions, " AND ")

                    sq.where = " WHERE " * combined_condition_str
                  #  sq.post_join = false
                else
                cte_name = "cte_" * string(sq.cte_count + 1)
                combined_conditions = String[]
                for condition in $(esc(conditions))
                    condition_str = string(expr_to_sql(condition, sq))
                    condition_str = replace(condition_str, "'\"" => "'",  "'\"" => "'", "\"'" => "'", "[" => "(", "]" => ")")
                    push!(combined_conditions, condition_str)
                end
                combined_condition_str = join(combined_conditions, " AND ")

                sq.where = combined_condition_str
                build_cte!(sq)
            end
            else
            aggregated_columns = Set{String}()
            
            if !isempty(sq.select)
                for part in split(sq.select, ", ")
                    if occursin(" AS ", part)
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
                sq.where = "WHERE " * join(non_aggregated_conditions, " AND ")
                build_cte!(sq)
            end
        end

        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
       
        sq
    end
end



function _colref_to_string(col)
    if isa(col, Symbol)
        return string(col)
    elseif isa(col, Expr) && col.head === :.
        parent_str = _colref_to_string(col.args[1])
        field_str  = string(col.args[2].value)
        return parent_str * "." * field_str
    else
        throw("Unsupported column reference: $col")
    end
end

"""
$docstring_arrange
"""
macro arrange(sqlquery, columns...)
    # Build up the ORDER BY specs
    order_specs = String[]
    
    for col in columns
        if isa(col, Expr) && col.head == :call && col.args[1] == :desc
            # Example: `desc(sales.id)` or `desc(:col)`
            # Descending
            colstr = _colref_to_string(col.args[2])
            push!(order_specs, colstr * " DESC")
        else
            # Ascending
            colstr = _colref_to_string(col)
            push!(order_specs, colstr * " ASC")
        end
    end

    # Construct ORDER BY clause
    order_clause = join(order_specs, ", ")

    return quote
        sq = $(esc(sqlquery))
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 
            sq.orderBy = " ORDER BY " * $order_clause
        sq
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
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 
        if isa(sq, SQLQuery)

            let group_columns = parse_tidy_db(columns_str, sq.metadata)
                group_clause = "GROUP BY " * join(group_columns, ", ")
                
                sq.groupBy = group_clause

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
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false;

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
            
            cte = CTE(name=cte_name, select=cte_select)
            push!(sq.ctes, cte)
            sq.from = cte_name
            sq.select = "*"
        end
        sq
    end
end

"""
$docstring_count
"""
macro count(sqlquery, group_by_columns...)
    # Set default sort expression to true.
    sort_expr = :(true)

    if length(group_by_columns) > 0 &&
       isa(group_by_columns[end], Expr) &&
       group_by_columns[end].head == :(=) &&
       group_by_columns[end].args[1] == :sort
        sort_expr = group_by_columns[end].args[2]
        group_by_columns = group_by_columns[1:end-1]  # Remove the sort keyword argument.
    end

    # Convert the grouping columns to string representations.
    group_by_cols_str = [string(col) for col in group_by_columns]
    group_clause = join(group_by_cols_str, ", ")

    return quote
        sq = $(esc(sqlquery))
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 
        
        if isa(sq, SQLQuery)
            # If grouping columns are specified.
            if !isempty($group_clause)
                for col in $group_by_cols_str
                    sq.metadata.current_selxn .= 0
                    matching_indices = findall(sq.metadata.name .== col)
                    sq.metadata.current_selxn[matching_indices] .= 1
                end
                sq.select = "SELECT " * $group_clause * ", COUNT(*) AS count"
                sq.groupBy = "GROUP BY " * $group_clause
                push!(sq.metadata, Dict("name" => "count", "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => sq.from))
            else
                # If no grouping columns, simply count all records.
                sq.metadata.current_selxn .= 0
                sq.select = "SELECT COUNT(*) AS count"
                push!(sq.metadata, Dict("name" => "count", "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => sq.from))
            end

            if !isempty($group_clause) && $(esc(sort_expr))
                sq.orderBy = "ORDER BY count DESC"
            end
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
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 
        
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

# COV_EXCL_START
cyan_crayon       = Crayon(foreground = :cyan, bold = true)         # for FROM and SELECT
blue_crayon       = Crayon(foreground = :blue, bold = true)         # for JOINs
yellow_crayon     = Crayon(foreground = :yellow, bold = true)       # for GROUP BY
orange_crayon     = Crayon(foreground = 208, bold = true)           # for CASE, WHEN, THEN, ELSE, END (208 is a common orange color)
lightblue_crayon  = Crayon(foreground = :light_blue, bold = true)      # for WHERE
pink_crayon       = Crayon(foreground = :magenta, bold = true)
cyan_crayon       = Crayon(foreground = :cyan, bold = true)         # for FROM and SELECT
light_gray        = Crayon(foreground = :red, bold = true)
green             = Crayon(foreground = :green, bold = false)
# COV_EXCL_STOP


macro show_query(sqlquery)
    return quote
        final_query = finalize_query($(esc(sqlquery)))
        
        formatted_query = replace(final_query, r"(?<=\)), " => ",\n")
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
        formatted_query = replace(formatted_query, " ASOF " => "\n\tASOF ")
        formatted_query = replace(formatted_query, " LIMIT " => "\n\tLIMIT ")
        
        pattern = r"\b(cte_\w+|WITH|FROM|SELECT|AS|LEFT|JOIN|RIGHT|OUTER|UNION|INNER|ASOF|GROUP\s+BY|CASE|WHEN|THEN|ELSE|END|WHERE|HAVING|ORDER\s+BY|PARTITION|ASC|DESC|INNER)\b"
        # COV_EXCL_START
        if TidierDB.color[]
            formatted_query = replace(formatted_query, pattern => s -> begin
                token = String(s)  
                token_upper = uppercase(strip(token))
                
                if token_upper in ["FROM", "SELECT", "WITH"]
                    return $cyan_crayon(token)
                elseif token_upper in ["AS"]
                    return $green(token)
                elseif token_upper in ["ASOF", "RIGHT", "LEFT", "OUTER", "SEMI", "JOIN", "INNER"]
                    return $blue_crayon(token)
                elseif occursin(r"^GROUP\s+BY$", token_upper)
                    return $yellow_crayon(token)
                elseif token_upper in ["CASE", "WHEN", "THEN", "ELSE", "END"]
                    return $orange_crayon(token)
                elseif token_upper in ["WHERE", "HAVING"]
                    return $lightblue_crayon(token)
                elseif occursin(r"^ORDER\s+BY$", token_upper)
                    return $pink_crayon(token)
                elseif token_upper in ["ASC", "DESC", "PARTITION"]
                    return $pink_crayon(token)
             #   elseif occursin(r"^CTE_\w+$", token_upper)
              #      return $light_magenta(token)                
                else
                    return token  
                end
            end)
        end
        # COV_EXCL_STOP
        println(formatted_query)
    end
end




function final_collect(sqlquery::SQLQuery, ::Type{<:duckdb})
    final_query = finalize_query(sqlquery)
    result = DBInterface.execute(sqlquery.db, final_query)
    return DataFrame(result)
end

# COV_EXCL_START
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
# COV_EXCL_STOP


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
        # COV_EXCL_START 
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
        # COV_EXCL_STOP
    end
end


"""
$docstring_head
"""
macro head(sqlquery, value = 6)
    value = string(value)
    return quote
        sq = $(esc(sqlquery))
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 
        
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

"""
$docstring_drop_missing
"""
macro drop_missing(sqlquery, columns...)
    if isempty(columns)
        return quote
            # Get the SQLQuery instance.
            sq = $(esc(sqlquery))
            sq = sq.post_first ? (t($(esc(sqlquery)))) : sq
            sq.post_first = false; 
            if isa(sq, SQLQuery)
                # Determine columns to process: use those with metadata.current_selxn >= 1.
                selected_cols = String[]
                for i in eachindex(sq.metadata.current_selxn)
                    if sq.metadata.current_selxn[i] >= 1
                        push!(selected_cols, sq.metadata.name[i])
                    end
                end

                # Build the combined SQL condition: each column must be NOT NULL.
                condition_parts = String[]
                for col in selected_cols
                    push!(condition_parts, string(col) * " IS NOT NULL")
                end
                combined_condition_str = join(condition_parts, " AND ")

                # Create a new CTE.
                cte_name = "cte_" * string(sq.cte_count + 1)
                new_cte = CTE(name=cte_name,
                              select="*",
                              from=(isempty(sq.ctes) ? sq.from : last(sq.ctes).name),
                              where=combined_condition_str)
                up_cte_name(sq, cte_name)
                push!(sq.ctes, new_cte)
                sq.where = " WHERE " * combined_condition_str
                sq.from = cte_name
                sq.cte_count += 1
                sq.where = ""
            else
                error("Expected sqlquery to be an instance of SQLQuery")
            end
            sq
        end
    else
        # When columns are provided, convert them (at macro expansion) to a literal vector of strings.
        local cols_literal = [string(col) for col in columns]
        
        return quote
            sq = $(esc(sqlquery))
            if isa(sq, SQLQuery)
                sq = sq.post_first ? (t($(esc(sqlquery)))) : sq
                sq.post_first = false; 
                selected_cols = filter_columns_by_expr($cols_literal, sq.metadata)

              #  selected_cols = $(QuoteNode(cols_literal))
                condition_parts = String[]
                for col in selected_cols
                    push!(condition_parts, string(col) * " IS NOT NULL")
                end
                combined_condition_str = join(condition_parts, " AND ")

                # Create a new CTE.
                cte_name = "cte_" * string(sq.cte_count + 1)
                new_cte = CTE(name=cte_name,
                              select="*",
                              from=(isempty(sq.ctes) ? sq.from : last(sq.ctes).name),
                              where=combined_condition_str)
                up_cte_name(sq, cte_name)
                push!(sq.ctes, new_cte)
                sq.where = " WHERE " * combined_condition_str
                sq.from = cte_name
                sq.cte_count += 1
                sq.where = ""
            else
                error("Expected sqlquery to be an instance of SQLQuery")
            end
            sq
        end
    end
end