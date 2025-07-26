"""
$docstring_select
"""
macro select(sqlquery, exprs...)
    exprs = parse_blocks(exprs...)
    return quote
        exprs_str = map(expr -> isa(expr, Symbol) ? string(expr) : expr, $exprs)
        sq = t($(esc(sqlquery)))
        if sq.select != "" build_cte!(sq); sq.select == ""; end
        let columns = parse_tidy_db(exprs_str, sq.metadata)
            columns_str = join(["SELECT ", join([string(column) for column in columns], ", ")])
            sq.select = columns_str
            sq.metadata.current_selxn .= 0
            for col in columns
                if occursin(".", col)
                    table_col_split = split(col, ".")
                    table_name, col_name = table_col_split[1], table_col_split[2]
                    for idx in eachindex(sq.metadata.current_selxn)
                        if sq.metadata.table_name[idx] == table_name && sq.metadata.name[idx] == col_name
                            sq.metadata.current_selxn[idx] = 2
                        end
                    end
                else
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
        sq = t($(esc(sqlquery)))

        if isa(sq, SQLQuery)
            if !sq.is_aggregated
                if sq.post_join || sq.post_mutate 
                    combined_conditions = String[]
                    for condition in $(esc(conditions))
                        condition_str = string(expr_to_sql(condition, sq))
                        condition_str = replace(condition_str, "'\"" => "'",  "'\"" => "'", "\"'" => "'", "[" => "(", "]" => ")")
                        push!(combined_conditions, condition_str)
                    end
                    combined_condition_str = join(combined_conditions, " AND ")

                    sq.where = " WHERE " * combined_condition_str
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
            #    println(sq.from)
                build_cte!(sq)
                sq.select = " * "
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
                cte_name = "cte_" * string(sq.cte_count + 1)
                select_expressions = !isempty(sq.select) ? [sq.select] : ["*"]
                cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
                new_cte = CTE(name=cte_name, select=cte_sql, groupBy = sq.groupBy, having=sq.having)
                up_cte_name(sq, cte_name)
                
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
        sq = t($(esc(sqlquery)))
        
        sq.orderBy = " ORDER BY " * $order_clause
        sq
    end
end


function groupby_exp(expr, sq)
    if isa(expr, Expr) && expr.head == :(=) && isa(expr.args[1], Symbol)
        # Extract column alias name as string
        col_name = string(expr.args[1])
        if current_sql_mode[] == snowflake()
            col_name = uppercase(col_name)  # COV_EXCL_LINE
        end
        push!(sq.metadata, Dict("name" => col_name, "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => "table"))
        # Convert the right-hand side expression to a SQL expression
        col_expr = expr_to_sql(expr.args[2], sq)
        col_expr = string(col_expr)
        # Return the alias and the SQL snippet (wrapped in parentheses to be safe)
        return col_name, "(" * col_expr * ") AS " * col_name
    else
        error("Unsupported expression in @group_by: $(expr)")
    end
end

"""
$docstring_group_by
"""
macro group_by(sqlquery, columns...)
    columns = parse_blocks(columns...)
    return quote
        columns_str = map(col -> isa(col, Symbol) ? string(col) : col, $columns)
        sq = t($(esc(sqlquery)))
        if isa(sq, SQLQuery)
                try
                    let group_columns = parse_tidy_db(columns_str, sq.metadata)
                        sq.groupBy = "GROUP BY " * join(group_columns, ", ")
                        current = group_columns
                        rest    = split(sq.select, ", ")[2:end]
                        allcols = unique(vcat(current, rest))
                        sq.select = "SELECT " * join(allcols, ", ")
                    end
                catch
                    sq.groupBy_exprs = true
                    local group_expressions = String[]
                    local group_aliases    = String[]

                    for col in $columns
                        if isa(col, Expr) && col.head == :(=)
                            let tup = groupby_exp(col, sq)
                                push!(group_expressions, tup[2])
                                push!(group_aliases,    tup[1])
                            end
                        else
                            for nm in parse_tidy_db([col], sq.metadata)
                                push!(group_expressions, nm)
                                push!(group_aliases,    nm)
                            end
                        end
                    end

                    sq.groupBy = "GROUP BY " * join(group_aliases, ", ")
                    local orig = filter(x->!isempty(strip(x)), split(sq.select, ", "))
                    if !isempty(orig) && startswith(orig[1], "SELECT ")
                        orig[1] = replace(orig[1], "SELECT " => "")
                    end
                    sq.select = "SELECT " * join(vcat(group_expressions, orig), ", ")
                    sq.select = replace(sq.select, " all," => "")
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
        sq = t($(esc(sqlquery)))

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
        sq = t($(esc(sqlquery)))
        sq.post_count = true
        sq.is_aggregated = true
        if isa(sq, SQLQuery)
            # If grouping columns are specified.
            if !isempty($group_clause)
                for col in $group_by_cols_str
                    sq.metadata.current_selxn .= 0
                    matching_indices = findall(sq.metadata.name .== col)
                    sq.metadata.current_selxn[matching_indices] .= 1
                end
                sq.select = "SELECT " * $group_clause * ", COUNT(*) AS n"
                sq.groupBy = "GROUP BY " * $group_clause
                push!(sq.metadata, Dict("name" => "n", "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => sq.from))
            else
                # If no grouping columns, simply count all records.
                sq.metadata.current_selxn .= 0
                sq.select = "SELECT COUNT(*) AS n"
                push!(sq.metadata, Dict("name" => "n", "type" => "UNKNOWN", "current_selxn" => 1, "table_name" => sq.from))
            end

            if !isempty($group_clause) && $(esc(sort_expr))
                sq.orderBy = "ORDER BY n DESC"
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

        sq = t($(esc(sqlquery)))
        
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

mutable struct DBQuery
    val::String
end

function Base.show(io::IO, ::MIME"text/plain", mytype::DBQuery)
    print(io, mytype.val)
end

macro show_query(sqlquery)
    return quote
        final_query = finalize_query($(esc(sqlquery)))  
        formatted_query = format_sql_query(final_query)
       
        display(DBQuery(formatted_query))
        # $(esc(sqlquery));
    end
    
end

# COV_EXCL_START
function format_sql_query(final_query::String)
    # Format basic SQL structure with newlines and indentation
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
    formatted_query = replace(formatted_query, " ANY_VALUE" => "\n\tANY_VALUE")
    
    # pattern for SQL keywords
    pattern = r"\b(cte_\w+|WITH|FROM|SELECT|AS|LEFT|JOIN|RIGHT|OUTER|UNION|INNER|ASOF|GROUP\s+BY|CASE|WHEN|THEN|ELSE|END|WHERE|HAVING|ORDER\s+BY|PARTITION|ASC|DESC|INNER)\b"
    
    if TidierDB.color[]
        formatted_query = replace(formatted_query, pattern => s -> begin
            token = String(s)  
            token_upper = uppercase(strip(token))
            
            if token_upper in ["FROM", "SELECT", "WITH"]
                "\e[36m$(token)\e[0m"  # Cyan
            elseif token_upper in ["AS"]
                "\e[32m$(token)\e[0m"  # Green
            elseif token_upper in ["ASOF", "RIGHT", "LEFT", "OUTER", "SEMI", "JOIN", "INNER"]
                "\e[34m$(token)\e[0m"  # Blue
            elseif occursin(r"^GROUP\s+BY$", token_upper)
                "\e[33m$(token)\e[0m"  # Yellow
            elseif token_upper in ["CASE", "WHEN", "THEN", "ELSE", "END"]
                "\e[38;5;208m$(token)\e[0m"  # Orange
            elseif token_upper in ["WHERE", "HAVING"]
                "\e[94m$(token)\e[0m"  # Light Blue
            elseif occursin(r"^ORDER\s+BY$", token_upper)
                "\e[35m$(token)\e[0m"  # Pink
            elseif token_upper in ["ASC", "DESC", "PARTITION"]
                "\e[35m$(token)\e[0m"  # Pink
            else
                token
            end
        end)
    end
    
    return formatted_query
end
# COV_EXCL_STOP

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
        sq = t($(esc(sqlquery)))
        
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
            sq = t($(esc(sqlquery)))
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
                sq = t($(esc(sqlquery)))
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