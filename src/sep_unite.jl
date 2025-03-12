function separate(db, col, new_cols, sep)
    try
        separate_exprs = String[]
        n = length(new_cols)
        for (i, new_col) in enumerate(new_cols)
            if i < n
                push!(separate_exprs, "NULLIF(split_part(" * col * ", '" * sep * "', " * string(i) * "), '') AS " * new_col)
            else
                push!(separate_exprs, """
                    NULLIF(
                        CASE 
                            WHEN array_length(string_to_array(""" * col * """, '""" * sep * """')) >= """ * string(i) * """ 
                            THEN array_to_string(ARRAY(SELECT unnest(string_to_array(""" * col * """, '""" * sep * """')) OFFSET """ * string(i-1) * """), '""" * sep * """')
                            ELSE split_part(""" * col * """, '""" * sep * """', """ * string(i) * """)
                        END, ''
                    ) AS """ * new_col)
            end
        end
        for new_col in new_cols
            push!(db.metadata, Dict("name" => new_col,
                                     "type" => "VARCHAR",
                                     "current_selxn" => 0,
                                     "table_name" => last(db.metadata.table_name)))
        end
        return join(separate_exprs, ", "), new_cols
    catch e
        matching_indices = findall(==(col), db.metadata.name)
        throw("@separate only supports separating columns of type STRING at this time. $col is of type $(db.metadata.type[matching_indices[1]])")
    end
end

"""
$docstring_separate
"""
macro separate(sqlquery, col, new_cols, sep)
    # Convert the target column name to a literal string.
    col_name = string(col)
    # Extract the new column names from the vector literal (convert each to string).
    new_cols_names = [string(x) for x in new_cols.args]

    return quote
        # Evaluate the SQLQuery object.
        sq = $(esc(sqlquery))
        sq.post_unnest ? build_cte!($(esc(sqlquery))) : nothing
        names = replace.($new_cols_names, ":" => "")
        separate_str, names = separate(sq, $((QuoteNode(col_name))), names, $(esc(sep)))

        if occursin("", sq.select)
            # Expand SELECT clause from metadata if it contains "*".
            cols_from_meta = [
                string(row[:name])
                for row in eachrow(sq.metadata) if row[:current_selxn] == 1
            ]
            expanded_select = join(cols_from_meta, ", ")
            expanded_select = replace(expanded_select, Regex("\\b" * $(QuoteNode(col_name)) * "\\b") => separate_str)
            sq.select = startswith(sq.select, "SELECT") ? replace(sq.select, Regex("\\b" * $(QuoteNode(col_name)) * "\\b") => separate_str) :
                "SELECT " * expanded_select
        else
            # Otherwise, directly substitute in the defined SELECT clause.
            sq.select = replace(sq.select, Regex("\\b" * $(QuoteNode(col_name)) * "\\b") => separate_str)
        end
        # Mark the newly added separate columns as selected in metadata.
        length_names = length(names)
        for i in 1:length_names
            sq.metadata.current_selxn[end - i + 1] = 1
        end
        sq.metadata.current_selxn[sq.metadata.name .== $(QuoteNode(col_name))] .= 0
       sq.post_unnest = true
        sq
    end
end

function unite(sq, new_col, col_names, sep; remove=true)
    try
        for col in col_names
            sq.metadata.current_selxn[sq.metadata.name .== col] .= 0
        end
        sql_snippet = "CONCAT_WS('" * sep * "', " * join(col_names, ", ") * ") AS " * new_col
        if occursin("*", sq.select) || isempty(sq.select)
            new_select = join([string(row[:name])
                                for row in eachrow(sq.metadata) if row[:current_selxn] == 1], ", ")
            sq.select = "SELECT " * (isempty(new_select) ? "" : new_select * ", ") * sql_snippet
        else
            sq.select = sq.select * ", " * sql_snippet
        end
        # Add the new column to the metadata (assumed type VARCHAR).
        push!(sq.metadata, Dict("name" => new_col,
                                 "type" => "VARCHAR",
                                 "current_selxn" => 1,
                                 "table_name" => last(sq.metadata.table_name)))
        #return sql_snippet, col_names, remove
    catch e
        throw("@unite error: " * string(e))
    end
end

"""
$docstring_unite
"""
macro unite(sqlquery, new_col, col_tuple, sep, args...)
    # Set default value for `remove` to true.
    remove_expr = :(true)
    if length(args) > 0 && isa(args[end], Expr) && args[end].head == :(=) && args[end].args[1] == :remove
        remove_expr = args[end].args[2]
        args = args[1:end-1]  # Remove the keyword assignment from args.
    end

    # Convert the new column and tuple of columns into literal strings.
    new_col_str = string(new_col)
    col_names = [string(x) for x in col_tuple.args]
    return quote
        # Evaluate the SQLQuery object.
        sq = $(esc(sqlquery))
        cols = filter_columns_by_expr($col_names, sq.metadata)
        unite(sq, $(QuoteNode(new_col_str)), cols, $(esc(sep)); remove=$(esc(remove_expr)))
        sq.post_unnest = true
        sq
    end
end