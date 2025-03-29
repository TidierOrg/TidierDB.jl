function unne(db, col; names_sep=nothing)
    try
        cleaned_string = replace.(db.metadata.type[db.metadata.name .== col], "STRUCT(" => "", ")" => "")
        
        # Split the remaining string by ", " to separate the fields
        fields = split.(cleaned_string, ", ")[1]
        
        # Create vectors for names and types
        names = String[]
        types = String[]
        
        for field in fields
            # Split each field into name and type
            name_type = split(field, " ")
            push!(names, name_type[1])  # First part is the name
            push!(types, name_type[2])  # Second part is the type
        end
        names = replace.(names, "\""=> "")
        names2 = names_sep === nothing ? names : "$col" .* names_sep .* names
        names_new = "$col" .* "." .* names .* " AS " .* names2 
        for (name, type) in zip(names, types)
            push!(db.metadata, Dict("name" => name, "type" => type, "current_selxn" => 0, "table_name" => last(db.metadata.table_name)))
        end
        return join(names_new, ", "), names
    catch e
        matching_indices = findall(==(col), db.metadata.name)
        throw("@unnest only supports unnesting columns of type STRUCT at this time. $col is of type $(db.metadata.type[matching_indices[1]])") # COV_EXCL_LINE
    end
end


"""
$docstring_unnest_wider
"""
macro unnest_wider(sqlquery, cols...)
    # Initialize an expression for names_sep (default is nothing)
    names_sep_expr = nothing
    # If the last argument is a keyword assignment for names_sep, extract it.
    if length(cols) > 0 && isa(cols[end], Expr) &&
       cols[end].head == :(=) && cols[end].args[1] == :names_sep
        names_sep_expr = cols[end].args[2]
        cols = cols[1:end-1]  # Remove the keyword arg from cols
    end

    # Convert each provided column into its literal string form.
    col_names = [string(c) for c in cols]
    
    return quote
        # Evaluate the SQLQuery object.
        sq = $(esc(sqlquery))
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 
        
        sq.post_unnest ? build_cte!(sq) : nothing

        unnest_cols = filter_columns_by_expr($col_names, sq.metadata)
        for col in unnest_cols
            # Generate the unnest SQL string and list of new names for this column.
            unnest_str, names = unne(sq, col; names_sep=$(esc(names_sep_expr)))
            # If the current SELECT clause contains "*" then expand it from metadata.
            if occursin("", sq.select)
                # Build a list of column names from the metadata for which current_selxn == 1.
                cols_from_meta = [
                    string(row[:name])
                    for row in eachrow(sq.metadata) if row[:current_selxn] == 1
                ]
                # Join these names into a comma-separated string.
                expanded_select = join(cols_from_meta, ", ")
                # Replace the target column name with the unnest expansion using exact matching.
                expanded_select = replace(expanded_select, Regex("\\b" * col * "\\b") => unnest_str)
                sq.select = startswith(sq.select, "SELECT") ? replace(sq.select, Regex("\\b" * col * "\\b") => unnest_str) :
                    sq.select = "SELECT " * expanded_select
            else
                # Otherwise, replace the column name directly in the already-defined SELECT clause with exact matching.
                sq.select = replace(sq.select, Regex("\\b" * col * "\\b") => unnest_str)
            end
            # Update the metadata: mark the newly added unnest columns as selected.
            length_names = length(names)
            for i in 1:length_names
                sq.metadata.current_selxn[end - i + 1] = 1
            end
            sq.metadata.current_selxn[sq.metadata.name .== col] .= 0
        end
        sq.post_unnest = true
        sq
    end
end

"""
$docstring_unnest_longer
"""
macro unnest_longer(sqlquery, cols...)
    # Convert each provided column into its literal string form.
    col_names = [string(c) for c in cols]
    return quote
        # Evaluate the SQLQuery object.
        sq = $(esc(sqlquery))
        sq = sq.post_first ? t($(esc(sqlquery))) : sq
        sq.post_first = false; 

        sq.post_unnest ? build_cte!(sq) : nothing
        # Embed the list of column names as a literal vector.
        unnest_cols = filter_columns_by_expr($col_names, sq.metadata)
        for col in unnest_cols
            # Build the unnest string in the form: unnest(col) AS col
            unnest_str = " unnest(" * col * ") AS " * col
            # Build a regex pattern that matches the target column only if it is preceded by
            # whitespace or start-of-string and followed by whitespace, comma, or end-of-string.
            pattern = r"(?<=^|\s)" * col * r"(?=\s|,|$)"
            if occursin("*", sq.select)
                # The SELECT clause contains a wildcard, so we expand the list of columns from metadata.
                cols_from_meta = [
                    string(row[:name])
                    for row in eachrow(sq.metadata) if row[:current_selxn] >= 1
                ]
                expanded_select = join(cols_from_meta, ", ")
                # Replace the target column with its unnest expansion.
                expanded_select = replace(expanded_select, pattern => unnest_str)
                # If the SELECT clause already starts with "SELECT", use replacement,
                # otherwise prepend "SELECT " to the expanded string.
                sq.select = startswith(sq.select, "SELECT") ? replace(sq.select, pattern => unnest_str) :
                    "SELECT " * expanded_select
            else
                # Else branch: build the expanded SELECT clause once from metadata,
                # then loop over all unnest columns to update it.
                cols_from_meta = [
                    string(row[:name])
                    for row in eachrow(sq.metadata) if row[:current_selxn] >= 1
                ]
                expanded_select = join(cols_from_meta, ", ")
                for col_inner in unnest_cols
                    # For each unnest column, build its unnest string and regex pattern.
                    local unnest_str_inner = "unnest(" * col_inner * ") AS " * col_inner
                    local pattern_inner = r"(?<=^|\s)" * col_inner * r"(?=\s|,|$)"
                    expanded_select = replace(expanded_select, pattern_inner => unnest_str_inner)
                end
                sq.select = "SELECT " * expanded_select
            end
        end
        sq
    end
end