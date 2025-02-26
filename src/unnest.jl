function unne(db, col)
    try
        cleaned_string = replace.(db.metadata.type[db.metadata.name .== col], "STRUCT(" => "", ")" => "")
        
        # Split the remaining string by ", " to separate the fields
        fields = split.(cleaned_string, ", ")[1]
        
        # Create vectors for names and types
        names = String[]
        types = String[]
        
        for field in fields
            # Split each field into name and type
            name_type = split.(field, " ")
            push!(names, name_type[1])  # First part is the name
            push!(types, name_type[2])  # Second part is the type
        end
        
        names_new = "$col" .* "." .* names .* " AS " .* names 
        for (name, type) in zip(names, types)
            push!(db.metadata, Dict("name" => name, "type" => type, "current_selxn" => 0, "table_name" => last(db.metadata.table_name)))
        end
      #  [println("names")]
        return join(names_new, ", "), names
    catch e
        matching_indices = findall(==(col), db.metadata.name)
       # println(db.metadata.type[matching_indices[1]])
        throw("@unnest only supports unnesting columns of type STRUCT at this time. $col is of type $(db.metadata.type[matching_indices[1]])")
    end
end

"""
$docstring_unnest_wider
"""
macro unnest(sqlquery, cols...)
    # Convert each provided column into its literal string form.
    col_names = [string(c) for c in cols]
    return quote
        # Evaluate the SQLQuery object.
        sq = $(esc(sqlquery))
        # Embed the list of column names as a literal vector.
        unnest_cols = $(QuoteNode(col_names))
        for col in unnest_cols
            # Generate the unnest SQL string and list of new names for this column.
            unnest_str, names = unne(sq, col)
            # If the current SELECT clause contains "*" then expand it from metadata.
            if occursin("", sq.select)
                # Build a list of column names from the metadata for which current_selxn == 1.
                cols_from_meta = [
                    string(row[:name])
                    for row in eachrow(sq.metadata) if row[:current_selxn] == 1
                ]
                # Join these names into a comma-separated string.
                expanded_select = join(cols_from_meta, ", ")
                # Replace the target column name with the unnest expansion.
                expanded_select = replace(expanded_select, col => unnest_str)
                println(sq.select)
                sq.select = startswith(sq.select, "SELECT") ? replace(sq.select, col => unnest_str) : 
                    sq.select = "SELECT " * expanded_select
            else
                # Otherwise, replace the column name directly in the already-defined SELECT clause.
                sq.select = replace(sq.select, col => unnest_str)
            end
            # Update the metadata: mark the newly added unnest columns as selected.
            length_names = length(names)
            for i in 1:length_names
                sq.metadata.current_selxn[end - i + 1] = 1
            end
        end
        sq
    end
end


export @unnest