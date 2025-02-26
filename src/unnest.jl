
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
macro unnest(sqlquery, col)
    # Capture the column name as a literal string at macro expansion time.
    col_name_literal = string(col)
    return quote
        # Evaluate the sqlquery object.
        sq = $(esc(sqlquery))
        col_name = $(QuoteNode(col_name_literal))
        if isa(sq, SQLQuery)
            # Generate the unnest SQL string for the target column.
            unnest_str, names = unne(sq, col_name)
            #n(names)
            #println(sq.select)
            # If the SELECT clause contains "*", then expand it from metadata.
            if occursin("", sq.select)
              #  println(sq.select)
                # Filter metadata to list out all the columns that should be selected.
                # For this example we assume that columns with current_selxn == 1 are the ones to list.
                cols = [
                    # Only include the column name without the table name prefix.
                    string(row[:name])
                    for row in eachrow(sq.metadata) if row[:current_selxn] == 1
                ]  
               # cols = vcat(cols, names)
            #   println(cols)
                #.* " " .* names  # Include all names in the names vector
                # Join the column names into a SELECT clause.
                expanded_select = join(cols, ", ")
                # Replace the target column with the unnest expansion.
                expanded_select = replace(expanded_select, col_name => unnest_str)

                sq.select = "SELECT " * expanded_select
             #   println(sq.select)
                length_names = length(names)
                for i in 1:length_names
                    sq.metadata.current_selxn[end - i + 1] = 1
                end
            else
            #    println("Lelse")
                # Otherwise, assume sq.select already lists the columns.
                sq.select = replace(sq.select, col_name => unnest_str)
            length_names = length(names)
            for i in 1:length_names
                sq.metadata.current_selxn[end - i + 1] = 1
            end
            end
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end
export @unnest