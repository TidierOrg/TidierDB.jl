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

function get_distinct_values2(sq, names_col::String)
    final_sql = strip(finalize_query(sq))
  #  println("\nFinal SQL for distinct: ", final_sql)
    distinct_sql = "SELECT DISTINCT " * names_col * " FROM (" * final_sql * ") AS subquery"
   # println("\nDistinct SQL: ", distinct_sql)
    results = DBInterface.execute(sq.db, distinct_sql)
    pivot_values = [string(row[Symbol(names_col)]) for row in results]
   # println("Pivot values: ", pivot_values)
    return pivot_values
end


function pivot_wider_sql(sq::SQLQuery, names_col::String, values_cols::Vector{String})
    # Create a new CTE to freeze the current query state.
    cte_name = "cte_" * string(sq.cte_count + 1)
    build_cte!(sq)

    # Get the distinct pivot values for names_col.
    pivot_values = get_distinct_values2(sq, names_col)
   # println("Pivot values (from helper): ", pivot_values)
    
    # For each distinct pivot value and for each values_col,
    # add a new metadata column with alias "<pivot>_<values_col>".
    for pv in pivot_values
        for vc in values_cols
            new_col = pv * "_" * vc
            push!(sq.metadata, Dict("name" => new_col,
                                    "type" => "UNKNOWN",
                                    "current_selxn" => 1,
                                    "table_name" => cte_name))
        end
    end

    # Mark the original pivot (names_from) column as dropped.
    for row in eachrow(sq.metadata)
        if row[:name] == names_col
            row.current_selxn = 0
        end
    end
    # Also mark any columns that match a values_from entry as dropped.
    for row in eachrow(sq.metadata)
        for value in values_cols
            if row[:name] == value
                row.current_selxn = 0
            end
         end
    end

    # Use the provided id_col or, by default, assume the first metadata column is the identifier.
    # (Using DataFrame syntax to get the "name" column)
    id_col = sq.metadata[!, :name][1]

    # Build the pivot SELECT list. Start with the identifier column.
    select_list = [id_col]
    # For each distinct pivot value and each values_col, generate a pivot segment.
    for pv in pivot_values
        for vc in values_cols
            alias = pv * "_" * vc
            seg = "ANY_VALUE(" * vc * ") FILTER(WHERE " * names_col * " = '" * pv * "') AS " * alias
            push!(select_list, seg)
        end
    end

    pivot_sql = join(select_list, ", ")
   # println("\nGenerated pivot SQL:\n", replace(pivot_sql, "ANY_VALUE" => "\nANY_VALUE"))
    return pivot_sql
end

macro pivot_wider(sqlquery, args...)
    # Initialize parameters.
    local names_from = nothing
    local values_from = nothing

    
    # Process each extra argument.
    for arg in args
        if isa(arg, Expr) && arg.head == :(=)
            if arg.args[1] == :names_from
                names_from = arg.args[2]
            elseif arg.args[1] == :values_from
                values_from = arg.args[2]
            end
        end
    end

    # Ensure the required parameters are provided.
    if names_from === nothing || values_from === nothing
        error("@pivot_wider2 requires that you specify names_from and values_from")
    end

    # --- Convert bare symbols to string literals ---
    if isa(names_from, Symbol)
        names_from = string(names_from)
    end
    if isa(values_from, Symbol)
        values_from = [string(values_from)]
    elseif isa(values_from, Expr) && values_from.head == :vect
        local new_vals = []
        for v in values_from.args
            if isa(v, Symbol)
                push!(new_vals, string(v))
            else
                push!(new_vals, v)
            end
        end
        values_from = Expr(:vect, new_vals...)
    end


    return quote
       # Evaluate the SQLQuery object.
       sq = $(esc(sqlquery))
       sq = sq.post_first ? t($(esc(sqlquery))) : sq
       sq.post_first = false

       # Build pivot_names vector for excluding these columns from GROUP BY.
       local pivot_names = Any[]
       push!(pivot_names, $(esc(names_from)))
       local _values_from = $(esc(values_from))
       if isa(_values_from, AbstractArray)
           for v in _values_from
               push!(pivot_names, v)
           end
       else
           push!(pivot_names, _values_from)
       end
       pivot_names = string.(pivot_names)
       
       # Build list of selected columns: those in metadata with current_selxn>=1 and not in pivot_names.
       local selected_cols = String[]
       for (i, col) in enumerate(sq.metadata[!, :name])
           if sq.metadata.current_selxn[i] >= 1 && !(col in pivot_names)
               push!(selected_cols, col)
           end
       end

       # Ensure values_from is a vector of strings.
       local values_cols_vector = if isa(_values_from, AbstractArray)
           string.(collect(_values_from))
       else
           [string(_values_from)]
       end

       # Generate the pivot SELECT clause.
       local pivot_select = pivot_wider_sql(sq, string($(esc(names_from))), values_cols_vector)
       # Update GROUP BY clause to use the non-pivot columns.
       sq.groupBy = join(selected_cols, ", ")
      # println("Updated GROUP BY: ", sq.groupBy)
      # println(pivot_select)
       # Set the generated pivot SELECT clause.
      # println("Pivot SELECT: ", pivot_select)
       sq.select = sq.groupBy * ", " * pivot_select

       # Create a new CTE with the pivot query.
       local cte_name = "cte_" * string(sq.cte_count + 1)
       local new_cte = CTE(name=cte_name,
                           select=sq.select,
                           from=(isempty(sq.ctes) ? sq.from : last(sq.ctes).name),
                           groupBy = sq.groupBy)
       up_cte_name(sq, cte_name)
       push!(sq.ctes, new_cte)
       
       # Update query state.
       sq.from = cte_name
       sq.cte_count += 1
       sq.where = ""
       sq.groupBy = ""
       sq.select = ""
       sq
    end
end
