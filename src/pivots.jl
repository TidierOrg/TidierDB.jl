function get_distinct_values2(sq, names_col::String)
    final_sql = strip(finalize_query(sq))
    distinct_sql = "SELECT DISTINCT " * names_col * " FROM (" * final_sql * ") AS subquery"
    results = DBInterface.execute(sq.db, distinct_sql)
    pivot_values = [string(row[Symbol(names_col)]) for row in results]
    return pivot_values
end

function pivot_wider_sql(sq::SQLQuery, names_col, values_cols::Vector{String})
    cte_name = "cte_" * string(sq.cte_count + 1)
    build_cte!(sq)
    if names_col isa Tuple 
        pivot_values = names_col[2]
        names_col = string(names_col[1])
    else
      pivot_values = get_distinct_values2(sq, names_col)
    end
    for pv in pivot_values
        spv = string(pv)  # Ensure pv is a string.
        for vc in values_cols
            new_col = if length(values_cols) > 1    
                    spv * "_" * vc
               else 
                    spv
               end
            push!(sq.metadata, Dict("name" => new_col,
                                    "type" => "UNKNOWN",
                                    "current_selxn" => 1,
                                    "table_name" => cte_name))
        end
    end

    for row in eachrow(sq.metadata)
        if row[:name] == names_col
            row.current_selxn = 0
        end
    end

    for row in eachrow(sq.metadata)
        for value in values_cols
            if row[:name] == value
                row.current_selxn = 0
            end
         end
    end

    select_list = []
    for pv in pivot_values
        spv = string(pv)
        for vc in values_cols
            alias = length(values_cols) > 1 ? spv * "_" * vc : spv
            seg = "ANY_VALUE(" * vc * ") FILTER(WHERE " * names_col * " = '" * spv * "') AS " * alias
            push!(select_list, seg)
        end
    end

    pivot_sql = ", " * join(select_list, ", ")

    return pivot_sql
end


"""
$docstring_pivot_wider
"""
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

    if names_from === nothing || values_from === nothing
        error("@pivot_wider2 requires that you specify names_from and values_from") # COV_EXCL_LINE
    end

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
    else 
        values_from = QuoteNode(values_from)
    end

    return quote
       sq = t($(esc(sqlquery)))

       local pivot_names = Any[]
       if $(esc(names_from)) isa Tuple
         push!(pivot_names, $(esc(names_from))[1])
       else
         push!(pivot_names, $(esc(names_from)))
       end
       local _values_from = $(esc(values_from))

       if isa(_values_from, AbstractArray)
           for v in _values_from
               push!(pivot_names, v)
           end
        elseif isa(_values_from, Expr)
            for v in filter_columns_by_expr([string(_values_from)], sq.metadata)
                push!(pivot_names, v)
            end
        else
           push!(pivot_names, _values_from)
       end
       pivot_names = string.(pivot_names)
       
       local selected_cols = String[]
       for (i, col) in enumerate(sq.metadata[!, :name])
           if sq.metadata.current_selxn[i] >= 1 && !(col in pivot_names)
               push!(selected_cols, col)
           end
       end

       local values_cols_vector = if isa(_values_from, AbstractArray)
           string.(collect(_values_from))
       else
          filter_columns_by_expr( [string(_values_from)], sq.metadata)
       end

       # Generate the pivot SELECT clause.
       if $(esc(names_from)) isa Tuple
         local pivot_select = pivot_wider_sql(sq, $(esc(names_from)), values_cols_vector)
       else
         local pivot_select = pivot_wider_sql(sq, string($(esc(names_from))), values_cols_vector)
       end
       # Update GROUP BY clause to use the non-pivot columns.
       sq.groupBy = join(selected_cols, ", ")

       sq.select =   sq.groupBy * pivot_select

       local cte_name = "cte_" * string(sq.cte_count + 1)
       local new_cte = CTE(name=cte_name,
                           select=sq.select,
                           from=(isempty(sq.ctes) ? sq.from : last(sq.ctes).name),
                           groupBy = sq.groupBy)
       up_cte_name(sq, cte_name)
       push!(sq.ctes, new_cte)
       
       sq.from = cte_name
       sq.cte_count += 1
       sq.where = ""
       sq.groupBy = ""
       sq.select = ""
       sq
    end
end


function pivot_longer_sql(sq::SQLQuery, pivot_cols::Vector{String},
                          names_to::String, values_to::String,
                          id_cols::Vector{String})
    cte_name = "cte_" * string(sq.cte_count + 1)
    build_cte!(sq)

    push!(sq.metadata, Dict("name" => names_to,
                            "type" => "UNKNOWN",
                            "current_selxn" => 1,
                            "table_name" => cte_name))
    push!(sq.metadata, Dict("name" => values_to,
                            "type" => "UNKNOWN",
                            "current_selxn" => 1,
                            "table_name" => cte_name))

    for row in eachrow(sq.metadata)
        if row[:name] in pivot_cols
            row.current_selxn = 0
        end
    end

    id_part = isempty(id_cols) ? "" : join(id_cols, ", ") * ", "
    selects = String[]
    for pc in pivot_cols
        seg = "SELECT " * id_part * "'" * pc * "' AS " * names_to *
              ", " * pc * " AS " * values_to * " FROM " * cte_name
        push!(selects, seg)
    end
    return join(selects, " UNION ALL ")
end


"""
$docstring_pivot_longer
"""
macro pivot_longer(sqlquery, args...)
    local cols      = nothing          # required
    local names_to  = "variable"       # default if omitted
    local values_to = "value"          # default if omitted

    for arg in args
        if isa(arg, Expr) && arg.head == :(=)
            key, val = arg.args
            if     key == :cols       cols      = val
            elseif key == :names_to   names_to  = val
            elseif key == :values_to  values_to = val
            end
        else
            cols === nothing && (cols = arg)   # first positional => cols
        end
    end
    cols === nothing && error("@pivot_longer requires that you specify cols")

    isa(names_to,  Symbol) && (names_to  = string(names_to))
    isa(values_to, Symbol) && (values_to = string(values_to))

    if isa(cols, Symbol)
        cols = [string(cols)]
    elseif isa(cols, Expr) && (cols.head == :vect || cols.head == :tuple)
        newc = [isa(c, Symbol) ? string(c) : c for c in cols.args]
        cols = Expr(:vect, newc...)
    else
        cols = QuoteNode(cols)
    end

    return quote
        sq = t($(esc(sqlquery)))

        _cols = $(esc(cols))
        pivot_cols_vector = isa(_cols, AbstractArray) ?
            string.(collect(_cols)) :
            filter_columns_by_expr([string(_cols)], sq.metadata)

        id_cols = String[]
        for (i, col) in enumerate(sq.metadata[!, :name])
            if sq.metadata.current_selxn[i] >= 1 && !(col in pivot_cols_vector)
                push!(id_cols, col)
            end
        end

        pivot_union_sql = pivot_longer_sql(sq,
                                           pivot_cols_vector,
                                           $(QuoteNode(names_to)),
                                           $(QuoteNode(values_to)),
                                           id_cols)

        sq.groupBy = ""
        sq.select  = pivot_union_sql

        cte_name = "cte_" * string(sq.cte_count + 1)
        new_cte  = CTE(name = cte_name,
                       select = sq.select,
                       from   = isempty(sq.ctes) ? sq.from : last(sq.ctes).name)
        up_cte_name(sq, cte_name)
        push!(sq.ctes, new_cte)

        sq.from      = cte_name
        sq.cte_count += 1
        sq.where     = ""
        sq.select    = ""
        sq
    end
end