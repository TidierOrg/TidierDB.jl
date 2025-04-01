function get_distinct_values2(sq, names_col::String)
    final_sql = strip(finalize_query(sq))
    distinct_sql = "SELECT DISTINCT " * names_col * " FROM (" * final_sql * ") AS subquery"
    results = DBInterface.execute(sq.db, distinct_sql)
    pivot_values = [string(row[Symbol(names_col)]) for row in results]
    return pivot_values
end

function pivot_wider_sql(sq::SQLQuery, names_col::String, values_cols::Vector{String})
    cte_name = "cte_" * string(sq.cte_count + 1)
    build_cte!(sq)

    pivot_values = get_distinct_values2(sq, names_col)

    for pv in pivot_values
        for vc in values_cols
            new_col = if length(values_cols) > 1    
                    pv * "_" * vc
               else 
                    pv
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
        for vc in values_cols
            if length(values_cols) > 1    
             alias = pv * "_" * vc
            else 
                alias = pv
            end
            seg = "ANY_VALUE(" * vc * ") FILTER(WHERE " * names_col * " = '" * pv * "') AS " * alias
            push!(select_list, seg)
        end
    end

    pivot_sql = ", " * join(select_list, ", ")

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
    end

    return quote
       sq = $(esc(sqlquery))
       sq = sq.post_first ? t($(esc(sqlquery))) : sq
       sq.post_first = false

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
