
# Handle arguments that could be a single symbol or a vector of symbols
function symbol_or_vector_to_strings(arg)
    if isa(arg, Expr) && arg.head == :vect
        # Arg is something like [:colA, :colB]
        return [symbol_to_string(a) for a in arg.args]
    else
        # Arg is a single symbol like :colA
        return [symbol_to_string(arg)]
    end
end

function parse_relocate_args(exprs)
    cols = Symbol[]  # columns to relocate
    before_col = nothing
    after_col = nothing
    
    for arg in exprs
        if isa(arg, Expr) && arg.head === :(=) && length(arg.args) == 2
            lhs, rhs = arg.args[1], arg.args[2]
            if lhs === :before
                parsed_before = symbol_or_vector_to_strings(rhs)
                if length(parsed_before) > 1
                    error("The 'before' argument currently supports only a single column.")
                end
                before_col = parsed_before[1]
            elseif lhs === :after
                parsed_after = symbol_or_vector_to_strings(rhs)
                if length(parsed_after) > 1
                    error("The 'after' argument currently supports only a single column.")
                end
                after_col = parsed_after[1]
            else
                # Unknown keyword argument; handle gracefully or ignore
            end
        else
            # Not an assignment expression, so it should be a column reference
            push!(cols, arg isa Symbol ? arg : Symbol(arg))
        end
    end
    return cols, before_col, after_col
end

"""
$docstring_relocate
"""
macro relocate(sqlquery, exprs...)
    # Convert a Symbol, String, or QuoteNode(Symbol) to String
  

    # Parse arguments
    cols, before_col, after_col = parse_relocate_args(exprs)

    return quote
        meta = $(esc(sqlquery)).metadata

        # Identify currently selected columns (current_selxn > 0)
        selected_indices = findall(x -> x > 0, meta.current_selxn)
        currently_selected_cols = meta.name[selected_indices]

        # Columns to move
        move_str = []

        to_move_str = filter_columns_by_expr($cols, $(esc(sqlquery)).metadata)

        before_str = isnothing($before_col) ? nothing : string($before_col)
        after_str  = isnothing($after_col)  ? nothing : string($after_col)
        
        # Check that columns to move are currently selected
        for c in to_move_str
            if c ∉ currently_selected_cols
                error("Column $(c) not found among currently selected columns.")
            end
        end

        # Determine the target index for insertion
        if before_str !== nothing
            idx = findfirst(==(before_str), currently_selected_cols)
            if idx === nothing
                error("The 'before' column $(before_str) not found among currently selected columns.")
            end
            # Insert before the found column by using the same idx
            target_index = idx
        elseif after_str !== nothing
            idx = findfirst(==(after_str), currently_selected_cols)
            if idx === nothing
                error("The 'after' column $(after_str) not found among currently selected columns.")
            end
            # Insert after the found column by using idx + 1
            target_index = idx + 1
        else
            # Default to the start if neither before nor after is specified
            target_index = 1
        end
        
        filtered_cols = filter(c -> c ∉ to_move_str, currently_selected_cols)
        target_index = min(target_index, length(filtered_cols) + 1)
        new_order = vcat(filtered_cols[1:target_index-1], to_move_str, filtered_cols[target_index:end])
        
        # Re-map new_order back to indices
        name_to_idx = Dict{String,Int}()
        for (i, idx) in enumerate(selected_indices)
            name_to_idx[meta.name[idx]] = idx
        end
        
        new_indices = [name_to_idx[c] for c in new_order]

        # Reorder meta rows
        meta = meta[new_indices, :]
        
        # Rebuild SELECT clause
        qualified_cols = Vector{String}()
        for i in 1:size(meta, 1)
            if meta.current_selxn[i] > 0
                col = meta.name[i]
                push!(qualified_cols, col)
            end
        end

        # Rebuild SELECT clause


        columns_str = join(["SELECT ", join(qualified_cols, ", ")])
        $(esc(sqlquery)).select = columns_str
        $(esc(sqlquery)).metadata = meta

        $(esc(sqlquery))
    end
end
