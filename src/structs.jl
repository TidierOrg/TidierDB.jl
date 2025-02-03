mutable struct CTE
    name::String
    select::String
    from::String
    where::String
    groupBy::String
    having::String
    function CTE(;name::String="", select::String="", from::String="", where::String="", groupBy::String="", having::String="")
        new(name, select, from, where, groupBy, having)
    end
end

mutable struct SQLQuery
    select::String
    from::String
    where::String
    groupBy::String
    orderBy::String
    having::String
    window_order::String
    windowFrame::String 
    is_aggregated::Bool
    post_aggregation::Bool
    post_join::Bool
    metadata::DataFrame
    distinct::Bool
    db::Any 
    ctes::Vector{CTE}
    cte_count::Int
    athena_params::Any    
    limit::String
    ch_settings::String
    join_count::Int

    function SQLQuery(;select::String="", from::String="", where::String="", groupBy::String="", orderBy::String="", having::String="", 
        window_order::String="", windowFrame::String="", is_aggregated::Bool=false, post_aggregation::Bool=false, post_join::Bool=false, metadata::DataFrame=DataFrame(), 
        distinct::Bool=false, db::Any=nothing, ctes::Vector{CTE}=Vector{CTE}(), cte_count::Int=0, athena_params::Any=nothing, limit::String="", 
        ch_settings::String="", join_count::Int = 0)
        new(select, from, where, groupBy, orderBy, having, window_order, windowFrame, is_aggregated, 
        post_aggregation, post_join, metadata, distinct, db, ctes, cte_count, athena_params, limit, ch_settings, join_count)
    end
end

function from_query(query::TidierDB.SQLQuery)
    # Custom copy method for TidierDB.CTE
    function copy(cte::TidierDB.CTE)
        return TidierDB.CTE(name=cte.name, select=cte.select, from=cte.from, where=cte.where, groupBy=cte.groupBy, having=cte.having)
    end
    
    # Create a new SQLQuery object with the same field values
    new_query = TidierDB.SQLQuery(
        select=query.select,
        from=query.from,
        where=query.where,
        groupBy=query.groupBy,
        orderBy=query.orderBy,
        having=query.having,
        window_order=query.window_order,
        windowFrame=query.windowFrame,
        is_aggregated=query.is_aggregated,
        post_aggregation=query.post_aggregation,
        post_join=query.post_join,

        metadata=deepcopy(query.metadata), 
        distinct=query.distinct,
        db=query.db,
        ctes=[copy(cte) for cte in query.ctes],  
        cte_count=query.cte_count,
        athena_params = query.athena_params,
        limit = query.limit,
        ch_settings = query.ch_settings
    )
    return new_query
end

t(table) = from_query(table)

function up_cte_name(sq, cte_name)
    if cte_name != "cte_1"
        if all(x -> x >= 1, sq.metadata.current_selxn)
            sq.metadata.table_name .= cte_name
        end
    end
end

function process_sq!(sq)
    # or if you want to also check sq.post_join, add it here
    cte_name = "cte_" * string(sq.cte_count + 1)

        sq.post_aggregation = false

        # Use provided select expression, or "*" if none was specified.
        select_expressions = !isempty(sq.select) ? [sq.select] : ["*"]

        # Build the SQL for the new CTE.
        cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
        if sq.is_aggregated && !isempty(sq.groupBy)
            cte_sql *= " " * sq.groupBy
            sq.groupBy = ""
        end
        if !isempty(sq.where)
            cte_sql *= " WHERE " * sq.where
            sq.where = ""
        end
        if !isempty(sq.having)
            cte_sql *= "  " * sq.having
            sq.having = ""
        end

        # Define the new CTE name.
        cte_name = "cte_" * string(sq.cte_count + 1)

        # Create the new CTE and update sq.
        new_cte = CTE(name=cte_name, select=cte_sql)
        up_cte_name(sq, cte_name)
        push!(sq.ctes, new_cte)
        sq.cte_count += 1
        sq.from = cte_name


    return sq
end
