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