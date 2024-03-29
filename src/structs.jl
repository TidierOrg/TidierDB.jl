mutable struct CTE
    name::String
    select::String
    from::String
    where::String
    groupBy::String
    having::String
    # Additional fields as necessary

    # Default constructor
    CTE() = new("", "", "", "", "", "")

    # Custom constructor accepting keyword arguments
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
    windowFrame::String  # New field to store window frame specifications
    is_aggregated::Bool
    post_aggregation::Bool
    metadata::DataFrame
    distinct::Bool
    db::Any  # Change the type from SQLite.DB to Any
    ctes::Vector{CTE}
    cte_count::Int

    SQLQuery() = new("", "", "", "", "", "", "", "", false, false, DataFrame(), false, nothing, Vector{CTE}(), 0)

    function SQLQuery(;select::String="", from::String="", where::String="", groupBy::String="", orderBy::String="", having::String="", window_order::String="", windowFrame::String="", is_aggregated::Bool=false, post_aggregation::Bool=false, metadata::DataFrame=DataFrame(), distinct::Bool=false, db::Any=nothing, ctes::Vector{CTE}=Vector{CTE}(), cte_count::Int=0)
        new(select, from, where, groupBy, orderBy, having, window_order, windowFrame, is_aggregated, post_aggregation, metadata, distinct, db, ctes, cte_count)
    end
end

mutable struct InterpolationContext
    variables::Dict{Symbol, Any}
    InterpolationContext() = new(Dict{Symbol, Any}())
end

# Create a global instance of the context, hidden from the module's users.
const GLOBAL_CONTEXT = InterpolationContext()

function add_interp_parameter!(name::Symbol, value::Any)
    GLOBAL_CONTEXT.variables[name] = value
end

