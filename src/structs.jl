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
    post_unnest::Bool
    post_mutate::Bool
    post_count::Bool
    groupBy_exprs::Bool
    function SQLQuery(; select::String="", from::String="", where::String="", groupBy::String="", orderBy::String="", having::String="", 
        window_order::String="", windowFrame::String="", is_aggregated::Bool=false, post_aggregation::Bool=false, post_join::Bool=false, metadata::DataFrame=DataFrame(), 
        distinct::Bool=false, db::Any=nothing, ctes::Vector{CTE}=Vector{CTE}(), cte_count::Int=0, athena_params::Any=nothing, limit::String="", 
        ch_settings::String="", join_count::Int = 0, post_unnest::Bool = false, post_mutate::Bool = false,  post_count::Bool = false, groupBy_exprs::Bool = false)
        new(select, from, where, groupBy, orderBy, having, window_order, windowFrame, is_aggregated, 
        post_aggregation, post_join, metadata, distinct, db, ctes, cte_count, athena_params, limit, ch_settings, join_count, post_unnest, post_mutate, post_count, groupBy_exprs)
    end
end

function from_query(query::TidierDB.SQLQuery)
    function copy(cte::TidierDB.CTE)
        return TidierDB.CTE(name=cte.name, select=cte.select, from=cte.from, where=cte.where, groupBy=cte.groupBy, having=cte.having)
    end
    
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
        ch_settings = query.ch_settings,
        join_count = query.join_count,
        post_unnest = query.post_unnest,
        post_mutate = query.post_mutate,
        post_count = query.post_count,
        groupBy_exprs = query.groupBy_exprs
    )
    return new_query
end

t(table) = from_query(table)

function up_cte_name(sq, cte_name)
    # Do not retag metadata after JOIN-created CTEs
    if getfield(sq, :post_join)
        return
    end
    if cte_name != "cte_1" && !isempty(strip(String(sq.select)))
        for i in eachindex(sq.metadata.current_selxn)
            if sq.metadata.current_selxn[i] >= 1
                sq.metadata.table_name[i] = cte_name
            end
        end
    end
end



function build_cte!(sq)
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
        sq.post_count = false

    return sq
end
function last_prev_with_prefix(want_prefix::AbstractString, cur_index::Int)
    last::Union{Nothing,String} = nothing
    for (nm, idx) in name_to_index
        if idx < cur_index && !isempty(want_prefix) && startswith(nm, want_prefix)
            if last === nothing || name_to_index[last] < idx
                last = nm
            end
        end
    end
    return last
end
function finalize_ctes(ctes::Vector{CTE})
    if isempty(ctes)
        return ""
    end

    # Index by full name and track names per base "cte_N"
    name_to_index = Dict{String,Int}()
    base_to_names = Dict{String,Vector{String}}()
    for (i, c) in enumerate(ctes)
        name_to_index[c.name] = i
        if (m = match(r"(cte_\d+)$", c.name)) !== nothing
            push!(get!(base_to_names, m.match, String[]), c.name)
        end
    end

    # helpers to parse j-prefix and base
    prefix_of(n::AbstractString) = begin
        s = String(n)
        (m = match(r"^((?:j\d+)+)cte_\d+$", s)) === nothing ? "" : m.captures[1]
    end
    base_of(n::AbstractString) = begin
        s = String(n)
        (m = match(r"(cte_\d+)$", s)) === nothing ? "" : m.match
    end

    # latest previous CTE for a base w/ exact prefix; if none, latest previous with any prefix
    function last_prev_for_base(base::AbstractString, cur_index::Int, want_prefix::AbstractString)
        names = get(base_to_names, String(base), String[])
        exact_last::Union{Nothing,String} = nothing
        any_last::Union{Nothing,String}   = nothing
        for nm in names
            idx = name_to_index[nm]
            if idx < cur_index
                any_last = nm
                if prefix_of(nm) == want_prefix
                    exact_last = nm
                end
            end
        end
        return exact_last === nothing ? any_last : exact_last
    end

    # latest previous CTE for a base with NO prefix (plain cte_N), else nothing
    function last_prev_plain_for_base(base::AbstractString, cur_index::Int)
        names = get(base_to_names, String(base), String[])
        last_plain::Union{Nothing,String} = nothing
        for nm in names
            idx = name_to_index[nm]
            if idx < cur_index && prefix_of(nm) == ""
                last_plain = nm
            end
        end
        return last_plain
    end


    function resolve_token(tok::AbstractString, cur_index::Int, cur_name::AbstractString)::String
        s           = String(tok)
        cur_prefix  = prefix_of(cur_name)
        tok_prefix  = prefix_of(s)
        b           = base_of(s)

        if haskey(name_to_index, s)
            idx = name_to_index[s]

            if idx < cur_index
                # exact, earlier CTE
                if cur_prefix == "" || tok_prefix == cur_prefix || isempty(b)
                    return s
                else
                    lp = last_prev_for_base(b, cur_index, cur_prefix)
                    return lp === nothing ? s : lp
                end

            elseif idx == cur_index
                # self-ref → map to previous with same base (prefer same prefix; else any)
                if isempty(b)
                    return s
                end
                lp = last_prev_for_base(b, cur_index, cur_prefix)
                return lp === nothing ? s : lp

            else
                # FORWARD REF → rewrite to latest previous with same base & current prefix
                if isempty(b)
                    return s
                end
                lp = last_prev_for_base(b, cur_index, cur_prefix)
                if lp !== nothing
                    return lp
                end
                # fallback: latest previous sharing the token's prefix
                if !isempty(tok_prefix)
                    lp2 = last_prev_with_prefix(tok_prefix, cur_index)
                    if lp2 !== nothing
                        return lp2
                    end
                end
                return s
            end

        else
            # not an exact CTE name; if it's a base, rewrite depending on current prefix
            if isempty(b)
                return s
            end
            if cur_prefix == ""
                lp = last_prev_plain_for_base(b, cur_index)
                return lp === nothing ? s : lp
            else
                lp = last_prev_for_base(b, cur_index, cur_prefix)
                return lp === nothing ? s : lp
            end
        end
    end


    # rewrite any CTE token in SQL body (handles FROM/JOIN/ON/etc.)
    function rewrite_sql(sql::AbstractString, cur_index::Int, cur_name::AbstractString)::String
        s = String(sql)
        return replace(s, r"\b((?:j\d+)*cte_\d+)\b" => (t -> resolve_token(t, cur_index, cur_name)))
    end

    cte_strings = String[]
    for (i, c) in enumerate(ctes)
        c.select = rewrite_sql(c.select, i, c.name)
        if !isempty(c.from)
            c.from = resolve_token(c.from, i, c.name)
        end

        cte_str = string(
            c.name, " AS (SELECT ", c.select,
            occursin(" FROM ", c.select) ? "" : " FROM " * c.from,
            (!isempty(c.where) ? " WHERE " * c.where : ""),
            (!isempty(c.groupBy) ? " GROUP BY " * c.groupBy : ""),
            (!isempty(c.having) ? " HAVING " * c.having : ""),
            ")"
        )
        push!(cte_strings, cte_str)
    end

    return "WITH " * join(cte_strings, ", ") * " "
end


function finalize_query(sqlquery::SQLQuery)
    cte_part = finalize_ctes(sqlquery.ctes)

    select_already_present = occursin(r"^SELECT\s+", uppercase(sqlquery.select))
    select_part = if sqlquery.distinct && !select_already_present
        "SELECT DISTINCT " * (isempty(sqlquery.select) ? "*" : sqlquery.select)
    elseif !select_already_present
        "SELECT " * (isempty(sqlquery.select) ? "*" : sqlquery.select)
    else
        sqlquery.select
    end

    # Initialize query_parts with the CTE part
    query_parts = [cte_part]

    # Since sq.from has been updated to reference a CTE, adjust the FROM clause accordingly
    if !isempty(sqlquery.ctes)
        # If CTEs are defined, FROM clause should reference the latest CTE (already updated in sq.from)
        push!(query_parts, select_part, "FROM " * sqlquery.from)
    else
        # If no CTEs are defined, use the original table name in sq.from
        push!(query_parts, select_part, "FROM " * sqlquery.from)
    end

    # Append other clauses if present
    if !isempty(sqlquery.where) push!(query_parts, " " * sqlquery.where) end
    if !isempty(sqlquery.groupBy) push!(query_parts, "" * sqlquery.groupBy) end
    if !isempty(sqlquery.having) push!(query_parts, " " * sqlquery.having) end
    if !isempty(sqlquery.orderBy) push!(query_parts, " " * sqlquery.orderBy) end
    if !isempty(sqlquery.limit) push!(query_parts, " LIMIT " * sqlquery.limit) end
    
    complete_query = join(filter(!isempty, query_parts), " ")

    if !isempty(sqlquery.ch_settings) && current_sql_mode[] == clickhouse()
        complete_query = complete_query * " \n " * string(sqlquery.ch_settings)
    end
    complete_query = replace(complete_query, "&&" => " AND ", "||" => " OR ",
     "FROM )" => ")" ,  "SELECT SELECT " => "SELECT ", "SELECT  SELECT " => "SELECT ", "DISTINCT SELECT " => "DISTINCT ", 
     "SELECT SELECT SELECT " => "SELECT ", "PARTITION BY GROUP BY" => "PARTITION BY", "GROUP BY GROUP BY" => "GROUP BY", "HAVING HAVING" => "HAVING", 
     r"var\"(.*?)\"" => s"\1", r"\"\\\$" => "\"\$",  "WHERE \"" => "WHERE ", "WHERE \"NOT" => "WHERE NOT", "%')\"" =>"%\")", "NULL)\"" => "NULL)",
    "NULL))\"" => "NULL))", r"(?i)INTERVAL(\d+)([a-zA-Z]+)" => s"INTERVAL \1 \2", "SELECT SUMMARIZE " =>  "SUMMARIZE ", "\"(__(" => "(", ")__(\"" => ")"
     , "***\"" => " ", "\"***" => " ", "***" => " ", "WHERE WHERE " => "WHERE ", "WHERE  WHERE " => "WHERE ", "(__(" => "", ")__(" => "", "SELECT , CONCAT_WS" => "SELECT CONCAT_WS")
     complete_query = replace(complete_query, ", AS " => " AS ", "OR  \"" => "OR ", "SELECT all," => "SELECT ")
    if current_sql_mode[] == postgres() || current_sql_mode[] == duckdb() || current_sql_mode[] == mysql() || current_sql_mode[] == mssql() || current_sql_mode[] == clickhouse() || current_sql_mode[] == athena() || current_sql_mode[] == gbq() || current_sql_mode[] == oracle()  || current_sql_mode[] == snowflake() || current_sql_mode[] == databricks()
        complete_query = replace(complete_query, "\"" => "'", "==" => "=")
    end
        complete_query = current_sql_mode[] == postgres() ?  replace(complete_query, r"INTERVAL (\d+) ([a-zA-Z]+)" => s"INTERVAL '\1 \2'") : complete_query
        complete_query = replace(complete_query, r"(?s)(\(SELECT\s+UNNEST.*?FROM\s+.*?\))" => s -> string(s) * ")",  "IS NULL) )'  AND" => "IS NULL) )  AND")
        
    return complete_query
end


