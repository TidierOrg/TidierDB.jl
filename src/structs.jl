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
    post_first::Bool
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
    function SQLQuery(;post_first = true, select::String="", from::String="", where::String="", groupBy::String="", orderBy::String="", having::String="", 
        window_order::String="", windowFrame::String="", is_aggregated::Bool=false, post_aggregation::Bool=false, post_join::Bool=false, metadata::DataFrame=DataFrame(), 
        distinct::Bool=false, db::Any=nothing, ctes::Vector{CTE}=Vector{CTE}(), cte_count::Int=0, athena_params::Any=nothing, limit::String="", 
        ch_settings::String="", join_count::Int = 0, post_unnest::Bool = false)
        new(post_first, select, from, where, groupBy, orderBy, having, window_order, windowFrame, is_aggregated, 
        post_aggregation, post_join, metadata, distinct, db, ctes, cte_count, athena_params, limit, ch_settings, join_count, post_unnest)
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
        post_first = false
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


    return sq
end

function finalize_ctes(ctes::Vector{CTE})
    if isempty(ctes)
        return ""
    end
  
    cte_strings = String[]
    for cte in ctes
     
        if startswith(cte.name, "jcte_")
            cte.select = replace(cte.select, r"FROM cte_" => "FROM jcte_")
            cte.from = replace(cte.from, r"^cte_" => "jcte_")
        elseif startswith(cte.name, r"j\d+cte")
            match_result = match(r"(j\d+cte_)", cte.name)
            if match_result !== nothing
                replacement = match_result.match
                cte.select = replace(cte.select, r"FROM cte_" => "FROM $replacement")
                cte.from = replace(cte.from, r"^cte_" => replacement)
            end
        end
    
        cte_str = string(
            cte.name, " AS (SELECT ", cte.select, 
            occursin(" FROM ", cte.select) ? "" : " FROM " * cte.from, 
            (!isempty(cte.where) ? " WHERE " * cte.where : ""), 
            (!isempty(cte.groupBy) ? " GROUP BY " * cte.groupBy : ""), 
            (!isempty(cte.having) ? " HAVING " * cte.having : ""), 
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
     , "***\"" => " ", "***" => " ", "WHERE WHERE " => "WHERE ", "WHERE  WHERE " => "WHERE ", "(__(" => "", ")__(" => "", "SELECT , CONCAT_WS" => "SELECT CONCAT_WS")
     complete_query = replace(complete_query, ", AS " => " AS ", "OR  \"" => "OR ")
    if current_sql_mode[] == postgres() || current_sql_mode[] == duckdb() || current_sql_mode[] == mysql() || current_sql_mode[] == mssql() || current_sql_mode[] == clickhouse() || current_sql_mode[] == athena() || current_sql_mode[] == gbq() || current_sql_mode[] == oracle()  || current_sql_mode[] == snowflake() || current_sql_mode[] == databricks()
        complete_query = replace(complete_query, "\"" => "'", "==" => "=")
    end
    
        complete_query = current_sql_mode[] == postgres() ?  replace(complete_query, r"INTERVAL (\d+) ([a-zA-Z]+)" => s"INTERVAL '\1 \2'") : complete_query
        complete_query = replace(complete_query, r"(?s)(\(SELECT\s+UNNEST.*?FROM\s+.*?\))" => s -> string(s) * ")")
        
    return complete_query
end
