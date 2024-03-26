module TidierDB

using LibPQ
using DataFrames
using MacroTools
using Chain
using SQLite
using Reexport
using DuckDB

@reexport using DataFrames: DataFrame
@reexport using Chain
@reexport using SQLite: DB, load!


 export start_query_meta, set_sql_mode, @arrange, @group_by, @filter, @select, @mutate, @summarize, @summarise, 
 @distinct, @left_join, @right_join, @inner_join, @count, @window_order, @window_frame, @show_query, @collect, @slice_max, 
 @slice_min, @slice_sample

include("docstrings.jl")
include("structs.jl")
include("db_parsing.jl")
include("TBD_macros.jl")
include("postgresparsing.jl")
include("joins_sq.jl")
include("slices_sq.jl")




current_sql_mode = Ref(:lite)

# Function to switch modes
function set_sql_mode(mode::Symbol)
    current_sql_mode[] = mode
end

# Unified expr_to_sql function to use right mode
function expr_to_sql(expr, sq; from_summarize::Bool = false)
    if current_sql_mode[] == :lite
        return expr_to_sql_lite(expr, sq, from_summarize=from_summarize)
    elseif current_sql_mode[] == :postgres
        # If similar logic is needed for Postgres, adjust accordingly.
        # Make sure the expr_to_sql_postgres function also accepts from_summarize as a keyword argument.
        return expr_to_sql_postgres(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == :duckdb
        # If similar logic is needed for DuckDB, adjust accordingly.
        # Make sure the expr_to_sql_duckdb function also accepts from_summarize as a keyword argument.
        return expr_to_sql_postgres(expr, sq; from_summarize=from_summarize)
    else
        error("Unsupported SQL mode: $(current_sql_mode[])")
    end
end




function get_table_metadata(db::SQLite.DB, table_name::String)
    query = "PRAGMA table_info($table_name);"
    result = SQLite.DBInterface.execute(db, query) |> DataFrame
    result[!, :current_selxn] .= 1
    resize!(result.current_selxn, nrow(result))
    return select(result, 2 => :name, 3 => :type, :current_selxn)
end

function start_query_meta(db::SQLite.DB, table::Symbol)
    metadata = get_table_metadata(db, string(table))
    return SQLQuery(from=string(table), metadata=metadata, db=db)  # Pass db to the constructor
end

function finalize_ctes(ctes::Vector{CTE})
    if isempty(ctes)
        return ""
    end

    cte_strings = String[]
    for cte in ctes
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

    complete_query = join(filter(!isempty, query_parts), " ")
    complete_query = replace(complete_query, "&&" => " AND ", "||" => " OR ",
     "FROM )" => ")" ,  "SELECT SELECT " => "SELECT ", "SELECT  SELECT " => "SELECT ", "DISTINCT SELECT " => "DISTINCT ", 
     "SELECT SELECT SELECT " => "SELECT ", "PARTITION BY GROUP BY" => "PARTITION BY", "GROUP BY GROUP BY" => "GROUP BY", "HAVING HAVING" => "HAVING", )

    if current_sql_mode[] == :postgres || current_sql_mode[] == :duckdb
        complete_query = replace(complete_query, "\"" => "'", "==" => "=")
    end

    return complete_query
end


function get_table_metadata(conn::LibPQ.Connection, table_name::String)
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '$table_name'
    ORDER BY ordinal_position;
    """
    result = LibPQ.execute(conn, query) |> DataFrame
    result[!, :current_selxn] .= 1
    return select(result, 1 => :name, 2 => :type, :current_selxn)
end


# Database-agnostic start_query_meta function
function start_query_meta(db, table::Symbol)
    table_name = string(table)
    metadata = if current_sql_mode[] == :lite
        get_table_metadata(db, table_name)
    elseif current_sql_mode[] == :postgres 
        get_table_metadata(db, table_name)
    elseif current_sql_mode[] == :duckdb 
        get_table_metadata(db, table_name)
    else
        error("Unsupported SQL mode: $(current_sql_mode[])")
    end
    return SQLQuery(from=table_name, metadata=metadata, db=db)
end

#end
function get_table_metadata(conn::DuckDB.Connection, table_name::String)
    query = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '$table_name'
    ORDER BY ordinal_position;
    """
    result = DuckDB.execute(conn, query) |> DataFrame
    result[!, :current_selxn] .= 1
    return select(result, 1 => :name, 2 => :type, :current_selxn)
end

function start_query_meta(db, table::Symbol)
    table_name = string(table)
    metadata = if current_sql_mode[] == :lite
        get_table_metadata(db, table_name)
    elseif current_sql_mode[] == :postgres || current_sql_mode[] == :duckdb
        get_table_metadata(db, table_name)
    else
        error("Unsupported SQL mode: $(current_sql_mode[])")
    end
    return SQLQuery(from=table_name, metadata=metadata, db=db)
end


function copy_to(conn, df::DataFrame, name::String)
    if current_sql_mode[] == :duckdb
        DuckDB.register_data_frame(conn, df, name)
    elseif current_sql_mode[] == :lite
        SQLite.load!(df, conn, name)
    else
        error("Unsupported SQL mode: $set_sql_mode")
    end
end
end
