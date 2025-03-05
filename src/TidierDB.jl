module TidierDB

using DataFrames
using MacroTools
using Chain
using Reexport
using DuckDB
using Arrow
using HTTP
using JSON3
using GZip
using Crayons

@reexport using DataFrames: DataFrame
@reexport using Chain
@reexport using DuckDB

 export db_table, set_sql_mode, @arrange, @group_by, @filter, @select, @mutate, @summarize, @summarise, 
 @distinct, @left_join, @right_join, @inner_join, @count, @window_order, @window_frame, @show_query, @collect, @slice_max, 
 @slice_min, @slice_sample, @rename, copy_to, duckdb_open, duckdb_connect, @semi_join, @full_join, @transmute,
 @anti_join, connect, from_query, @interpolate, add_interp_parameter!, update_con,  @head, 
 clickhouse, duckdb, sqlite, mysql, mssql, postgres, athena, snowflake, gbq, oracle, databricks, SQLQuery, show_tables, 
 t, @union, @create_view, drop_view, @compute, warnings, @relocate, @union_all, @setdiff, @intersect, ghseet_connect,
 @unnest_wider, @unnest_longer, dt

 abstract type SQLBackend end

 struct clickhouse <: SQLBackend end
 struct duckdb <: SQLBackend end
 struct sqlite <: SQLBackend end # COV_EXCL_LINE
 struct mysql <: SQLBackend end # COV_EXCL_LINE
 struct mssql <: SQLBackend end # COV_EXCL_LINE
 struct postgres <: SQLBackend end
 struct athena <: SQLBackend end # COV_EXCL_LINE
 struct snowflake <: SQLBackend end
 struct gbq <: SQLBackend end
 struct oracle <: SQLBackend end # COV_EXCL_LINE
 struct databricks <: SQLBackend end
 
 const  _warning_ = Ref(false)
 const window_agg_fxns = [:lead, :lag, :dense_rank, :nth_value, :ntile, :rank_dense, :row_number, :first_value, :last_value, :cume_dist]
 current_sql_mode = Ref{SQLBackend}(duckdb())
 const color = Ref{Bool}(true)
 function set_sql_mode(mode::SQLBackend) current_sql_mode[] = mode end
 

include("docstrings.jl")
include("structs.jl")
include("db_parsing.jl")
include("TBD_macros.jl")
include("mutate_and_summ.jl")
include("parsing_sqlite.jl")
include("parsing_duckdb.jl")
include("parsing_postgres.jl")
include("parsing_mysql.jl")
include("parsing_mssql.jl")
include("parsing_clickhouse.jl")
include("parsing_athena.jl")
include("parsing_gbq.jl")
include("parsing_snowflake.jl")
include("parsing_oracle.jl")
include("parsing_databricks.jl")
include("joins_sq.jl")
include("slices_sq.jl")
include("windows.jl")
include("view_compute.jl")
include("relocate.jl")
include("union_intersect_setdiff.jl")
include("unnest.jl")


# Unified expr_to_sql function to use right mode
function expr_to_sql(expr, sq; from_summarize::Bool = false)
    if current_sql_mode[] == duckdb()
        return expr_to_sql_duckdb(expr, sq, from_summarize=from_summarize)
        # COV_EXCL_START
    elseif current_sql_mode[] == postgres()
        return expr_to_sql_postgres(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == sqlite()
        return expr_to_sql_lite(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == mysql()
        return expr_to_sql_mysql(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == mssql()
        return expr_to_sql_mssql(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == clickhouse()
        return expr_to_sql_clickhouse(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == athena()
        return expr_to_sql_trino(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == gbq()
        return expr_to_sql_gbq(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == oracle()
        return expr_to_sql_oracle(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == snowflake()
        return expr_to_sql_snowflake(expr, sq; from_summarize=from_summarize)
    elseif current_sql_mode[] == databricks()
        return expr_to_sql_duckdb(expr, sq; from_summarize=from_summarize)
    else
        error("Unsupported SQL mode: $(current_sql_mode[])")
    end
end


"""
$docstring_warnings
"""
function warnings(flag::Bool) _warning_[] = flag end
# COV_EXCL_STOP


# DuckDB
function get_table_metadata(conn::Union{DuckDB.DB, DuckDB.Connection}, table_name::String; alias::String="")
    set_sql_mode(duckdb());
    if endswith(table_name, ".geoparquet'")
    query = 
        """
        DESCRIBE SELECT * FROM read_parquet($(table_name)) LIMIT 0
        """
    else
        query = 
        """
        DESCRIBE SELECT * FROM $(table_name) LIMIT 0
        """
    end
    result = DuckDB.execute(conn, query) |> DataFrame
    result[!, :current_selxn] .= 1
    if occursin("*" , table_name) || alias != ""
        if alias != ""
            table_name = alias
        else 
            table_name = "data"
        end
    elseif occursin(r"[:/\\]", table_name)
        table_name = split(basename(table_name), '.')[1]
    elseif occursin(".", table_name)
        table_name = split(basename(table_name), '.')[end]
    else
        table_name = table_name
    end

    table_name = occursin("-", table_name) ? replace(table_name, "-" => "_") : table_name

    result[!, :table_name] .= table_name
    # Adjust the select statement to include the new table_name column
    return select(result, 1 => :name, 2 => :type, :current_selxn, :table_name)
end




# too many backends are tesed here to reasonably include in code coverage
# COV_EXCL_START
"""
$docstring_db_table
"""
function db_table(db, table, athena_params::Any=nothing; iceberg::Bool=false, delta::Bool=false, alias::String="")
    table_name = string(table)
    
    if current_sql_mode[] == postgres() ||current_sql_mode[] ==  duckdb() || current_sql_mode[] ==  mysql() || 
        current_sql_mode[] ==  mssql() || current_sql_mode[] == clickhouse() || current_sql_mode[] == gbq() ||
        current_sql_mode[] == oracle() || current_sql_mode[] == sqlite()
        if iceberg
            DBInterface.execute(db, "INSTALL iceberg;")
            DBInterface.execute(db, "LOAD iceberg;")
            table_name2 = "iceberg_scan('$table_name', allow_moved_paths = true)"
            metadata = get_table_metadata(db, table_name2)
        elseif delta
            DuckDB.execute(db, "INSTALL delta;")
            DuckDB.execute(db, "LOAD delta;")
            table_name2 = "delta_scan('$table_name')"
        elseif occursin("docs.google", table_name) 
            table_name2 = "read_gsheet('$table_name')"
           # println(table_name2)
            alias == "" ? alias = "gsheet" : alias = alias
            metadata = get_table_metadata(db, table_name2, alias = alias)
        elseif startswith(table_name, "read") 
            table_name2 = "$table_name"
            alias = alias == "" ? "data" : alias
           # println(table_name2)
            metadata = get_table_metadata(db, table_name2, alias = alias)
        elseif occursin(r"[:/\\]", table_name) 
            table_name2 = "'$table_name'"
            if current_sql_mode[] == duckdb()
                metadata = get_table_metadata(db, table_name2; alias = alias)
            else
                metadata = get_table_metadata(db, table_name2;)
            end
        else
            metadata = get_table_metadata(db, table_name; alias = alias)
        end
    elseif current_sql_mode[] == athena()
        metadata = get_table_metadata(db, table_name; athena_params)
    elseif current_sql_mode[] == snowflake() || current_sql_mode[] == databricks()
        metadata = get_table_metadata(db, table_name)
    else
        error("Unsupported SQL mode: $(current_sql_mode[])")
    end
    clickhouse_settings =""
    formatted_table_name = if current_sql_mode[] == snowflake()
        "$(db.database).$(db.schema).$table_name"
    elseif db isa DatabricksConnection || current_sql_mode[] == databricks()
        "$(db.database).$(db.schema).$table_name"
    elseif current_sql_mode[] == clickhouse() && occursin(r"[:/]", table_name)
       clickhouse_settings = " SETTINGS enable_url_encoding=0, max_http_get_redirects=10 "
        "url('$table_name')"
    elseif iceberg
        "iceberg_scan('$table_name', allow_moved_paths = true)"
    elseif delta
        "delta_scan('$table_name')"
    elseif occursin(r"[:/\\]", table_name) && !(iceberg || delta) && !startswith(table_name, "read") 
        if occursin(r"\*", table_name) || alias != ""
             alias = alias == "" ? "data" : alias
        else 
            alias = (split(basename(table_name), '.')[1])
        end 
        name = if occursin(".geoparquet", table_name)
             "read_parquet('$table_name') AS $alias "
        else
             "'$table_name' AS $alias "
        end
        formatted_table_name = begin
            parts = split(name, " AS ")
            if length(parts) == 2
                parts[2] = replace(parts[2], "-" => "_")
                join(parts, " AS ")
            else
                name
            end
        end
     elseif startswith(table_name, "read") 
         "$table_name"  
     elseif alias != ""
        table_name * " AS $alias"
     else 
        table_name
    end

    return SQLQuery(from=formatted_table_name, metadata=metadata, db=db, athena_params=athena_params, ch_settings=clickhouse_settings)
end

function db_table(db, table::Vector{String}, athena_params::Any=nothing)
    if isempty(table)
        error("Empty vector of file paths provided")
    end

    # Get file type from the first file

    # Check the current SQL mode
    if current_sql_mode[] == duckdb()
        file_type = lowercase(splitext(first(table))[2])
        
        # Format paths: wrap each in single quotes and join with commas
        formatted_paths = join(map(path -> "'$path'", table), ", ")

        formatted_table_name = if file_type == ".csv"
            "read_csv([$formatted_paths])"
        elseif file_type == ".parquet" || file_type == ".geoparquet" 
            "read_parquet([$formatted_paths])"
        else
            error("Unsupported file type: $file_type")
        end

        # Get metadata from the first file
        meta_vec = first(table)
        metadata = get_table_metadata(db, "'$meta_vec'")

        return SQLQuery(from=formatted_table_name, metadata=metadata, db=db, athena_params=athena_params)

    elseif current_sql_mode[] == clickhouse()

        # Construct the ClickHouse SQL query with UNION ALL for each file
        union_queries = join(map(path -> """
            SELECT *
            FROM url('$path')
        """, table), " UNION ALL ")

        # Wrap the union_queries in a subquery for further processing
        formatted_table_name = "($union_queries)"
        if occursin(r"[:/]", first(table))
            clickhouse_settings = " SETTINGS enable_url_encoding=0, max_http_get_redirects=10 "
        end
        meta_vec = first(table)
        metadata = get_table_metadata(db, "'$meta_vec'")

        return SQLQuery(from=formatted_table_name, metadata=metadata, db=db, athena_params=athena_params, ch_settings = clickhouse_settings)

    else
        error("Unsupported SQL mode: $(current_sql_mode[])")
    end
end

function db_table(db, table::DataFrame, alias::String) 
    DuckDB.register_data_frame(db, table, alias)
    metadata = get_table_metadata(db, alias)
    return SQLQuery(from = alias, metadata=metadata, db=db)
end
const dt = db_table # COV_EXCL_LINE
# COV_EXCL_STOP

"""
$docstring_copy_to
"""
function copy_to(conn, df_or_path::Union{DataFrame, AbstractString}, name::String)
    # Check if the input is a DataFrame
    if isa(df_or_path, DataFrame)
        if current_sql_mode[] == duckdb()
            name_view = name * "view"
            DuckDB.register_data_frame(conn, df_or_path, name_view)
            DBInterface.execute(conn, "CREATE OR REPLACE TABLE $name AS SELECT * FROM $name_view")
            DBInterface.execute(conn, "DROP VIEW $name_view ")
        end
    # COV_EXCL_START
    elseif isa(df_or_path, AbstractString)
        if current_sql_mode[] != duckdb()
            error("Direct file loading is only supported for DuckDB in this implementation.")
        end
        # Determine the file type based on the extension
        if startswith(df_or_path, "http")
            # Install and load the httpfs extension if the path is a URL
            DuckDB.execute(conn, "INSTALL httpfs;")
            DuckDB.execute(conn, "LOAD httpfs;")
        end
        if occursin(r"\.csv$", df_or_path)
            # Construct and execute a SQL command for loading a CSV file
            sql_command = "CREATE TABLE $name AS SELECT * FROM '$df_or_path';"
            DuckDB.execute(conn, sql_command)
        elseif occursin(r"\.parquet$", df_or_path)
            # Construct and execute a SQL command for loading a Parquet file
            sql_command = "CREATE TABLE $name AS SELECT * FROM '$df_or_path';"
            DuckDB.execute(conn, sql_command)
        elseif occursin(r"\.arrow$", df_or_path)
            # Construct and execute a SQL command for loading a CSV file
            arrow_table = Arrow.Table(df_or_path)
            DuckDB.register_table(conn, arrow_table, name)
        elseif occursin(r"\.json$", df_or_path)
            # For Arrow files, read the file into a DataFrame and then insert
            sql_command = "CREATE TABLE $name AS SELECT * FROM read_json('$df_or_path');"
            DuckDB.execute(conn, "INSTALL json;")
            DuckDB.execute(conn, "LOAD json;")
            DuckDB.execute(conn, sql_command)
        elseif startswith(df_or_path, "read")
             DuckDB.execute(conn, "CREATE TABLE $name AS SELECT * FROM $df_or_path;")
        else
            error("Unsupported file type for: $df_or_path")
        end
    else
        error("Unsupported type for df_or_path: Must be DataFrame or file path string.")
    end
end
# COV_EXCL_STOP

"""
$docstring_connect
"""
function connect(::duckdb; kwargs...)
    set_sql_mode(duckdb())
    db = DBInterface.connect(DuckDB.DB, ":memory:")
    DBInterface.execute(db, "SET autoinstall_known_extensions=1;")
    DBInterface.execute(db, "SET autoload_known_extensions=1;")
    
    # Install and load the httpfs extension
    DBInterface.execute(db, "INSTALL httpfs;")
    DBInterface.execute(db, "LOAD httpfs;")
    return db
end

# COV_EXCL_START
function connect(::snowflake, identifier::String, auth_token::String, database::String, schema::String, warehouse::String)
        set_sql_mode(snowflake())
        api_url = "https://$identifier.snowflakecomputing.com/api/v2/statements"
        return SnowflakeConnection(identifier, auth_token, database, schema, warehouse, api_url)
end

function connect(::databricks, identifier::String, auth_token::String, database::String, schema::String, warehouse::String)
        set_sql_mode(databricks())
        identifier = lstrip(identifier, '/')
        api_url = "https://$(identifier).cloud.databricks.com/api/2.0/sql/statements"
        return DatabricksConnection(identifier, auth_token, database, schema, warehouse, api_url)
end

function connect(::duckdb, db_type::Symbol; access_key::String="", secret_key::String="", aws_access_key_id::String="", aws_secret_access_key::String="", aws_region::String="")
    # Connect to the DuckDB database
    db = DBInterface.connect(DuckDB.DB, ":memory:")

    # Enable auto-install and auto-load of known extensions
    DBInterface.execute(db, "SET autoinstall_known_extensions=1;")
    DBInterface.execute(db, "SET autoload_known_extensions=1;")

    # Install and load the httpfs extension
    DBInterface.execute(db, "INSTALL httpfs;")
    DBInterface.execute(db, "LOAD httpfs;")

    if db_type == :gbq
        DuckDB.execute(db, """
        CREATE SECRET (
            TYPE GCS,
            KEY_ID '$access_key',
            SECRET '$secret_key'
        );
        """)
    elseif db_type == :aws
        DBInterface.execute(db, "SET s3_region='$aws_region';")
        DBInterface.execute(db, "SET s3_access_key_id='$aws_access_key_id';")
        DBInterface.execute(db, "SET s3_secret_access_key='$aws_secret_access_key';")
    end

    return db
end

function connect(::duckdb, token::String)
     return DBInterface.connect(DuckDB.DB, token)
end

function ghseet_connect(db)
        DuckDB.query(db, "INSTALL gsheets FROM community; LOAD gsheets;")
        DuckDB.query(db, "CREATE SECRET (TYPE gsheet);")
end
# COV_EXCL_STOP

end
