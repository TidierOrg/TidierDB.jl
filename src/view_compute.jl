

# for views and replacing
function final_compute(sqlquery::SQLQuery, ::Type{<:duckdb}, sql_cr_or_relace)
    final_query = finalize_query(sqlquery)
    final_query = sql_cr_or_relace * final_query
    return DBInterface.execute(sqlquery.db, final_query)
end

"""
$docstring_create_view
"""
macro create_view(sqlquery, name, replace = false)
    if replace == true 
        sql_cr_or_replace = "CREATE OR REPLACE VIEW $name AS "
    elseif replace == false
        sql_cr_or_replace = "CREATE VIEW  $name AS "
    end
    return quote
        sq = $(esc(sqlquery))
        if current_sql_mode[] == duckdb()
            final_compute($(esc(sqlquery)), duckdb, $sql_cr_or_replace)
        elseif current_sql_mode[] == postgres()
            final_compute($(esc(sqlquery)), postgres, $sql_cr_or_replace)
        elseif current_sql_mode[] == gbq()
            final_compute($(esc(sqlquery)), gbq, $sql_cr_or_replace)
        elseif current_sql_mode[] == mysql()
           final_compute($(esc(sqlquery)), mysql, $sql_cr_or_replace)
        else
            backend = current_sql_mode[]
            print("$backend not yet supported") # COV_EXCL_LINE
        end
    end
end


"""
$docstring_drop_view
"""
function drop_view(db, name)
    DBInterface.execute(db, "DROP VIEW $name")
end


macro create_table(sqlquery, name, args...)
    replace_flag = false
    temp_flag    = false          # new flag

    for arg in args
        if arg === true || arg === false          # legacy positional arg
            replace_flag = arg
        elseif isa(arg, Expr) && arg.head === :(=)
            lhs, rhs = arg.args
            if lhs === :replace
                replace_flag = rhs
            elseif lhs === :temp
                temp_flag = rhs
            else
                error("@create_table: unknown keyword $(lhs)")
            end
        else
            error("@create_table: unsupported argument $(arg)")
        end
    end

    replace_clause = replace_flag ? " OR REPLACE" : ""
    temp_clause    = temp_flag    ? " TEMP"       : ""
    sql_prefix     = "CREATE$(replace_clause)$(temp_clause) TABLE $name AS "

    quote
        backend = current_sql_mode[]
        sq = $(esc(sqlquery))

        if backend == duckdb()
            final_query = finalize_query(sq)
            final_query = $sql_prefix * final_query
            #println(final_query)
            DBInterface.execute(sq.db, final_query)

        elseif backend == postgres()
            final_compute($(esc(sqlquery)), postgres, $sql_prefix)

        elseif backend == gbq()
            final_compute(sq, gbq, $sql_prefix)

        elseif backend == mysql()
            final_compute($(esc(sqlquery)), mysql, $sql_prefix)

        else
            backend = current_sql_mode[]
            print("$(backend) not yet supported")  # COV_EXCL_LINE
        end
    end
end

# COV_EXCL_START
"""
$docstring_write_file
"""
function write_file(sqlquery::SQLQuery, path::String="")
    backend = current_sql_mode[]
    if backend == duckdb()
        final_query = finalize_query(sqlquery)
        final_query = "copy($final_query) to '$path'"
        DBInterface.execute(sqlquery.db, final_query)
    else
        print("$backend not yet supported") # COV_EXCL_LINE
    end
end
# COV_EXCL_END