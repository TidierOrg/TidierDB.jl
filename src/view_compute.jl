

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


macro compute(sqlquery, name, replace = false)
    
    if replace == true 
        sql_cr_or_replace = "CREATE OR REPLACE Table $name AS "
    elseif replace == false
         sql_cr_or_replace = "CREATE Table $name AS "
    end
    return quote
       # prin
        backend = current_sql_mode[]
        sq = $(esc(sqlquery))
        if backend == duckdb()
            final_query = finalize_query(sq)
            final_query = $sql_cr_or_replace  * final_query
            DBInterface.execute(sq.db, final_query)
        elseif  backend == postgres()
            final_compute($(esc(sqlquery)), postgres, $sql_cr_or_replace)
        elseif backend == gbq()
            final_compute(sq, gbq, $sql_cr_or_replace)
        elseif current_sql_mode[] == mysql()
            final_compute($(esc(sqlquery)), mysql, $sql_cr_or_replace)
         else
             backend = current_sql_mode[]
             print("$backend not yet supported") # COV_EXCL_LINE
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