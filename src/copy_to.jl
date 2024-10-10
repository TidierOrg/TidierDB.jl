import MySQL
import CSV
import DBInterface
using DataFrames
using Chain

function collapse(x, sep = ",", left = "", right = "")
    left * reduce((a, b) -> a * sep * b, x) * right
end

collapse_sql(x) = collapse(string.(x), "`,`", "(`", "`)")

function write_special_csv(df; delim = "|!|", newline = "|#|", header = false, filename = "temp.csv")
    CSV.write(filename, df, delim = delim, newline = newline, header = header, missingstring="\\N")
end


"""
    copy_to2(df, table_name, con; replace = false)

Escreve no mariadb!!!

# Arguments
- df
- table_name
- con
- `replace`: should it replace on duplicate keys?
"""
function copy_to2(df::DataFrames.AbstractDataFrame, table_name, con::MySQL.Connection; replace = false)
    db_names = names(con, table_name)
    df_names = names(df)
    common_names = intersect(db_names, df_names)

    if length(common_names) == 0 
        @warn "No columns in common! Returning nothing"
        return nothing
    end

    temp_file = tempname()

    df2 = DataFrames.select(df, common_names)

    write_special_csv(df2, filename = temp_file)

    comando = replace ? "REPLACE" : "IGNORE"


    query = """
LOAD DATA LOCAL INFILE '$(temp_file)' $comando INTO TABLE `$table_name`
CHARACTER SET 'utf8'
COLUMNS TERMINATED BY '|!|'
LINES TERMINATED BY '|#|'
$(collapse_sql(common_names));
"""

    output = DBInterface.execute(con, query)    

    Base.Filesystem.rm(temp_file)

    output
end

function Main.names(con::MySQL.Connection, table_name)
    query_columns = """SHOW COLUMNS FROM `$(table_name)`"""

    colunas_db =
        @chain begin
        DBInterface.execute(con, query_columns)
        DataFrame
        _.Field
        end

    colunas_db
end