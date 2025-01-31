module TestTidierDB

using TidierDB
using Test
using Documenter

DocMeta.setdocmeta!(TidierDB, :DocTestSetup, :(using TidierDB); recursive=true)

doctest(TidierDB)

end

using TidierData
using TidierStrings
import TidierDB as DB
using Test
using TidierDates

test_df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:99], 
                  groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:100], 
                  value = repeat(1:20, 5), 
                  percent = [i/200 for i in 1:100]);

df2 = DataFrame(
               id2 = [string(Char('A' + i ÷ 26), Char('A' + i % 26)) for i in 0:79],
               category = repeat(["X", "Y", "Z"], inner=27)[1:80], 
               score = [50 + rand(1:50) for i in 1:80])

df3 = DataFrame(id3 = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:89],
                description = ["Desc" * string(i) for i in 1:90],
                value2 = [10 * i for i in 1:90])

db = DB.connect(DB.duckdb());
test_db = DB.db_table(db, test_df, "test_df");
join_db = DB.db_table(db, df2, "df_join");
join_db2 = DB.db_table(db, df3, "df_join2");

@testset "TidierDB to TidierData comparisons" verbose = true begin
   include("comp_tests.jl")
end

#DB.DBInterface.close(db)