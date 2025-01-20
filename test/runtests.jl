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

test_df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:199], 
                  groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:200], 
                  value = repeat(1:20, 10), 
                  percent = [i/200 for i in 1:200]);

df2 = DataFrame(
               id2 = [string(Char('A' + i ÷ 26), Char('A' + i % 26)) for i in 0:159],
               category = repeat(["X", "Y", "Z"], inner=54)[1:160], # Ensure length is 160
               score = [50 + rand(1:50) for i in 1:160])

df3 = DataFrame(id3 = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:179],
                description = ["Desc" * string(i) for i in 1:180],
                value2 = [10 * i for i in 1:180])

db = DB.connect(DB.duckdb());
DB.copy_to(db, test_df, "test_df");
DB.copy_to(db, df2, "df_join");
DB.copy_to(db, df3, "df_join2");
test_db = DB.db_table(db, "test_df");
join_db = DB.db_table(db, "df_join");
join_db2 = DB.db_table(db, "df_join2");

@testset "TidierDB to TidierData comparisons" verbose = true begin
   include("comp_tests.jl")
end