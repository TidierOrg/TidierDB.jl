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

test_df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);
 df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
                score = [88, 92, 77, 83, 95, 68, 74]);
db = DB.connect(DB.duckdb());
DB.copy_to(db, test_df, "test_df");
DB.copy_to(db, df2, "df_join");
test_db = DB.db_table(db, "test_df");
join_db = DB.db_table(db, "df_join");

@testset "TidierDB to TidierData comparisons" verbose = true begin
    include("comp_tests.jl")
end