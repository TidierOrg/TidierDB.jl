# TidierDB is unique in its statement parsing flexiblility.  This means that in addition to using any built in SQL database functions, user defined functions (or UDFS) are readily avaialable in TidierDB.  

using TidierDB # DuckDB is reexported by TidierDB
db = connect(duckdb())
mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv";
mtcars = db_tbable(db, mtcars_path);

# ##  DuckDB function chaining
# In DuckDB, functions can be chained together with `.`. TidierDB lets you leverage this. 
@chain t(mtcars) begin 
    @mutate(model2 = model.upper().string_split(" ").list_aggr("string_agg",".").concat("."))
    @select model model2
    @collect
end

# ## `rowid` and pseudocolumns
# When a table is not being read directly from a file, `rowid` is avaialable for use. In general, TidierDB should support all pseudocolumns.
# ```
# copy_to(db, mtcars_path, "mtcars"); # copying table in for demostration purposes 
# @chain db_table(db, :mtcars) begin
#       @filter(rowid == 4)
#       @select(model:hp)
#       @collect
# end
# 1×5 DataFrame
#  Row │ model              mpg       cyl     disp      hp     
#      │ String?            Float64?  Int64?  Float64?  Int64? 
# ─────┼───────────────────────────────────────────────────────
#    1 │ Hornet Sportabout      18.7       8     360.0     175
# ```

# ## UDFs in DuckDB
# TidierDB's flexibility means that once created, UDFs can immediately be used in with `@mutate` or `@transmute`
df = DataFrame(a = [1, 2, 3], b = [1, 2, 3])
dfv = db_table(db, df, "df_view");

# A more in depth disccusion of UDFs in DuckDB.jl can be found [here](https://discourse.julialang.org/t/is-it-hard-to-support-julia-udfs-in-duckdb/118509/24?u=true). 
# define a function 
bino = (a, b) -> (a + b) * (a + b)
# Create the scalar function 
fun = DuckDB.@create_scalar_function bino(a::Int, b::Int)::Int;
DuckDB.register_scalar_function(db, fun);

# Use the UDF in mutate without any further modifcation.
@chain t(dfv) @mutate(c = bino(a, b)) @collect

# Notably, when the function is redefined (with the same arguments), the DuckDB UDF will change as well.
bino = (a, b) -> (a + b) * (a - b);
@chain t(dfv) @mutate(c = bino(a, b)) @collect

# ## UDFs in SQLite 
# ```
# using SQLite
# sql = connect(sqlite());
# df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
#                         groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
#                         value = repeat(1:5, 2), 
#                         percent = 0.1:0.1:1.0);
# 
# copy_to(db, sql, "df_mem");
# SQLite.@register sql function diff_of_squares(x, y)
#               x^2 - y^2
#               end;
# 
# @chain db_table(sql, "df_mem") begin 
#       @select(value, percent)
#       @mutate(plus3 = diff_of_squares(value, percent))
#       @collect
# end
# 10×3 DataFrame
#  Row │ value  percent  plus3   
#      │ Int64  Float64  Float64 
# ─────┼─────────────────────────
#    1 │     1      0.1     0.99
#    2 │     2      0.2     3.96
#    3 │     3      0.3     8.91
#    4 │     4      0.4    15.84
#    5 │     5      0.5    24.75
#    6 │     1      0.6     0.64
#    7 │     2      0.7     3.51
#    8 │     3      0.8     8.36
#    9 │     4      0.9    15.19
#   10 │     5      1.0    24.0
# ```