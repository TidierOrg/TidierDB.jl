# TidierDB is unique in its statement parsing flexiblility.  This means that in addition to using any built in SQL database functions, user defined functions (or UDFS) are readily avaialable in TidierDB.  
using TidierDB # DuckDB is reexported by TidierDB
db = connect(duckdb())

# ## UDFs in DuckDB
# Once created, UDFs can immediately be used in with `@mutate` or `@transmute`
df = DataFrame(a = [1, 2, 3], b = [1, 2, 3])
dfv = db_table(db, df, "df_view")

# A more in depth disccusion of UDFs in DuckDB.jl can be found [here](https://discourse.julialang.org/t/is-it-hard-to-support-julia-udfs-in-duckdb/118509/24?u=true). 
# define a function in julia, create the scalar function in DuckDB, and then register it 
bino = (a, b) -> (a + b) * (a + b)
fun = DuckDB.@create_scalar_function bino(a::Int, b::Int)::Int
DuckDB.register_scalar_function(db, fun)
@chain t(dfv) @mutate(c = bino(a, b)) @collect

# Notably, when the function is redefined (with the same arguments) in julia, the DuckDB UDF representation will change as well.
bino = (a, b) -> (a + b) * (a - b)
@chain t(dfv) @mutate(c = bino(a, b)) @collect

# ##  DuckDB function chaining
# In DuckDB, functions can be chained together with `.`. TidierDB lets you leverage this.
mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv";
mtcars = db_table(db, mtcars_path);
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
