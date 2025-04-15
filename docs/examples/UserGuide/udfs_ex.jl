# TidierDB is unique in its statement parsing flexiblility.  This means that in addition to using any built in SQL database functions, user defined functions (or UDFS) are readily avaialable in TidierDB.  
using TidierDB # DuckDB is reexported by TidierDB
db = connect(duckdb());
df = DataFrame(a = [1, 2, 3], b = [1, 2, 3]);
dfv = dt(db, df, "df_view");

# ## UDFs in DuckDB
# Once created, UDFs can immediately be used in with `@mutate` or `@transmute`
# A more in depth disccusion of UDFs in DuckDB.jl can be found [here](https://discourse.julialang.org/t/is-it-hard-to-support-julia-udfs-in-duckdb/118509/24?u=true). 
# There are 3 steps 1) Define a function in julia, 2) create the scalar function in DuckDB, and 3) register it 
bino = (a, b) -> (a + b) * (a + b)
fun = DuckDB.@create_scalar_function bino(a::Int, b::Int)::Int
DuckDB.register_scalar_function(db, fun)
@chain dfv @mutate(c = bino(a, b)) @collect

# Notably, when the function is redefined (with the same arguments) in julia, the DuckDB UDF representation will change as well.
bino = (a, b) -> (a + b) * (a - b)
@chain dfv @mutate(c = bino(a, b)) @collect

# ##  DuckDB function chaining
# In DuckDB, functions can be chained together with `.`. TidierDB lets you leverage this.
mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv";
mtcars = dt(db, mtcars_path);
@chain mtcars begin 
    @mutate(model2 = model.upper().string_split(" ").list_aggr("string_agg",".").concat("."))
    @select model model2
    @head() 
    @collect
end

# ## `rowid` and pseudocolumns
# When a table is not being read directly from a file, `rowid` is avaialable for use. In general, TidierDB should support all pseudocolumns.

copy_to(db, mtcars_path, "mtcars"); # copying table in for demostration purposes 
@chain dt(db, "mtcars") begin
      @filter(rowid == 4)
      @select(model:hp)
      @collect
 end
