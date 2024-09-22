# TidierDB is unique in its statement parsing flexiblility.  This means that using any built in SQL function or user defined functions (or UDFS) or is readily avaialable.  
# To use any function built into a database in `@mutate` or in `@summarize`, simply correctly write the correctly, but replace `'` with `"`. This also applies to any UDF. The example below will illustrate UDFs in the context of DuckDB.


# ```
# # Set up the connection
# using TidierDB  #rexports DuckDB
# db = DuckDB.DB()
# con = DuckDB.connect(db) # this will be important for UDFs
# mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
# mtcars = db_tbable(con, mtcars_path);
# ```
# ## aggregate function in `@summarize`
# Lets use the DuckDB `kurtosis` aggregate function 
# ```
# @chain t(mtcars) begin
#       @group_by cyl 
#       @summarize(kurt = kurtosis(mpg))
#       @collect 
# end
# 3×2 DataFrame
#  Row │ cyl     kurt      
#      │ Int64?  Float64?  
# ─────┼───────────────────
#    1 │      4  -1.43411
#    2 │      6  -1.82944
#    3 │      8   0.330061
# ```

# ## aggregate functions in `@mutate`
# To aggregate sql functions that are builtin to any database, but exist outside of the TidierDB parser, simply wrap the function call in `agg()`
# ```
# @chain t(mtcars) begin 
#     @group_by(cyl)
#     @mutate(kurt = agg(kurtosis(mpg)))
#     @select cyl mpg kurt
#     @collect 
# end

# 32×3 DataFrame
#  Row │ cyl     mpg       kurt      
#      │ Int64?  Float64?  Float64?  
# ─────┼─────────────────────────────
#    1 │      8      18.7   0.330061
#    2 │      8      14.3   0.330061
#    3 │      8      16.4   0.330061
#    4 │      8      17.3   0.330061
#    5 │      8      15.2   0.330061
#    6 │      8      10.4   0.330061
#    7 │      8      10.4   0.330061
#   ⋮  │   ⋮        ⋮          ⋮
#   27 │      6      21.0  -1.82944
#   28 │      6      21.4  -1.82944
#   29 │      6      18.1  -1.82944
#   30 │      6      19.2  -1.82944
#   31 │      6      17.8  -1.82944
#   32 │      6      19.7  -1.82944
#                     19 rows omitted
# end

# ```

# ##  DuckDB function chaining
# In DuckDB, functions can be chained together with `.`. TidierDB lets you leverage this. 
# ```
# @chain t(mtcars) begin 
#     @mutate(model2 = model.upper().string_split(" ").list_aggr("string_agg",".").concat("."))
#     @select model model2
#     @collect
# end
# 32×2 DataFrame
#  Row │ model              model2             
#      │ String?            String?            
# ─────┼───────────────────────────────────────
#    1 │ Mazda RX4          MAZDA.RX4.
#    2 │ Mazda RX4 Wag      MAZDA.RX4.WAG.
#    3 │ Datsun 710         DATSUN.710.
#    4 │ Hornet 4 Drive     HORNET.4.DRIVE.
#    5 │ Hornet Sportabout  HORNET.SPORTABOUT.
#    6 │ Valiant            VALIANT.
#    7 │ Duster 360         DUSTER.360.
#   ⋮  │         ⋮                  ⋮
#   27 │ Porsche 914-2      PORSCHE.914-2.
#   28 │ Lotus Europa       LOTUS.EUROPA.
#   29 │ Ford Pantera L     FORD.PANTERA.L.
#   30 │ Ferrari Dino       FERRARI.DINO.
#   31 │ Maserati Bora      MASERATI.BORA.
#   32 │ Volvo 142E         VOLVO.142E.
#                               19 rows omitted
# ```

# ## `rowid` and pseudocolumns
# When a table is not being read directly from a file, `rowid` is avaialable for use. In general, TidierDB should support all pseudocolumns.
# ```
# copy_to(db, mtcars_path, "mtcars"); # copying table in for demostration purposes 
# @chain db_table(con, :mtcars) begin
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

# ## UDF SQLite Example
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

# ## How to create UDF in DuckDB
# Example coming soon..