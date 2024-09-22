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

# ## UDFs in `@mutate`
# The UDF drops inplace with no further adjustments. Continue below to learn how to create a UDF in DuckDB.jl
# ```
# @chain t(mtcars) begin
#        @mutate(test = diff_of_squares(cyl, hp))
#        @select(test, cyl, hp)
#        @collect
# end
# 32×3 DataFrame
#  Row │ test     cyl    hp    
#      │ Int64    Int64  Int64 
# ─────┼───────────────────────
#    1 │  -12064      6    110
#    2 │  -12064      6    110
#    3 │   -8633      4     93
#    4 │  -12064      6    110
#    5 │  -30561      8    175
#    6 │  -10989      6    105
#    7 │  -59961      8    245
#   ⋮  │    ⋮       ⋮      ⋮
#   27 │   -8265      4     91
#   28 │  -12753      4    113
#   29 │  -69632      8    264
#   30 │  -30589      6    175
#   31 │ -112161      8    335
#   32 │  -11865      4    109
#               19 rows omitted
# ```


# ## How to create UDF in DuckDB.jl
# Once a UDF is regestered in your DuckDB db, you can use it as you would any other SQL function, with no decorators. 
# This next section will walk through defining a function, how to register it and finally, how to use it with TidierDB.
# Of note, if other 

# ## Defining a UDF 
# First, lets define a function that calculates the difference of squares. 
# Input and Output Types:
# - `input::DuckDB.duckdb_data_chunk` is the incoming data chunk (a batch of rows) that DuckDB passes to the function.
# - `output::DuckDB.duckdb_vector` is where the result of the function is written.
# ```
# function DiffOfSquares(info::DuckDB.duckdb_function_info, input::DuckDB.duckdb_data_chunk, output::DuckDB.duckdb_vector)
#        # We first convert the raw input to a DataChunk object using `DuckDB.DataChunk(input, false)`
#     input = DuckDB.DataChunk(input, false)
#        # Determine how many rows (n) are in the chunk using `DuckDB.get_size(input)`.
#     n = DuckDB.get_size(input)
#        # We retrieve the first and second input columns with DuckDB.get_vector() 
#        # And convert them into Julia arrays with DuckDB.get_array. 
#     a_data = DuckDB.get_array(DuckDB.get_vector(input, 1), Int64, n)
#     b_data = DuckDB.get_array(DuckDB.get_vector(input, 2), Int64, n)
#        # create an output array output_data corresponding to the output column
#     output_data = DuckDB.get_array(DuckDB.Vec(output), Int64, n)
#        # loop through each row, perform the desired operation and store the result in output_data[row].
#     for row in 1:n
#         output_data[row] = a_data[row]^2 - b_data[row]^2
#     end
# end;
# ```

# ## Configure the UDF
# Once the function is defined, the next step is to register it in  your DuckDB db. This involves creating a scalar function object, specifying the input/output types, linking the function, and registering it with the database.
# ```
# # Create scalar function object
# f = DuckDB.duckdb_create_scalar_function()
# DuckDB.duckdb_scalar_function_set_name(f, "diff_of_squares")
# ```

# Input parameters are defined with `duckdb_create_logical_type(type)` where type is, for example, `DUCKDB_TYPE_BIGINT` for integers or `DUCKDB_TYPE_VARCHAR` for strings.
# ```
# # Define input parameters as BIGINT
# type = DuckDB.duckdb_create_logical_type(DuckDB.DUCKDB_TYPE_BIGINT)
# DuckDB.duckdb_table_function_add_parameter(f, type)
# DuckDB.duckdb_table_function_add_parameter(f, type)

# # Define return type as BIGINT
# DuckDB.duckdb_scalar_function_set_return_type(f, type)
# DuckDB.duckdb_destroy_logical_type(type)
# ```

# ## Link and Register the Julia Function 
# `@cfunction` is used to convert the Julia function into a callable C function, which DuckDB can invoke.
# ```
# CDiffOfSquares = @cfunction(DiffOfSquares, Cvoid, (DuckDB.duckdb_function_info, DuckDB.duckdb_data_chunk, DuckDB.duckdb_vector))

# # Set the function handler and register
# DuckDB.duckdb_scalar_function_set_function(f, CDiffOfSquares)
# DuckDB.duckdb_register_scalar_function(con.handle, f)
# ```

