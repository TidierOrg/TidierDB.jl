# How can functions pass arguments to a TidierDB chain?

# In short, you have to use a macro instead in conjuction with `@interpolate`

# ## Setting up the macro
# To write a macro that will take arguments and pass them to a TidierDB chain, there are 3 steps:
#   1. Write macro with the desired argument(s), and, after the quote, add the chain. Arguments to be changed/interpolated must be prefixed with `!!`
#   2. Use `@interpolate` to make these arguemnts accessible to the chain. `@interpolate` takes touples as argument (one for the `!!`name, and one for the actual content you want the chain to use) 
#   3. Run `@interpolate` and then the chain macro sequentially

# ```
# using TidierDB
# db = connect(duckdb())
# path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
# copy_to(db, path, "mtcars");
#
# # STEP 1
# macro f1(conditions, columns) # The arguemnt names will be names of the `!!` values
#     return quote
#     # add chain here
#       @chain db_table(db, :mtcars) begin
#            @filter(!!conditions > 3)
#            @select(!!columns)
#            @aside @show_query _
#            @collect
#          end # ends the chain
#     end # ends the quote.
# end # ends the macro
# ```
# ```julia
# # STEP 2
# variable = :gear;
# cols = [:model, :mpg, :gear, :wt];
# @interpolate((conditions, variable), (columns, cols));
# @f1(variable, cols)
# ```
# ```
# 17×4 DataFrame
#  Row │ model           mpg       gear    wt       
#      │ String?         Float64?  Int32?  Float64? 
# ─────┼────────────────────────────────────────────
#    1 │ Mazda RX4           21.0       4     2.62
#    2 │ Mazda RX4 Wag       21.0       4     2.875
#    3 │ Datsun 710          22.8       4     2.32
#   ⋮  │       ⋮            ⋮        ⋮        ⋮
#   15 │ Ferrari Dino        19.7       5     2.77
#   16 │ Maserati Bora       15.0       5     3.57
#   17 │ Volvo 142E          21.4       4     2.78
#                                    11 rows omitted
# ```

# Lets say you wanted to filter on new variable with a different name and select new columns, 
# ```julia
# new_condition = :wt;
# new_cols = [:model, :drat]
# @interpolate((conditions, new_condition), (columns, new_cols));
# @f1(new_condition, new_cols)
# ```
# ```
# 20×2 DataFrame
#  Row │ model              drat     
#      │ String?            Float64? 
# ─────┼─────────────────────────────
#    1 │ Hornet 4 Drive         3.08
#    2 │ Hornet Sportabout      3.15
#    3 │ Valiant                2.76
#   ⋮  │         ⋮             ⋮
#   18 │ Pontiac Firebird       3.08
#   19 │ Ford Pantera L         4.22
#   20 │ Maserati Bora          3.54
#                     14 rows omitted
# ```

# You can also interpolate vectors of strings into a `@filter(col in (values))` as well by using the following syntax `@filter(col in [!!values])`

# In short, the first argument in `@interpolate` must be the name of the macro argument it refers to, and the second argument is what you would like to replace it.

# We recognize this adds friction and that it is not ideal, but given the TidierDB macro expressions/string interplay, this is currently the most graceful and functional option available and hopefully a temporary solution to better interpolation that mirrors TidierData.jl.
