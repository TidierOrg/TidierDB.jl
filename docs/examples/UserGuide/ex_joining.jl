# TidierDB supports mutliple join types including equi-joins, nonequi-joins, and as of or rolling joins.

# ## General Syntax 
# All joins share the same argument format
#   - `*_join(query, join_table, joining_keys...)

# ## Equi Joins
# Equi joins can be written in any of the following ways, and the key column will be dropped from the right hand (new) table to avoid duplication.
#   - `@left_join(DB.t(table), "table2", key_col)`
#   - `@left_join(DB.t(table), "table2", key_col = key_col2)`
#  To join mutliple columns, separate the different pairs with a `,`
#   - `@left_join(DB.t(table), "table2", key_col == key_col2, key2 == key2)`

# ## Inequality Joins
# Inequality joins or non-equi-joins use the same syntax, just with a inequality operators
#   - `@left_join(DB.t(table), "table2", key_col >= key_col2, key2 < key2)`

# ## AsOf
# To use an AsOf or rolling join, simply wrap the inequality in `closest. Of note, at this time, only one inequality can be supported at a time with AsOf joins
#   - `@left_join(DB.t(table), "table2", closest(key_col >= key_col2), key2 == key2)`

# When the joining table is already availabe on the database, a string of the table name used as shown above. 
# However, the joining table can also be a TidierDB query, in which case, the query is written as follows
#   - `@left_join(DB.t(table), DB.t(query), key)`

# ## Examples
# The examples below will use the `mtcars` dataset and a synthetic dataset called `mt2` 
# hosted on a personal MotherDuck instance. Examples will cover how to join 
# tables with different schemas in different databases, and how to write queries on 
# tables and then join them together, and how to do this by levaraging views. 

# ## Setup
# ```julia
# using TidierDB
# db = connect(duckdb(), "md:")
# 
# mtcars = db_table(db, "my_db.mtcars")
# mt2 = db_table(db, "ducks_db.mt2")
# ```
# 
# ## Wrangle tables and self join
# ```julia
# query = @chain t(mtcars) begin
#     @group_by cyl
#     @summarize begin
#         across(mpg, (mean, minimum, maximum))
#         num_cars = n()
#         end
#     @mutate begin
#         efficiency = case_when(
#             mpg_mean >= 25, "High",
#             mpg_mean >= 15, "Moderate",
#             "Low" )
#       end
# end;

# query2 = @chain t(mtcars) @filter(mpg>20) @mutate(mpg = mpg *4); 

# @chain t(query) begin
#     @left_join(t(query2), cyl == cyl)
#     @group_by(efficiency)
#     @summarize(avg_mean = mean(mpg))
#     @mutate(mean = avg_mean / 4 )
#     @aside @show_query _
#     @collect
# end
# ```
# ```
# 2×3 DataFrame
#  Row │ efficiency  avg_mean  mean    
#      │ String      Float64   Float64 
# ─────┼───────────────────────────────
#    1 │ High        106.655   26.6636
#    2 │ Moderate     84.5333  21.1333
# ```

# ## Different schemas
# To connect to a table in a different schema, prefix it with a dot. For example, "schema_name.table_name".
# In this query, we are also filtering out cars that contain "M" in the name from the `mt2` table before joining. 
# ```julia
# other_db = @chain db_table(db, "ducks_db.mt2") @filter(!str_detect(car, "M"))
# @chain t(mtcars) begin
#     @left_join(t(other_db), model == car)
#     @select(model, fuel_efficiency)
#     @head(5)
#     @collect
# end
# ```
# ```
# 5×2 DataFrame
#  Row │ model              fuel_efficiency 
#      │ String             Int64           
# ─────┼────────────────────────────────────
#    1 │ Datsun 710                      24
#    2 │ Hornet 4 Drive                  18
#    3 │ Hornet Sportabout               16
#    4 │ Valiant                         15
#    5 │ Duster 360                      14
# ```

# To join directly to the table, you can use the `@left_join` macro with the table name as a string.
# ```julia
# @chain t(mtcars) begin
#     @left_join("ducks_db.mt2", model == car)
#     @select(model, fuel_efficiency)
#     @head(5)
#     @collect
# end
# ```
# ```
# 5×2 DataFrame
#  Row │ model              fuel_efficiency 
#      │ String             Int64           
# ─────┼────────────────────────────────────
#    1 │ Datsun 710                      24
#    2 │ Hornet 4 Drive                  18
#    3 │ Hornet Sportabout               16
#    4 │ Valiant                         15
#    5 │ Duster 360                      14
# ```

# ## Using a View
# You can also use `@create_view` to create views and then join them. This is an alternate reuse complex queries.
# ```julia
# # notice, this is not begin saved, bc a view is created in the database at the end of the chain
# @chain t(mtcars) begin
#        @group_by cyl
#        @summarize begin
#             across(mpg, (mean, minimum, maximum))
#             num_cars = n()
#         end
#        @mutate begin
#            efficiency = case_when(
#            mpg_mean >= 25, "High",
#            mpg_mean >= 15, "Moderate",
#               "Low" )
#         end
#        #create a view in the database
#        @create_view(viewer)
# end;
#
# # access the view like as if it was any other table
# @chain db_table(db, "viewer") begin 
#     @left_join(t(query2), cyl == cyl)
#     @group_by(efficiency)
#     @summarize(avg_mean = mean(mpg))
#     @mutate(mean = avg_mean / 4 )
#     @collect
# end
# ```
# ```
# 2×3 DataFrame
#  Row │ efficiency  avg_mean  mean    
#      │ String      Float64   Float64 
# ─────┼───────────────────────────────
#    1 │ High        106.655   26.6636
#    2 │ Moderate     84.5333  21.1333
# ```

# ## AsOf/Rolling join
# This example reproduces an example in the (DuckDB Docs)[https://duckdb.org/docs/guides/sql_features/asof_join.html#what-is-an-asof-join]
# ```
# prices = db_table(db, "https://duckdb.org/data/prices.csv", "prices")
# holdings = db_table(db, "https://duckdb.org/data/holdings.csv", "holdings")

# @chain t(holdings) begin
#    @inner_join(t(prices), ticker = ticker, closest(when >= when))
#    @select(holdings.ticker, holdings.when) 
#    @mutate(value = price * shares)
#    @collect
# end
#  4×3 DataFrame
#  Row │ ticker  when                 value   
#      │ String  DateTime             Float64 
# ─────┼──────────────────────────────────────
#    1 │ APPL    2001-01-01T00:00:30     2.94
#    2 │ APPL    2001-01-01T00:01:30    48.26
#    3 │ GOOG    2001-01-01T00:00:30    23.45
#    4 │ GOOG    2001-01-01T00:01:30    21.16
# ```