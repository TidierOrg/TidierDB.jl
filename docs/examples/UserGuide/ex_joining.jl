# This page will illustrate how to join different tables in TidierDB. 
# The examples will use the `mtcars` dataset and a synthetic dataset called `mt2` 
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
#     @left_join(t(query2), join_by(cyl == cyl))
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
#     @left_join(t(other_db), join_by(model == car))
#     @select(car, model)
#     @head(5)
#     @collect
# end
# ```
# ```
# 5×2 DataFrame
#  Row │ car                model              
#      │ String             String            
# ─────┼──────────────────────────────────────
#    1 │ Datsun 710         Datsun 710
#    2 │ Hornet 4 Drive     Hornet 4 Drive
#    3 │ Hornet Sportabout  Hornet Sportabout
#    4 │ Valiant            Valiant
#    5 │ Duster 360         Duster 360
# ```

# To join directly to the table, you can use the `@left_join` macro with the table name as a string.
# ```julia
# @chain t(mtcars) begin
#     @left_join("ducks_db.mt2", join_by(model == car))
#     @select(car, model)
#     @head(5)
#     @collect
# end
# ```
# ```
# 5×2 DataFrame
#  Row │ car                model              
#      │ String             String            
# ─────┼──────────────────────────────────────
#    1 │ Mazda RX4          Mazda RX4
#    2 │ Mazda RX4 Wag      Mazda RX4 Wag
#    3 │ Datsun 710         Datsun 710
#    4 │ Hornet 4 Drive     Hornet 4 Drive
#    5 │ Hornet Sportabout  Hornet Sportabout
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
#     @left_join(t(query2), join_by(cyl == cyl))
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
