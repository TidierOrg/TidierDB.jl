# While using TidierDB, you may need to generate part of a query and reuse it multiple times. `from_query()` enables a query portion to be reused multiple times as shown below.

# ```julia
# import TidierDB as DB
# con = DB.connect(:duckdb)
# DB.copy_to(con, "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv", mtcars2)
# ```

# Start a query to analyze fuel efficiency by number of cylinders. However, to further build on this query later, end the chain without using `@show_query` or `@collect`
# ```julia
# query = DB.@chain DB.db_table(con, :mtcars2) begin
#     DB.@group_by cyl
#     DB.@summarize begin
#         across(mpg, (mean, minimum, maximum))
#         num_cars = n()
#         end
#     DB.@mutate begin
#         efficiency = case_when(
#             mean_mpg >= 25, "High",
#             mean_mpg >= 15, "Moderate",
#             "Low" )
#        end
# end;
# ```

# Now, `from_query` will allow you to reuse the query to calculate the average horsepower for each efficiency category
# ```julia
# DB.@chain DB.from_query(query) begin
#    DB.@left_join(mtcars2, cyl, cyl)
#    DB.@group_by(efficiency)
#    DB.@summarize(avg_hp = mean(hp))
#    DB.@collect
# end
# ```
# ```
# 2×2 DataFrame
#  Row │ efficiency  avg_hp   
#      │ String?     Float64? 
# ─────┼──────────────────────
#    1 │ Moderate    180.238
#    2 │ High         82.6364
# ```

# Reuse the query again to find the car with the highest MPG for each cylinder category
# ```julia
# DB.@chain DB.from_query(query) begin
#    DB.@left_join(mtcars2, cyl, cyl)
#    DB.@group_by cyl
#    DB.@slice_max(mpg)
#    DB.@select model cyl mpg
#    DB.@collect 
# end
# ```
# ```
# 3×3 DataFrame
#  Row │ model             cyl     mpg      
#      │ String?           Int64?  Float64? 
# ─────┼────────────────────────────────────
#    1 │ Pontiac Firebird       8      19.2
#    2 │ Toyota Corolla         4      33.9
#    3 │ Hornet 4 Drive         6      21.4
# ```