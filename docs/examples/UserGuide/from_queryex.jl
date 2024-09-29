# While using TidierDB, you may need to generate part of a query and reuse it multiple times. `from_query()` enables a query portion to be reused multiple times as shown below.

# ```julia
# import TidierDB as DB
# con = DB.connect(duckdb())
# mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
# ```

# Start a query to analyze fuel efficiency by number of cylinders. However, to further build on this query later, end the chain without using `@show_query` or `@collect`
# ```julia
# query = DB.@chain DB.db_table(con, mtcars_path) begin
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
#    DB.@left_join("mtcars2", cyl, cyl)
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
#    DB.@left_join("mtcars2", cyl, cyl)
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

# ## Preview or save an intermediate table
# While querying a dataset, you may wish to see an intermediate table, or even save it. You can use `@aside` and `from_query(_)`, illustrated below, to do just that. 
# While we opted to print the results in this simple example below, we could have saved them by using `name = DB.@chain...`

# ```julia
# import ClickHouse;
# conn = conn = DB.connect(DB.clickhouse(); host="localhost", port=19000, database="default", user="default", password="")
# path = "https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet"
# DB.@chain DB.db_table(conn, path) begin
#    DB.@count(cyl)
#    @aside println(DB.@chain DB.from_query(_) DB.@head(5) DB.@collect)
#    DB.@arrange(desc(count))
#    DB.@collect
# end
# ```
# ```
# 5×2 DataFrame
#  Row │ artists  count      
#      │ String?  UInt64 
# ─────┼─────────────────
#    1 │ missing       1
#    2 │ Wizo          3
#    3 │ MAGIC!        3
#    4 │ Macaco        1
#    5 │ SOYOU         1
# 31438×2 DataFrame
#    Row │ artists          count      
#        │ String?          UInt64 
# ───────┼─────────────────────────
#      1 │ The Beatles         279
#      2 │ George Jones        271
#      3 │ Stevie Wonder       236
#      4 │ Linkin Park         224
#      5 │ Ella Fitzgerald     222
#      6 │ Prateek Kuhad       217
#      7 │ Feid                202
#    ⋮   │        ⋮           ⋮
#  31432 │ Leonard               1
#  31433 │ marcos g              1
#  31434 │ BLVKSHP               1
#  31435 │ Memtrix               1
#  31436 │ SOYOU                 1
#  31437 │ Macaco                1
#  31438 │ missing               1
#                31424 rows omitted
# ```