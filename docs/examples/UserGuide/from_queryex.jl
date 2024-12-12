# While using TidierDB, you may need to generate part of a query and reuse it multiple times. There are two ways to do this 
# 1. `from_query(query)` or its alias `t(query)`
# 2. `@create_view(name)`

# ## Setup
# ```julia
# import TidierDB as DB
# con = DB.connect(duckdb())
# mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
# mtcars = DB.db_table(con, mtcars_path)
# ```

# Start a query to analyze fuel efficiency by number of cylinders. However, to further build on this query later, end the chain without using `@show_query` or `@collect`
# ```julia
# query = DB.@chain DB.t(mtcars) begin
#     DB.@group_by cyl
#     DB.@summarize begin
#         across(mpg, (mean, minimum, maximum))
#         num_cars = n()
#         end
#     DB.@mutate begin
#         efficiency = case_when(
#             mpg_mean >= 25, "High",
#             mpg_mean >= 15, "Moderate",
#             "Low" )
#        end
# end;
# ```

# ## `from_query()` or `t(query)`
# Now, `from_query`, or `t()` a convienece wrapper, will allow you to reuse the query to calculate the average horsepower for each efficiency category
# ```julia
# DB.@chain DB.t(query) begin
#    DB.@left_join(DB.t(mtcars), cyl = cyl)
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

# ## @create_view
# This can also be done with `@create_view`.
# ```julia
# query2 = @chain t(mtcars) @filter(mpg>20) @mutate(mpg = mpg *4); 
# DB.@chain  DB.db_table(db, "mtcars") begin
#            DB.@group_by cyl
#            DB.@summarize begin
#                across(mpg, (mean, minimum, maximum))
#                num_cars = n()
#                end
#            DB.@mutate begin
#                efficiency = case_when(
#                    mpg_mean >= 25, "High",
#                    mpg_mean >= 15, "Moderate",
#                    "Low" )
#              end
#        DB.@create_view(viewer)
#        end;
#
#
# DB.@chain DB.db_table(db, "viewer") begin
#            DB.@left_join(DB.t(query2), cyl = cyl)
#            DB.@group_by(efficiency)
#            DB.@summarize(avg_mean = mean(mpg))
#            DB.@mutate(mean = avg_mean / 4 )
#            @aside DB.@show_query _
#            DB.@collect
# end
# 2×3 DataFrame
#  Row │ efficiency  avg_mean  mean    
#      │ String      Float64   Float64 
# ─────┼───────────────────────────────
#    1 │ High        106.655   26.6636
#    2 │ Moderate     84.5333  21.1333
# ```

# ## Preview or save an intermediate table
# While querying a dataset, you may wish to see an intermediate table, or even save it. You can use `@aside` and `from_query(_)`, illustrated below, to do just that. 
# While we opted to print the results in this simple example below, we could have saved them by using `name = DB.@chain...`

# ```julia
# import ClickHouse;
# conn = conn = DB.connect(DB.clickhouse(); host="localhost", port=19000, database="default", user="default", password="")
# path = "https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet"
# DB.@chain DB.db_table(conn, path) begin
#    DB.@count(artists)
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