# While using TidierDB, you may need to generate part of a query and reuse it multiple times. There are two ways to do this 
# 1. `from_query(query)` or its alias `t(query)`
# 2. `@create_view(name)`

# ## Setup
using TidierDB
con = connect(duckdb());
mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv";
mtcars = dt(con, mtcars_path);

# Start a query to analyze fuel efficiency by number of cylinders. However, to further build on this query later, end the chain without using `@show_query` or `@collect`
query = @chain mtcars begin
    @group_by cyl
    @summarize begin
        across(mpg, (mean, minimum, maximum))
        num_cars = n()
        end
    @mutate begin
        efficiency = case_when(
            mpg_mean >= 25, "High",
            mpg_mean >= 15, "Moderate",
            "Low" )
    end
end;

# ## `from_query()` or `t(query)`
# `from_query`, or `t()` a convienece wrapper, will allow you to reuse the query to calculate the average horsepower for each efficiency category
@chain query begin
   @left_join(mtcars, cyl)
   @group_by(efficiency)
   @summarize(avg_hp = mean(hp))
   @collect
end

# ## @create_view
# Queries can also be reused as views. 
query2 = @chain mtcars @filter(mpg>20) @mutate(mpg = mpg *4); 
@chain mtcars begin
    @group_by cyl
    @summarize begin
        across(mpg, (mean, minimum, maximum))
        num_cars = n()
        end
    @mutate begin
        efficiency = case_when(
            mpg_mean >= 25, "High",
            mpg_mean >= 15, "Moderate",
            "Low" )
        end
    @create_view(viewer)
end;

@chain dt(con, "viewer") begin
    @left_join(query2, cyl == cyl)
    @summarize(avg_mean = mean(mpg), _by = efficiency)
    @mutate(mean = avg_mean / 4 )
    @collect
end

# ## Preview or save an intermediate table
# While querying a dataset, you may wish to see an intermediate table, or even save it. You can use `@aside` and `from_query(_)`, illustrated below, to do just that. 
# While we opted to print the results in this simple example below, we could have saved them by using `name = @chain...`

# ```julia
# import ClickHouse;
# conn = conn = connect(clickhouse(); host="localhost", port=19000, database="default", user="default", password="")
# path = "https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet"
# @chain dt(conn, path) begin
#    @count(artists)
#    @aside println(@chain from_query(_) @head(5) @collect)
#    @arrange(desc(count))
#    @collect
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