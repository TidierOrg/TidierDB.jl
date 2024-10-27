# In this example, we will reproduce a DuckDB and duckplyr blog post example to demonstrate TidierDB's v0.5.0 capability. 

# The [example by Hannes](https://duckdb.org/2024/10/09/analyzing-open-government-data-with-duckplyr.html) that is being reproduced is exploring Open Data from the New Zealand government that is ~ 1GB.

# ## Set up
# First we will set up the local duckdb database and pull in the metadata for the files. Notice we are not reading this data into memory, only the paths and and column, and table names.
# To follow along, copy the set up code below after downloading the data, but add the directory to the local data.
# ```julia
# import TidierDB as DB
# db = DB.connect(DB.duckdb())

# dir = "/Downloads/nzcensus/"
# data   = dir * "Data8277.csv"
# age    = dir * "DimenLookupAge8277.csv"
# area   = dir * "DimenLookupArea8277.csv"
# ethnic = dir * "DimenLookupEthnic8277.csv"
# sex    = dir * "DimenLookupSex8277.csv"
# year   = dir * "DimenLookupYear8277.csv"

# data = DB.db_table(db, data);
# age = DB.db_table(db, age);
# area = DB.db_table(db, area);
# ethnic = DB.db_table(db, ethnic);
# sex = DB.db_table(db, sex);
# year = DB.db_table(db, year);
# ```
# ## Exploration
# While this long chain could be broken up into multiple smaller chains, lets reproduce the duckplyr code from example and demonstrate how TidierDB also supports multiple joins after filtering, mutating, etc the joining tables. 6 different tables are being joined together through sequential inner joins.
# ```julia
# @chain DB.t(data) begin
#   DB.@filter(str_detect(count, r"^\d+$")) 
#   DB.@mutate(count_ = as_integer(count)) 
#   DB.@filter(count_ > 0) 
#   DB.@inner_join(
#     (@chain DB.t(age) begin 
#     DB.@filter(str_detect(Description, r"^\d+ years$")) 
#     DB.@mutate(age_ = as_integer(str_remove(Code, "years"))) end),
#     Age = Code
#   ) 
#   DB.@inner_join((@chain DB.t(year) DB.@mutate(year_ = Description)), year = Code)
#   DB.@inner_join((@chain DB.t(area) begin
#     DB.@mutate(area_ = Description) 
#     DB.@filter(!str_detect(area_, r"^Total")) 
#   end)
#     , Area = Code) 
#     DB.@inner_join((@chain DB.t(ethnic) begin
#       DB.@mutate(ethnic_ = Description) 
#       DB.@filter(!str_detect( ethnic_, r"^Total",)) end), Ethnic = Code)
#   DB.@inner_join((@chain DB.t(sex) begin
#     DB.@mutate(sex_ = Description) 
#     DB.@filter(!str_detect( sex_, r"^Total")) 
#   end)
#    , Sex = Code)
#   DB.@inner_join((@chain DB.t(year) DB.@mutate(year_ = Description)), Year = Code)
#   @aside DB.@show_query _
#   DB.@create_view(joined_up)
# end;

# @chain DB.db_table(db, "joined_up") begin 
#   DB.@filter begin
#     age_ >= 20
#     age_ <= 40
#     str_detect(area_, r"^Auckland")
#     year_ == "2018"
#     ethnic_ != "European"
#     end
#   DB.@group_by sex_
#   DB.@summarise(group_count = sum(count_))
#   DB.@collect
# end
# ```
# ## Results 
# When we collect this to a local dataframe, we can see that the results match the duckplyr/DuckDB example.
# ```
# 2×2 DataFrame
#  Row │ sex_    group_count 
#      │ String  Int128      
# ─────┼─────────────────────
#    1 │ Female       398556
#    2 │ Male         397326
# ```
