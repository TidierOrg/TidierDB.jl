# TidierDB supports mutliple join types including equi-joins, nonequi-joins, and as of or rolling joins.

# ## General Syntax 
# All joins share the same argument format
#   - `*_join(query, join_table, joining_keys...)`

# ## Equi Joins
# Equi joins can be written in any of the following ways, and the key column will be dropped from the right hand (new) table to avoid duplication.
#   - `@left_join(table, "table2", key_col)`
#   - `@left_join(table, "table2", key_col = key_col2)`
#  To join mutliple columns, separate the different pairs with a `,`
#   - `@left_join(table, "table2", key_col == key_col2, key2 == key2)`

# ## Inequality Joins
# Inequality joins or non-equi-joins use the same syntax, just with a inequality operators
#   - `@left_join(table, "table2", key_col >= key_col2, key2 < key2)`

# ## AsOf
# To use an AsOf or rolling join, simply wrap the inequality in `closest. Of note, at this time, only one inequality can be supported at a time with AsOf joins
#   - `@left_join(table, "table2", closest(key_col >= key_col2), key2 == key2)`

# When the joining table is already availabe on the database, a string of the table name used as shown above. 
# However, the joining table can also be a TidierDB query, in which case, the query is written as follows
#   - `@left_join(table,query, key)`

# ## Examples
# Examples below will cover how to join tables with different schemas in different databases, 
# and how to write queries on tables and then join them together, and how to do this by levaraging views. Some examples 

using TidierDB
db = connect(duckdb())
mtcars = dt(db, "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv")

# ## Wrangle tables and self join
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

query2 = @chain mtcars @filter(mpg>20) @mutate(mpg = mpg *4); 

@chain query begin
    @left_join(query2, cyl == cyl)
    @summarize(avg_mean = mean(mpg), _by = efficiency)
    @mutate(mean = avg_mean / 4 )
    @collect
end


# ## Different schemas
# To connect to a table in a different schema, prefix it with a dot. For example, "schema_name.table_name".
# In this query, we are also filtering out cars that contain "M" in the name from the `mt2` table before joining. 
# ```julia
# mt2 = dt(db, "ducks_db.mt2")
# other_db = @chain dt(db, "ducks_db.mt2") @filter(!str_detect(car, "M"))
# @chain mtcars begin
#     @left_join(other_db, model == car)
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
# @chain mtcars begin
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

@chain dt(db, "viewer") begin # access the view like any other table
    @left_join(query2, cyl == cyl)
    @summarize(avg_mean = mean(mpg), _by = efficiency)
    @mutate(mean = avg_mean / 4 )
    @collect
end

# ## AsOf/Rolling join
# This example reproduces an example in the [DuckDB Docs](https://duckdb.org/docs/guides/sql_features/asof_join.html#what-is-an-asof-join)
prices = dt(db, "https://duckdb.org/data/prices.csv", "prices");
holdings = dt(db, "https://duckdb.org/data/holdings.csv", "holdings");
@chain holdings begin
    @inner_join(prices, ticker = ticker, closest(when >= when))
    @select(holdings.ticker, holdings.when) 
    @mutate(value = price * shares)
    @collect
 end
