# TidierDB supports all aggregate functions accross the supported databases, as well as window functions. 

# ## Aggregate Functions
# `@summarize`, by default, supports all aggregate functions built into a SQL database, with the exception that any `'` that would be used in SQL should be replaced wiht `"`. 
using TidierDB
db = connect(duckdb());
mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv";
mtcars = db_table(db, mtcars_path);

# ## Aggregate Functions in `@summarize`
# Lets use the DuckDB `kurtosis` aggregate function 
@chain t(mtcars) begin
       @group_by cyl 
       @summarize(kurt = kurtosis(mpg))
       @collect 
 end

## Aggregate Functions in `@mutate`
# By default, `@mutate`/`@transmute` supports (however, you can easily expand this list)
# - `maximum`, `minimum`, `mean`, `std`, `sum`, `cumsum`
# To use aggregate sql functions that are built in to any database backend, but exist outside of the TidierDB parser list above, simply wrap the function call in `agg()`
@chain t(mtcars) begin 
     @group_by(cyl)
     @mutate(kurt = agg(kurtosis(mpg)))
     @select cyl mpg kurt
     @head()
     @collect 
end

# Alternatively , if you anticipate regularly using specific aggregate functions, you can update the underlying parser avoid using `agg` all together 
push!(TidierDB.window_agg_fxns, :kurtosis);
@chain t(mtcars) begin 
     @group_by(cyl)
     @mutate(kurt = kurtosis(mpg))
     @select cyl mpg kurt
     @head()
     @collect 
end

# ## Window Functions
# TidierDB's `@mutate`/`@transmute` support all of the window functions below
# - `lead`, `lag`, `dense_rank`, `nth_value`, `ntile`, `rank_dense`, `row_number`, `first_value`, `last_value`, `cume_dist`
# 
# When ordering a window function, `@arrange` should _not_ be used. Rather, use `@window_order` or, preferably, `_order` and `_frame` in `@mutate`.

@chain t(mtcars) begin
    @mutate(row_id = row_number(), 
        _by = cyl, 
        _order = mpg # _frame is not used in this example 
        )
    @collect
end 

# The above query could have alternatively been written as 
@chain t(mtcars) begin
    @group_by cyl
    @window_order mpg
    @mutate(row_id = row_number())
    @collect
end 
