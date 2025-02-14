# TidierDB supports all aggregate functions accross the supported databases, as well as window functions. 

# ## Aggregate Functions
# `@summarize`, by default, supports all aggregate functions built in to a SQL database, with the exception that any `'` that would be used in SQL should be replaced wiht `"`. 
using TidierDB
db = connect(duckdb())
mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
mtcars = db_table(db, mtcars_path);

# ## aggregate function in `@summarize`
# Lets use the DuckDB `kurtosis` aggregate function 
@chain t(mtcars) begin
       @group_by cyl 
       @summarize(kurt = kurtosis(mpg))
       @collect 
 end

## aggregate functions in `@mutate`
# `@mutate`/`@transmute` supports - the following aggregate functions by default: `maximum`, `minimum`, `mean`, `std`, `sum`, `cumsum`
# To use aggregate sql functions that are built in to any database not, but exist outside of the TidierDB parser, simply wrap the function call in `agg()`
@chain t(mtcars) begin 
     @group_by(cyl)
     @mutate(kurt = agg(kurtosis(mpg)))
     @select cyl mpg kurt
     @collect 
end

# Alternatively , if you anticipate regularly using specific aggregate functions, you can use update the underlying parser and drop the need to use `agg`
push!(TidierDB.window_agg_fxns, :kurtosis);
@chain t(mtcars) begin 
     @group_by(cyl)
     @mutate(kurt = kurtosis(mpg))
     @select cyl mpg kurt
     @collect 
end


# ## Window Functions
# TidierDB's `@mutate`/`@transmute` support all of the window functions below
#`lead`, `lag`, `dense_rank`, `nth_value`, `ntile`, `rank_dense`, `row_number`, `first_value`, `last_value`, `cume_dist`
