# TidierDB supports all aggregate functions accross the supported databases, as well as window functions. 

# ## Aggregate Functions
# `@summarize`, by default, supports all aggregate functions built in to a SQL database, with the exception that any `'` that would be used in SQL should be replaced wiht `"`. 
using TidierDB
db = connect(duckdb())
mtcars_path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
mtcars = db_table(db, mtcars_path);


# `@mutate`/`@transmute` supports - the following aggregate functions by default: `maximum`, `minimum`, `mean`, `std`, `sum`, `cumsum`
# However, users can expand this list through two different methods outlineed here.


# ## Window Functions
# TidierDB's `@mutate`/`@transmute` 
# - Window Functions: `lead`, `lag`, `dense_rank`, `nth_value`, `ntile`, `rank_dense`, `row_number`, `first_value`, `last_value`, `cume_dist`
