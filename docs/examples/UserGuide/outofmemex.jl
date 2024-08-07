# While using the DuckDB backend, TidierDB's lazy intferace enables querying datasets larger than your available RAM. 

# To illustrate this, we will recreate the [Hugging Face x Polars](https://huggingface.co/docs/dataset-viewer/en/polars) example. The final table results are shown below and in this [Hugging Face x DuckDB example](https://huggingface.co/docs/dataset-viewer/en/duckdb)

# First we will load TidierDB and set up a local database.
# ```julia
# using TidierDB
# db = connect(duckdb())
# ```
# To run queries on larger than RAM files, we will set up our `db` as DuckDB outlines [here](https://duckdb.org/2024/07/09/memory-management.html#:~:text=DuckDB%20deals%20with%20these%20scenarios,tries%20to%20minimize%20disk%20spilling.)
# ```julia
# DBinterface.execute(db, "SET memory_limit = '2GB';");
# DuckDB.execute(db, "SET temp_directory = '/tmp/duckdb_swap';");
# DuckDB.execute(db, "SET max_temp_directory_size = '100B';")
# ```

# Executing a query on a large table is slower, so we will copy the tables into this our database. 
# ```julia
# urls = ["https://huggingface.co/datasets/blog_authorship_corpus/resolve/refs%2Fconvert%2Fparquet/blog_authorship_corpus/train/0000.parquet",
#  "https://huggingface.co/datasets/blog_authorship_corpus/resolve/refs%2Fconvert%2Fparquet/blog_authorship_corpus/train/0001.parquet"];
# copy_to(db, urls, "astro");
# ```

# We will also set `stream = true` in `@collect` to stream the results. Now, query the table and collect.
# ```julia
# @chain db_table(db, "astro") begin
#     @group_by(horoscope)
#     @summarise(count = n(), avg_blog_length = mean(length(text)))
#     @arrange(desc(count))
#     @aside @show_query _
#     @collect(stream = true)
# end
# ```
# Placing `@aside @show_query _` before `@collect` above lets us see the SQL query and collect it to a local DataFrame at the same time.
# ```
# SELECT horoscope, COUNT(*) AS count, AVG(length(text)) AS avg_blog_length
#         FROM read_parquet(['https://huggingface.co/datasets/blog_authorship_corpus/resolve/refs%2Fconvert%2Fparquet/blog_authorship_corpus/train/0000.parquet', 'https://huggingface.co/datasets/blog_authorship_corpus/resolve/refs%2Fconvert%2Fparquet/blog_authorship_corpus/train/0001.parquet'])
#         GROUP BY horoscope  
#         ORDER BY avg_blog_length DESC
# 12×3 DataFrame
#  Row │ horoscope    count   avg_blog_length 
#      │ String?      Int64?  Float64?        
# ─────┼──────────────────────────────────────
#    1 │ Aquarius      49568         1125.83
#    2 │ Cancer        63512         1097.96
#    3 │ Libra         60304         1060.61
#    4 │ Capricorn     49402         1059.56
#    5 │ Sagittarius   50431         1057.46
#    6 │ Leo           58010         1049.6
#    7 │ Taurus        61571         1022.69
#    8 │ Gemini        52925         1020.26
#    9 │ Scorpio       56495         1014.03
#   10 │ Pisces        53812         1011.75
#   11 │ Virgo         64629          996.684
#   12 │ Aries         69134          918.081
# ```
