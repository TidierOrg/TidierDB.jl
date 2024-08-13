# # Comparing TidierDB vs Ibis

# TidierDB is a reimplementation of dbplyr from R, so the syntax is remarkably similar. But how does TidierDB compare to Python's Ibis? 
# This page will [perform a similar comparison to the Ibis Documentation comparing Ibis and dplyr](https://ibis-project.org/tutorials/ibis-for-dplyr-users) 

# ## Set up
# Ibis
# ```python
# import ibis
# import ibis.selectors as s # allows for different styles of column selection
# from ibis import _ # eliminates need to type table name before each column vs typing cols as strings
# ibis.options.interactive = True # automatically collects first 10 rows of table
# ```
# TidierDB
# ```julia
# using TidierDB
# db = connect(duckdb())
# # This next line is optional, but it will let us avoid writing `db_table` or `from_query` for each query
# t(table) = from_query(table)
# ```
# Of note, TidierDB does not yet have an "interactive mode" so each example result will be collected.

# ## Loading Data
# With Ibis, there are specific functions to read in different file types
# ```python
# mtcars = ibis.read_csv("https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv")
# ```
# In TidierDB, there is only `db_table`, which determines the file type and generates the syntax appropriate for the backend in use.
# ```julia
# mtcars = db_table(db, "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv");
# ```
# ## Previewing the data
# TidierDB and Ibis use `head`/`@head` to preview the first rows of a dataset.

# Ibis
# ```python
# mtcars.head(6)
# ```
# ```
# ┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┓
# ┃ model             ┃ mpg     ┃ cyl   ┃ disp    ┃ hp    ┃ drat    ┃ wt      ┃ qsec    ┃ vs    ┃ am    ┃ gear  ┃ carb  ┃
# ┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━┩
# │ string            │ float64 │ int64 │ float64 │ int64 │ float64 │ float64 │ float64 │ int64 │ int64 │ int64 │ int64 │
# ├───────────────────┼─────────┼───────┼─────────┼───────┼─────────┼─────────┼─────────┼───────┼───────┼───────┼───────┤
# │ Mazda RX4         │    21.0 │     6 │   160.0 │   110 │    3.90 │   2.620 │   16.46 │     0 │     1 │     4 │     4 │
# │ Mazda RX4 Wag     │    21.0 │     6 │   160.0 │   110 │    3.90 │   2.875 │   17.02 │     0 │     1 │     4 │     4 │
# │ Datsun 710        │    22.8 │     4 │   108.0 │    93 │    3.85 │   2.320 │   18.61 │     1 │     1 │     4 │     1 │
# │ Hornet 4 Drive    │    21.4 │     6 │   258.0 │   110 │    3.08 │   3.215 │   19.44 │     1 │     0 │     3 │     1 │
# │ Hornet Sportabout │    18.7 │     8 │   360.0 │   175 │    3.15 │   3.440 │   17.02 │     0 │     0 │     3 │     2 │
# │ Valiant           │    18.1 │     6 │   225.0 │   105 │    2.76 │   3.460 │   20.22 │     1 │     0 │     3 │     1 │
# └───────────────────┴─────────┴───────┴─────────┴───────┴─────────┴─────────┴─────────┴───────┴───────┴───────┴───────┘
# ```
# TidierDB
# ```julia
# @chain t(mtcars) @head(6) @collect
# ```
# ```
# 6×12 DataFrame
#  Row │ model              mpg       cyl     disp      hp      drat      wt        qsec      vs      am      gear    carb   
#      │ String?            Float64?  Int64?  Float64?  Int64?  Float64?  Float64?  Float64?  Int64?  Int64?  Int64?  Int64? 
# ─────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │ Mazda RX4              21.0       6     160.0     110      3.9      2.62      16.46       0       1       4       4
#    2 │ Mazda RX4 Wag          21.0       6     160.0     110      3.9      2.875     17.02       0       1       4       4
#    3 │ Datsun 710             22.8       4     108.0      93      3.85     2.32      18.61       1       1       4       1
#    4 │ Hornet 4 Drive         21.4       6     258.0     110      3.08     3.215     19.44       1       0       3       1
#    5 │ Hornet Sportabout      18.7       8     360.0     175      3.15     3.44      17.02       0       0       3       2
#    6 │ Valiant                18.1       6     225.0     105      2.76     3.46      20.22       1       0       3       1
# ```

# ## Filtering
# The example below demonstrates how to filter using multiple criteria in both Ibis and TidierData
# Ibis
# ```python
# mtcars.filter(((_.mpg > 22) & (_.drat > 4) | (_.hp == 113)))
# ```
# ```
# ┏━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┓
# ┃ model          ┃ mpg     ┃ cyl   ┃ disp    ┃ hp    ┃ drat    ┃ wt      ┃ qsec    ┃ vs    ┃ am    ┃ gear  ┃ carb  ┃
# ┡━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━┩
# │ string         │ float64 │ int64 │ float64 │ int64 │ float64 │ float64 │ float64 │ int64 │ int64 │ int64 │ int64 │
# ├────────────────┼─────────┼───────┼─────────┼───────┼─────────┼─────────┼─────────┼───────┼───────┼───────┼───────┤
# │ Lotus Europa   │    30.4 │     4 │    95.1 │   113 │    3.77 │   1.513 │   16.90 │     1 │     1 │     5 │     2 │
# │ Fiat 128       │    32.4 │     4 │    78.7 │    66 │    4.08 │   2.200 │   19.47 │     1 │     1 │     4 │     1 │
# │ Honda Civic    │    30.4 │     4 │    75.7 │    52 │    4.93 │   1.615 │   18.52 │     1 │     1 │     4 │     2 │
# │ Toyota Corolla │    33.9 │     4 │    71.1 │    65 │    4.22 │   1.835 │   19.90 │     1 │     1 │     4 │     1 │
# │ Fiat X1-9      │    27.3 │     4 │    79.0 │    66 │    4.08 │   1.935 │   18.90 │     1 │     1 │     4 │     1 │
# │ Porsche 914-2  │    26.0 │     4 │   120.3 │    91 │    4.43 │   2.140 │   16.70 │     0 │     1 │     5 │     2 │
# └────────────────┴─────────┴───────┴─────────┴───────┴─────────┴─────────┴─────────┴───────┴───────┴───────┴───────┘
# ```
# TidierDB
# ```julia
# @chain t(mtcars) begin
#        @filter((mpg > 22 && drat > 4) || hp == 113)
#        @collect
# end
# ```
# ```
# 6×12 DataFrame
#  Row │ model           mpg       cyl     disp      hp      drat      wt        qsec      vs      am      gear    carb   
#      │ String?         Float64?  Int64?  Float64?  Int64?  Float64?  Float64?  Float64?  Int64?  Int64?  Int64?  Int64? 
# ─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │ Lotus Europa        30.4       4      95.1     113      3.77     1.513     16.9        1       1       5       2
#    2 │ Fiat 128            32.4       4      78.7      66      4.08     2.2       19.47       1       1       4       1
#    3 │ Honda Civic         30.4       4      75.7      52      4.93     1.615     18.52       1       1       4       2
#    4 │ Toyota Corolla      33.9       4      71.1      65      4.22     1.835     19.9        1       1       4       1
#    5 │ Fiat X1-9           27.3       4      79.0      66      4.08     1.935     18.9        1       1       4       1
#    6 │ Porsche 914-2       26.0       4     120.3      91      4.43     2.14      16.7        0       1       5       2
# ```

# ## Creating new columns
# Both TidierDB and Ibis use `mutate`/`@mutate` to add new columns

# Ibis
# ```python
# (
#    mtcars
#         .mutate(kpg = _.mpg * 1.61)
#         .select("model", "kpg")
# )
# ```
# ```
# ┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
# ┃ model             ┃ kpg     ┃
# ┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
# │ string            │ float64 │
# ├───────────────────┼─────────┤
# │ Mazda RX4         │  33.810 │
# │ Mazda RX4 Wag     │  33.810 │
# │ Datsun 710        │  36.708 │
# │ Hornet 4 Drive    │  34.454 │
# │ Hornet Sportabout │  30.107 │
# │ Valiant           │  29.141 │
# │ Duster 360        │  23.023 │
# │ Merc 240D         │  39.284 │
# │ Merc 230          │  36.708 │
# │ Merc 280          │  30.912 │
# │ …                 │       … │
# └───────────────────┴─────────┘
# ```
# TidierDB
# ```julia
# @chain t(mtcars) begin 
#        @mutate(kpg = mpg * 1.61)
#        @select(model, kpg)
#        @collect
# end
# ```
# ```
# 32×2 DataFrame
#  Row │ model              kpg      
#      │ String?            Float64? 
# ─────┼─────────────────────────────
#    1 │ Mazda RX4            33.81
#    2 │ Mazda RX4 Wag        33.81
#    3 │ Datsun 710           36.708
#    4 │ Hornet 4 Drive       34.454
#    5 │ Hornet Sportabout    30.107
#    6 │ Valiant              29.141
#   ⋮  │         ⋮             ⋮
#   27 │ Porsche 914-2        41.86
#   28 │ Lotus Europa         48.944
#   29 │ Ford Pantera L       25.438
#   30 │ Ferrari Dino         31.717
#   31 │ Maserati Bora        24.15
#   32 │ Volvo 142E           34.454
#                     20 rows omitted
# ```

# ## Sorting columns
# Ibis uses `order_by` similar to SQLs `ORDER BY`

# Ibis
# ```python
# mtcars.order_by(_.mpg)
# ```
# ```
# ┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┓
# ┃ model               ┃ mpg     ┃ cyl   ┃ disp    ┃ hp    ┃ drat    ┃ wt      ┃ qsec    ┃ vs    ┃ am    ┃ gear  ┃ carb  ┃
# ┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━┩
# │ string              │ float64 │ int64 │ float64 │ int64 │ float64 │ float64 │ float64 │ int64 │ int64 │ int64 │ int64 │
# ├─────────────────────┼─────────┼───────┼─────────┼───────┼─────────┼─────────┼─────────┼───────┼───────┼───────┼───────┤
# │ Cadillac Fleetwood  │    10.4 │     8 │   472.0 │   205 │    2.93 │   5.250 │   17.98 │     0 │     0 │     3 │     4 │
# │ Lincoln Continental │    10.4 │     8 │   460.0 │   215 │    3.00 │   5.424 │   17.82 │     0 │     0 │     3 │     4 │
# │ Camaro Z28          │    13.3 │     8 │   350.0 │   245 │    3.73 │   3.840 │   15.41 │     0 │     0 │     3 │     4 │
# │ Duster 360          │    14.3 │     8 │   360.0 │   245 │    3.21 │   3.570 │   15.84 │     0 │     0 │     3 │     4 │
# │ Chrysler Imperial   │    14.7 │     8 │   440.0 │   230 │    3.23 │   5.345 │   17.42 │     0 │     0 │     3 │     4 │
# │ Maserati Bora       │    15.0 │     8 │   301.0 │   335 │    3.54 │   3.570 │   14.60 │     0 │     1 │     5 │     8 │
# │ Merc 450SLC         │    15.2 │     8 │   275.8 │   180 │    3.07 │   3.780 │   18.00 │     0 │     0 │     3 │     3 │
# │ AMC Javelin         │    15.2 │     8 │   304.0 │   150 │    3.15 │   3.435 │   17.30 │     0 │     0 │     3 │     2 │
# │ Dodge Challenger    │    15.5 │     8 │   318.0 │   150 │    2.76 │   3.520 │   16.87 │     0 │     0 │     3 │     2 │
# │ Ford Pantera L      │    15.8 │     8 │   351.0 │   264 │    4.22 │   3.170 │   14.50 │     0 │     1 │     5 │     4 │
# │ …                   │       … │     … │       … │     … │       … │       … │       … │     … │     … │     … │     … │
# └─────────────────────┴─────────┴───────┴─────────┴───────┴─────────┴─────────┴─────────┴───────┴───────┴───────┴───────┘
# ```
# While TidierDB uses `@arrange` like TidierData.jl

# TidierDB
# ```
# @chain t(mtcars) @arrange(mpg) @collect
# ```
# ```
# 32×12 DataFrame
#  Row │ model                mpg       cyl     disp      hp      drat      wt        qsec      vs      am      gear    carb   
#      │ String?              Float64?  Int64?  Float64?  Int64?  Float64?  Float64?  Float64?  Int64?  Int64?  Int64?  Int64? 
# ─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │ Cadillac Fleetwood       10.4       8     472.0     205      2.93     5.25      17.98       0       0       3       4
#    2 │ Lincoln Continental      10.4       8     460.0     215      3.0      5.424     17.82       0       0       3       4
#    3 │ Camaro Z28               13.3       8     350.0     245      3.73     3.84      15.41       0       0       3       4
#    4 │ Duster 360               14.3       8     360.0     245      3.21     3.57      15.84       0       0       3       4
#    5 │ Chrysler Imperial        14.7       8     440.0     230      3.23     5.345     17.42       0       0       3       4
#    6 │ Maserati Bora            15.0       8     301.0     335      3.54     3.57      14.6        0       1       5       8
#   ⋮  │          ⋮              ⋮        ⋮        ⋮        ⋮        ⋮         ⋮         ⋮        ⋮       ⋮       ⋮       ⋮
#   27 │ Porsche 914-2            26.0       4     120.3      91      4.43     2.14      16.7        0       1       5       2
#   28 │ Fiat X1-9                27.3       4      79.0      66      4.08     1.935     18.9        1       1       4       1
#   29 │ Honda Civic              30.4       4      75.7      52      4.93     1.615     18.52       1       1       4       2
#   30 │ Lotus Europa             30.4       4      95.1     113      3.77     1.513     16.9        1       1       5       2
#   31 │ Fiat 128                 32.4       4      78.7      66      4.08     2.2       19.47       1       1       4       1
#   32 │ Toyota Corolla           33.9       4      71.1      65      4.22     1.835     19.9        1       1       4       1
#                                                                                                               20 rows omitted
# ```

# ## Selecting columns
# In Ibis, columns must be prefixed with the table name, or in this case `_`, or they can be given as a string. Finally to using helper functions like `startswith` requires importing selectors as above.

# Ibis
# ```
# mtcars.select(s.startswith("m"), "drat", _.wt)
# ```
# ```
# ┏━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┓
# ┃ model             ┃ mpg     ┃ drat    ┃ wt      ┃
# ┡━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━┩
# │ string            │ float64 │ float64 │ float64 │
# ├───────────────────┼─────────┼─────────┼─────────┤
# │ Mazda RX4         │    21.0 │    3.90 │   2.620 │
# │ Mazda RX4 Wag     │    21.0 │    3.90 │   2.875 │
# │ Datsun 710        │    22.8 │    3.85 │   2.320 │
# │ Hornet 4 Drive    │    21.4 │    3.08 │   3.215 │
# │ Hornet Sportabout │    18.7 │    3.15 │   3.440 │
# │ Valiant           │    18.1 │    2.76 │   3.460 │
# │ Duster 360        │    14.3 │    3.21 │   3.570 │
# │ Merc 240D         │    24.4 │    3.69 │   3.190 │
# │ Merc 230          │    22.8 │    3.92 │   3.150 │
# │ Merc 280          │    19.2 │    3.92 │   3.440 │
# │ …                 │       … │       … │       … │
# └───────────────────┴─────────┴─────────┴─────────┘
# ```
# TidierDB does not require names to be prefixed and, like TidierData, tidy column selection with `starts_with`, `ends_with`, and `contains` is supported at base. TidierDB also supports providing column names as strings, although this would only be needed in the setting of renaming a column with a space in it.

# TidierDB
# ```julia
# @chain t(mtcars) @select(starts_with("m"), "drat", wt) @collect
# ```
# ```
# 32×4 DataFrame
#  Row │ model              mpg       drat      wt       
#      │ String?            Float64?  Float64?  Float64? 
# ─────┼─────────────────────────────────────────────────
#    1 │ Mazda RX4              21.0      3.9      2.62
#    2 │ Mazda RX4 Wag          21.0      3.9      2.875
#    3 │ Datsun 710             22.8      3.85     2.32
#    4 │ Hornet 4 Drive         21.4      3.08     3.215
#    5 │ Hornet Sportabout      18.7      3.15     3.44
#    6 │ Valiant                18.1      2.76     3.46
#   ⋮  │         ⋮             ⋮         ⋮         ⋮
#   27 │ Porsche 914-2          26.0      4.43     2.14
#   28 │ Lotus Europa           30.4      3.77     1.513
#   29 │ Ford Pantera L         15.8      4.22     3.17
#   30 │ Ferrari Dino           19.7      3.62     2.77
#   31 │ Maserati Bora          15.0      3.54     3.57
#   32 │ Volvo 142E             21.4      4.11     2.78
#                                         20 rows omitted
# ```

# ## Multi step queries and summarizing
# Aggregating data is done with `aggregate` in Ibis and `@summarize` in TidierDB. To group data, Ibis uses `by = ` within the `aggregate` call vs TidierDB adheres to `@group_by` convention

# Ibis
# ```python
# mtcars.aggregate(
#     total_hp=_.hp.sum(),
#     avg_hp=_.hp.mean(),
#     having=_.hp.sum() < 1000,
#     by=['cyl']
# )
# ```
# ```
# ┏━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┓
# ┃ cyl   ┃ total_hp ┃ avg_hp     ┃
# ┡━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━┩
# │ int64 │ int64    │ float64    │
# ├───────┼──────────┼────────────┤
# │     6 │      856 │ 122.285714 │
# │     4 │      909 │  82.636364 │
# └───────┴──────────┴────────────┘
# ```
# In TidierDB, `@filter` will automatically determine whether the criteria belong in a `WHERE` or `HAVING` SQL clause.  

# TidierDB
# ```julia
# @chain t(mtcars) begin
#     @group_by(cyl)
#     @summarize(total_hp = sum(hp),
#                avg_hp = avg(hp))
#     @filter(total_hp < 1000)
#     @collect
# end
# ```
# ```
# 2×3 DataFrame
#  Row │ cyl     total_hp  avg_hp   
#      │ Int64?  Int128?   Float64? 
# ─────┼────────────────────────────
#    1 │      6       856  122.286
#    2 │      4       909   82.6364
# ```

# ## Renaming columns
# Both tools use `rename`/@rename to rename columns

# Ibis
# ```python
# mtcars.rename(make_model = "model").select(_.make_model)
# ```
# ```
# ┏━━━━━━━━━━━━━━━━━━━┓
# ┃ make_model        ┃
# ┡━━━━━━━━━━━━━━━━━━━┩
# │ string            │
# ├───────────────────┤
# │ Mazda RX4         │
# │ Mazda RX4 Wag     │
# │ Datsun 710        │
# │ Hornet 4 Drive    │
# │ Hornet Sportabout │
# │ Valiant           │
# │ Duster 360        │
# │ Merc 240D         │
# │ Merc 230          │
# │ Merc 280          │
# │ …                 │
# └───────────────────┘
# ```
# TidierDB
# ```julia
# @chain t(mtcars) @rename(model_make = model) @select(model_make) @collect
# ```
# ```
# 32×1 DataFrame
#  Row │ model_make        
#      │ String?           
# ─────┼───────────────────
#    1 │ Mazda RX4
#    2 │ Mazda RX4 Wag
#    3 │ Datsun 710
#    4 │ Hornet 4 Drive
#    5 │ Hornet Sportabout
#    6 │ Valiant
#   ⋮  │         ⋮
#   27 │ Porsche 914-2
#   28 │ Lotus Europa
#   29 │ Ford Pantera L
#   30 │ Ferrari Dino
#   31 │ Maserati Bora
#   32 │ Volvo 142E
#           20 rows omitted
# ```