# Establishing a connection with the Snowflake SQL Rest API requires a OAuth token specific to the Role the user will use to query tables with.

# ## Connecting 
# Connection is established with the `connect` function as shown below. Connection requires 5 items as strings
# - Account Identifier
# - OAuth token
# - Database Name
# - Schema Name
# - Compute Warehouse name

# Two things to note: 
# - Your OAuth Token may frequently expire, which may require you to rerun your connection line.
# - Since each time `dt` runs, it runs a query to pull the metadata, you may choose to use run `db_table` and save the results, and use these results with`from_query()`
#   - This will reduce the number of queries to your database
#   - Allow you to build a a SQL query and `@show_query` even if the OAuth_token has expired. To `@collect` you will have to reconnect and rerun db_table if your OAuth token has expired

# ```julia
# set_sql_mode(snowflake())
# ac_id = "string_id"
# token = "OAuth_token_string" 
# con = connect(:snowflake, ac_id, token, "DEMODB", "PUBLIC", "COMPUTE_WH")
# # After connection is established, a you may begin querying.
# stable_table_metadata = dt(con, "MTCARS")
# @chain from_query(stable_table_metadata) begin
#    @select(WT)
#    @mutate(TEST = WT *2)
#    #@aside @show_query _
#    @collect
# end
# ```
# ```
# 32×2 DataFrame
#  Row │ WT       TEST    
#      │ Float64  Float64 
# ─────┼──────────────────
#    1 │   2.62     5.24
#    2 │   2.875    5.75
#    3 │   2.32     4.64
#    4 │   3.215    6.43
#   ⋮  │    ⋮        ⋮
#   29 │   3.17     6.34
#   30 │   2.77     5.54
#   31 │   3.57     7.14
#   32 │   2.78     5.56
#          24 rows omitted
# ```

