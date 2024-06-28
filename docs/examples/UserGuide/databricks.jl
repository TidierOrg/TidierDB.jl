# Establishing a connection with the Databricks SQL Rest API requires a token. 

# ## Connecting 
# Connection is established with the `connect` function as shown below. Connection requires 5 items as strings
# - account instance : [how do to find your instance](https://docs.databricks.com/en/workspace/workspace-details.html)
# - OAuth token : [how to generate your token](https://docs.databricks.com/en/dev-tools/auth/pat.html)
# - Database Name
# - Schema Name
# - warehouse_id

# One thing to note,
# Since each time `db_table` runs, it runs a query to pull the metadata, you may choose to use run `db_table` and save the results, and use these results with `from_query()`. This will reduce the number of queries to your database and is illustrated below.

# ```julia
# instance_id = "string_id"
# token "string_token"
# warehouse_id = "e673cd4f387f964a"
# con = connect(:databricks, instance_id, token, "DEMODB", "PUBLIC", warehouse_id)
# # After connection is established, a you may begin querying.
# stable_table_metadata = db_table(con, "mtcars")
# @chain from_query(stable_table_metadata) begin
#    @select(wt)
#    @mutate(test = wt *2)
#    #@aside @show_query _
#    @collect
# end
# ```
# ```
#  32×2 DataFrame
#  Row │ wt       test    
# │ Float64  Float64 
# ─────┼──────────────────
# 1 │   2.62     5.24
# 2 │   2.875    5.75
# 3 │   2.32     4.64
# 4 │   3.215    6.43
# ⋮  │    ⋮        ⋮
# 29 │   3.17     6.34
# 30 │   2.77     5.54
# 31 │   3.57     7.14
# 32 │   2.78     5.56
#      24 rows omitted
# ```