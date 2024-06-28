# Establishing a connection with the Databricks SQL Rest API requires a token. 

# ## Connecting 
# Connection is established with the `connect` function as shown below. Connection requires 5 items as strings
# - account instance : [how do to find your instance](https://docs.databricks.com/en/workspace/workspace-details.html)
# - OAuth token : [how to generate your token](https://docs.databricks.com/en/dev-tools/auth/pat.html)
# - Database Name
# - Schema Name
# - warehouse_id


# Connecting will then proceed as follows: 
# ```julia
# instance_id = "string_id"
# token "string_token"
# warehouse_id = "e673cd4f387f964a"
# con = connect(:databricks, instance_id, token, "DEMODB", "PUBLIC", warehouse_id)
# # After connection is established, a you may begin querying.
# stable_table_metadata = db_table(con, "MTCARS")
# @chain from_query(stable_table_metadata) begin
#    @select(WT)
#    @mutate(TEST = WT *2)
#    #@aside @show_query _
#    @collect
# end
# ```