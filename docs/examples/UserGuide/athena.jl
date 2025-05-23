# To use the Athena AWS backend with TidierDB, set up and a small syntax difference are covered here. 

# ## Connecting 
# Connection is established through AWS.jl as shwon below.

# ```julia
# using TidierDB, AWS
# set_sql_mode(athena())
# # Replace your credentials as needed below
# aws_access_key_id = get(ENV,"AWS_ACCESS_KEY_ID","key")
# aws_secret_access_key = get(ENV, "AWS_SECRET_ACCESS_KEY","secret_key")
# aws_region = get(ENV,"AWS_DEFAULT_REGION","region")

# const AWS_GLOBAL_CONFIG = Ref{AWS.AWSConfig}()
# creds = AWSCredentials(aws_access_key_id, aws_secret_access_key)

# AWS_GLOBAL_CONFIG[] = AWS.global_aws_config(region=aws_region, creds=creds)

# catalog = "AwsDataCatalog"
# workgroup = "primary"
# db = "demodb"
# all_results = true
# results_per_increment = 10
# out_loc = "s3://location/"

# athena_params = Dict(
#     "ResultConfiguration" => Dict(
#         "OutputLocation" => out_loc
#     ),
#     "QueryExecutionContext" => Dict(
#         "Database" => db,
#         "Catalog" => catalog
#     ),
#     "Workgroup" => workgroup
# )
# ```

# ## `dt` differences
# There are two differences for `dt` which are seen in the query below
# 1. The table needs to be passed as a string in the format database.table, ie `"demodb.table_name`
# 2. `dt` requires a third argument: the athena_params from above.

# I would like to acknowledge the work of Manu Francis and this [blog post](https://medium.com/@manuedavakandam/beginners-guide-to-aws-athena-with-julia-a0192f7f4b4a), which helped guide this process  