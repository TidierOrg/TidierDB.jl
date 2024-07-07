# TidierDB allows you leverage DuckDB's seamless database integration.

# Using DuckDB, you can connect to an AWS or GoogleCloud Database to query directly without making any local copies. 
# 
# ```julia
# Using TidierDB
# 
# #Connect to Google Cloud via DuckDB
# #google_db = connect(:duckdb, :gbq, access_key="string", secret_key="string")
# #Connect to AWS via DuckDB
# aws_db = connect2(:duckdb, :aws, aws_access_key_id=get(ENV, "AWS_ACCESS_KEY_ID", "access_key"), aws_secret_access_key=get(ENV, "AWS_SECRET_ACCESS_KEY", "secret_access key"), aws_region=get(ENV, "AWS_DEFAULT_REGION", "us-east-1"))
# s3_csv_path = "s3://path/to_data.csv"

# @chain db_table(aws_db, s3_csv_path) begin
#     @filter(!starts_with(column1, "M"))
#     @group_by(cyl)
#     @summarize(mpg = mean(mpg))
#     @mutate(mpg_squared = mpg^2, 
#                mpg_rounded = round(mpg), 
#                mpg_efficiency = case_when(
#                                  mpg >= cyl^2 , "efficient",
#                                  mpg < 15.2 , "inefficient",
#                                  "moderate"))            
#     @filter(mpg_efficiency in ("moderate", "efficient"))
#     @arrange(desc(mpg_rounded))
#     @collect
# end
# ```
# ```
# 2×5 DataFrame
#  Row │ cyl     mpg       mpg_squared  mpg_rounded  mpg_efficiency 
#      │ Int64?  Float64?  Float64?     Float64?     String?        
# ─────┼────────────────────────────────────────────────────────────
#    1 │      4   27.3444      747.719         27.0  efficient
#    2 │      6   19.7333      389.404         20.0  moderate
# ```
