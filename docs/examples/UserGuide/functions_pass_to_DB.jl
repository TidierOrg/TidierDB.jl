# How can functions pass arguments to a TidierDB chain?

# In short, you have to use a macro instead.

# ## Setting up the macro
# To write a macro that will take arguments and pass them to a TidierDB chain, there are 3 steps:
#   1. Write macro with the desired argument(s), and, after the quote, add the chain. Arguments to be changed/interpolated must be prefixed with `!!`
#   2. Use `@interpolate` to make these arguemnts accessible to the chain. `@interpolate` takes touples as argument (one for the `!!`name, and one for the actual info you want the chain to use) 
#   3. Run `@interpolate` and then the new macro sequentially

# ```
# using TidierDB
# path = "https://gist.githubusercontent.com/seankross/a412dfbd88b3db70b74b/raw/5f23f993cd87c283ce766e7ac6b329ee7cc2e1d1/mtcars.csv"
# copy_to(db, path, "mtcars");
#
# # STEP 1
# macro f1(conditions, number)
#     return quote
#     # add chain here
#       @chain db_table(db, :mtcars) begin
#             @filter(!starts_with(model, "M"))
#             @group_by(cyl)
#             @summarize(mpg = mean(mpg))
#             @mutate(mpg_squared = mpg^2, 
#                     mpg_rounded = round(mpg), 
#                     mpg_efficiency = case_when(
#                           mpg >= cyl^2 , "efficient",
#                           mpg < !!number , "inefficient",
#                           "moderate"))         
#             @filter(mpg_efficiency in [!!conditions])
#             @arrange(desc(mpg_rounded))
#             @collect
#          end # end chain
#     end # end macro.
# end
# ```
# ```julia
# # STEP 2
# conditions = ["moderate", "efficient"];
# number = 15.2;
# @interpolate((:conditions, conditions), (:number, number));
# @f1(conditions, number)
# ```
# ```2×5 DataFrame
#  Row │ cyl     mpg       mpg_squared  mpg_rounded  mpg_efficiency 
#      │ Int64?  Float64?  Float64?     Float64?     String?        
# ─────┼────────────────────────────────────────────────────────────
#    1 │      4   27.3444      747.719         27.0  efficient
#    2 │      6   19.7333      389.404         20.0  moderate
# ```

# Lets say you wanted to change try a new condition with a new name, but use the name number, 
# ```julia
# new_condition = ["moderate"];
# @interpolate((:conditions, new_condition), (:number, number));
# @f1(new_condition, number)
# ```
# ```
# 1×5 DataFrame
#  Row │ cyl     mpg       mpg_squared  mpg_rounded  mpg_efficiency 
#      │ Int64?  Float64?  Float64?     Float64?     String?        
# ─────┼────────────────────────────────────────────────────────────
#    1 │      6   19.7333      389.404         20.0  moderate
# ```

# Lets say you wanted to just change `condition`, after making the change you would have to reinterpolate the difference. 
# ```julia
# condition = ["efficient"]
# @interpolate((:conditions, condition))
# @f1(condition, number)
# ```
# ```
# 1×5 DataFrame
#  Row │ cyl     mpg       mpg_squared  mpg_rounded  mpg_efficiency 
#      │ Int64?  Float64?  Float64?     Float64?     String?        
# ─────┼────────────────────────────────────────────────────────────
#    1 │      4   27.3444      747.719         27.0  efficient
# ```

# We recognize this is a bit verbose, and not ideal, but given the TidierDB macro expressions/string interplay, this is the most graceful and functional option available.