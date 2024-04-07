# There are a few important syntax and behavior differences between TidierDB.jl and TidierData.jl outlined below. 

## Starting Chain
# `db_table(connection, :table_name)` is used to start a chain instead of a classic dataframe

## group_by -> mutate
# In TidierDB, when performing `@group_by` then `@mutate`, after applying all of the mutations in the clause to the grouped data, the table is ungrouped. To perform subsequent grouped mutations/slices/summarizations, the user would have to regroup the data. This is something we will work to resolve, but as of version .0.1.0, this is the bevahior. This is demonstrated below with 
#using TidierDB
#df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
#                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
#                        value = repeat(1:5, 2), 
#                        percent = 0.1:0.1:1.0);

# mem = duckdb_open(":memory:");
# db = duckdb_connect(mem);
# For these examples we will use DuckDB, the default backend, although SQLite, Postgres, MySQL, MSSQL, and ClickHouse are possible.
# copy_to(db, df, "df_mem"); # copying over the df to memory

# @chain db_table(db, :df_mem) begin
#    @group_by(groups)
#    @summarise(mean = mean(percent))
#    @slice_max(percent)
#    @collect
# end     

# @chain db_table(db, :df_mem) begin
#    @group_by(groups)
#    @mutate(max = maximum(percent), min = minimum(percent))
#    @group_by(groups)
#    @summarise(mean = mean(percent))
#    @collect
# end     

## Joining
# There are 2 key differences for joining:
# 1. When joining 2 tables, the new table you are choosing to join must be prefixed with a colon. 
# 2. The column on both the new and old table must be specified. They do not need to be the same, and given SQL behavior where both columns are kept when joining two tables, it is preferrable if they have different names. This avoids "ambiguous reference" errors that would otherwise come up and complicate the use of tidy selection for columns. 

# df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
#                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
#                score = [88, 92, 77, 83, 95, 68, 74]);

# copy_to(db, df2, "df_join");

# @chain db_table(db, :df_mem) begin
#    @left_join(:df_join, id2, id)
#    @collect
#end

## `case_when`
# In TidierDB, after the clause is completed, the result for the new column should is separated by comma ( , )
# this is in contrast to TidierData.jl, where the result for the new column is separated by a => 
# @chain db_table(db, :df_mem) begin
#    @mutate(new_col = case_when(percent > .5, "Pass",  # in TidierData, percent > .5 => "Pass", 
#                                percent <= .5, "Try Again", # percent <= .5 => "Try Again"
#                                true, "middle"))
#    @collect
# end

## Interpolation
# To use !! Interpolation, instead of being able to define the alternate names/value in the global context, the user has to `add_interp_parameter!`. This will hopefully be fixed in future versions. Otherwise behavior is the same.
# Also, when using interpolation with exponenents, the interpolated value must go inside of parenthesis. 
# add_interp_parameter!(:test, :percent) # this still supports strings, vectors of names, and values

# @chain db_table(db, :df_mem) begin
#    @mutate(new_col = case_when((!!test)^2 > .5, "Pass",
#                                (!!test)^2 < .5, "Try Again",
#                                "middle"))
#    @collect
# end

## Slicing Ties
# Slice will always return ties due to SQL behavior
## Joining
# There are 2 key differences for joining:
# 1. When joining 2 tables, the new table you are choosing to join must be prefixed with a colon. 
# 2. The column on both the new and old table must be specified. They do not need to be the same, and given SQL behavior where both columns are kept when joining two tables, it is preferrable if they have different names. This avoids "ambiguous reference" errors that would otherwise come up and complicate the use of tidy selection for columns. 

# df2 = DataFrame(id2 = ["AA", "AC", "AE", "AG", "AI", "AK", "AM"],
#                category = ["X", "Y", "X", "Y", "X", "Y", "X"],
#                score = [88, 92, 77, 83, 95, 68, 74]);

#copy_to(db, df2, "df_join");

# @chain db_table(db, :df_mem) begin
#    @left_join(:df_join, id2, id)
#    @collect
# end

## `case_when`
# In TidierDB, after the clause is completed, the result for the new column should is separated by comma ( , )
# this is in contrast to TidierData.jl, where the result for the new column is separated by a => 
# @chain db_table(db, :df_mem) begin
#    @mutate(new_col = case_when(percent > .5, "Pass",  # in TidierData, percent > .5 => "Pass", 
#                                percent <= .5, "Try Again", # percent <= .5 => "Try Again"
#                                true, "middle"))
#    @collect
# end

## Interpolation
# To use !! Interpolation, instead of being able to define the alternate names/value in the global context, the user has to `add_interp_parameter!`. This will hopefully be fixed in future versions. Otherwise behavior is the same.
# Also, when using interpolation with exponenents, the interpolated value must go inside of parenthesis. 
# add_interp_parameter!(:test, :percent) # this still supports strings, vectors of names, and values

# @chain db_table(db, :df_mem) begin
#    @mutate(new_col = case_when((!!test)^2 > .5, "Pass",
#                                (!!test)^2 < .5, "Try Again",
#                                "middle"))
#    @collect
# end

## Slicing Ties
# Slice will always return ties due to SQL behavior