# On this page, we'll briefly explore how to use TidierDB macros and `$` witth `@eval` to bulid a function

# For a more indepth explanation, please check out the TidierData page on interpolation

using TidierDB
using DataFrames
db = connect(duckdb());
df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);
copy_to(db, df, "dfm");
df_mem = db_table(db, "dfm");

## Interpolation
# Variables are interpoated using `@eval` and `$`. Place `@eval` before you begin the chain or call a TidierDb macro
# Why Use @eval? In Julia, macros like @filter are expanded at parse time, before runtime variables like vals are available. By using @eval, we force the expression to be evaluated at runtime, allowing us to interpolate the variable into the macro.

num = [3]; column = :id
@eval @chain t(df_mem) begin
        @filter(value in $num) 
        @select($column)
        @collect
    end

# ## Function set up 
# Begin by defining your function as your normally would, but before `@chain` you need to use `@eval`. For the variables to be interpolated in need to be started with `$`
function test(vals, cols)
    @eval @chain t(df_mem) begin
        # vals and cols have $ first
        @filter(value in $vals) 
        @select($cols)
        @collect
    end
end;

vals = [1,  2,  3, 3];
test(vals, [:groups, :value, :percent])

# Now with a new variable 
other_vals = [1];
cols = [:value, :percent];
test(other_vals, cols)


# Defineing a new function
function gs(groups, aggs, new_name, threshold)
    @eval @chain t(df_mem) begin
        # groups and aggs have $ first
        @group_by($groups) 
        @summarize($new_name = mean($aggs))
        @filter($new_name > $threshold)
        @collect
    end
end;

gs(:groups, :percent, :mean_percent, .5)

# Change the column and threshold
gs(:groups, :value, :mean_value, 2)

