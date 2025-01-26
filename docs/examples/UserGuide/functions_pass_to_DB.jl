# On this page, we'll briefly explore how to use TidierDB macros and `$` witth `@eval` to bulid a function

# For a more indepth explanation, please check out the TidierData page on interpolation

using TidierDB, DataFrames;

db = connect(duckdb());
df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);

dfv = db_table(db,  df, "dfm");

# ## Interpolation
# Variables are interpoated using `@eval` and `$`. Place `@eval` before you begin the chain or call a TidierDb macro
# Why Use @eval? In Julia, macros like @filter are expanded at parse time, before runtime variables like vals are available. By using @eval, we force the expression to be evaluated at runtime, allowing us to interpolate the variable into the macro.

num = [3]; 
column = :id;
@eval @chain t(dfv) begin
        @filter(value in $num) 
        @select($column)
        @collect
    end

# ## Function set up 
# Begin by defining your function as your normally would, but before `@chain` you need to use `@eval`. For the variables to be interpolated in need to be started with `$`
function test(vals, cols)
    @eval @chain t(dfv) begin
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
    @eval @chain t(dfv) begin
        @group_by($groups) 
        @summarize($new_name = mean($aggs))
        @filter($new_name > $threshold)
        @collect
    end
end;

gs(:groups, :percent, :mean_percent, .5)

# Change the column and threshold
gs(:groups, :value, :mean_value, 2)


# ## Write pipeline function to use inside of chains
# Lets say there is a particular sequence of macros that you want repeatedly use. Wrap this series into a function that accepts a `t(query` as its first argument and returns a `SQLquery` and you can easily resuse it.
function moving_aggs(table, start, stop, group, order, col)
    qry = @eval @chain $table begin 
        @group_by $group
        @window_frame $start $stop
        @window_order $order
        @mutate(across($col, (minimum, maximum, mean)))
    end
    return qry
end;

@chain t(dfv) begin
    moving_aggs(-2, 1, :groups, :percent, :value)
    @filter value_mean > 2.75 
    @aside @show_query _
    @collect
end

# Filtering before the window functions
@chain t(dfv) begin
    @filter(value >=2 )
    moving_aggs(-1, 1, :groups, :percent, :value)
    @aside @show_query _
    @collect
end

# ## Interpolating Queries
# To use a prior, uncollected TidierDB query in other TidierDB macros, interpolate the needed query without showing or collecting it 
ok = @chain t(dfv) @summarize(mean = mean(value));
# The mean value represented in SQL from the above is 3

# With `@filter`
@eval @chain t(dfv) begin 
    @filter( value > $ok) 
    @collect 
end

# With `@mutate`
@eval @chain t(dfv) begin 
    @mutate(value2 =  value + $ok) 
    @collect 
end

# With `@summarize`
@eval @chain t(dfv) begin 
    @summarize(value =  mean(value) * $ok) 
    @collect 
end
