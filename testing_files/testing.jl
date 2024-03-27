using CSV, LibPQ

include("fxns.jl")
include("TBD_macros.jl")
include("db_parsing.jl")
include("postgresparsing.jl")
include("joins_sq.jl")
mtcars = CSV.read("mtcars.csv", DataFrame)
join_test = CSV.read("join_test.csv", DataFrame)

db = SQLite.DB() 
SQLite.load!(mtcars, db, "mtcars2")
SQLite.load!(join_test, db, "join_test3")

set_sql_mode(:lite)  # Switch to SQLite mode
set_sql_mode(:postgres)

@chain start_query_meta(db, :mtcars2) begin
  @filter is_missing(carb)
  @collect
end

@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @mutate(min= maximum(mpg), a = round(wt))
    @collect
   # @show_query
end

@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @summarise( min = maximum(mpg))
   # @collect
    @show_query
end



DBInterface.execute(db, "


SELECT groups, AVG(percent) AS mean
        FROM df_mem
        GROUP BY groups 
        HAVING groups != b
        ") |>DataFrame
        
SQLite.load!(join_test, db, "join_test3")
data2 = DataFrame(id = [1, 1, 2, 2],
variable = ["A", "B", "A", "B"],
value = [1, 2, 3, 4]);


SQLite.load!(data2, db, "data2")
test = start_query_meta(db, :mtcars2)
@show_query(test)
@chain start_query_meta(db, :mtcars2) begin
   @mutate(carb = replace_missing(carb, 9),
              efficiency = case_when(
                    mpg >= 21 , "12",
                    mpg == 15.8 , "15.8",
                    mpg > 15 , "14",
                    "44") )
   # @mutate(ifelse = if_else(carb2>=4, "wow", cyl))
   #@select(ifelse)
#  @show_query
  @collect
end
@chain start_query_meta(db, :mtcars2) begin
   @mutate(carb = replace_missing(carb, 9),efficiency = case_when(
        mpg >= 21 , "12",
        mpg == 15.8 , "15.8",
        mpg > 15 , "14",
        "44") )
    @mutate(ifelse = if_else(carb>=4, "wow", "ok"))
   #@select(ifelse)
  @collect
end

@chain start_query_meta(db, :mtcars2) begin
   # @select !mpg Column1  (wt:vs)
    #@group_by(cyl)
    @arrange(desc(!!my_var))
   # @slice_sample(3)
    @show_query
   # @collect
end 
my_var = [:cyl, :mpg]
#my_var = ["cyl", "mpg"]
#my_var = "cyl"
@chain start_query_meta(db, :mtcars2) begin
    @select !!my_var vs:gear
  #  @filter(!!my_var >4 )
  #  @slice_max(mpg, 5)
    @show_query
    #@collect
end 

my_var = ["cyl", "mpg"]
my_var = [:cyl, :mpg]
@chain start_query_meta(db, :mtcars2) begin
    @select !hp  !(gear:carb)
  #  @filter(!!my_var >4 )
  #  @slice_max(mpg, 5)
   # @show_query
    @collect
end 
my_var = :cyl
test = 4
@chain start_query_meta(db, :mtcars2) begin
   # @select !!my_var vs:gear
    @filter(!!my_var > !!test )
  #  @slice_max(mpg, 5)
    @show_query
   # @collect
end 
my_var = [:cyl, :mpg]
@chain start_query_meta(db, :mtcars2) begin
    @filter(drat> 2)
    @group_by(gear)
    @summarise(cyl2 = mean(!!my_var))
    @filter(cyl2 >4, gear >2)
  #  @slice_max(mpg, 5)
    @show_query

   # @collect
end 

@chain start_query_meta(db, :mtcars2) begin
    @group_by( gear)
    @summarize(across((cyl, mpg), (mean, sum)))
    @show_query
   # @collect
end
my_var = [:drat, :mpg, :wt]
@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl, gear)
    @summarize(across((!!my_var), (mean, sum)))
    #@show_query
    @collect
end
@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl, gear)
    @summarise(across((drat, mpg, wt), (mean, sum)))
    #@show_query
    @collect
end
@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl, gear)
    @summarize(across((starts_with("a"), ends_with("s")), (mean, sum)))
    @show_query
    #@collect
end


@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @summarize(sum = sum(hp))
    @show_query
    #@collect
end
@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @summarize(mpg = mean(mpg), sum = sum(mpg))
   # @show_query
    @collect
end    

@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @summarize(mpg = mean(mpg), sum = sum(mpg))
    @mutate(rounded = round(mpg), sqaured2 = mpg^2)
    @mutate(rounded2 = mpg^3-100^2, efficiency = case_when(
        mpg >= 21 , "12",
        mpg == 15.8 , "15.8",
        mpg > 15 , "14",
        "44"),
        cyl2 = cyl+2 )
    #@mutate(cyl = cyl+2)
   #@show_query
    @collect
end
@chain start_query_meta(db, :mtcars2) begin
    @mutate(rounded = round(mpg), sqaured2 = mpg^2)
    @filter(sqaured2 < 200)
    #@show_query
    @collect
end

@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @summarize(mean_mpg = mean(mpg), sum_mpg = sum(mpg))
    @mutate( rounded = round(mean_mpg), sqaured2 = mean_mpg^2,
    efficiency = case_when(
        mean_mpg >= 21 , "12",
        mean_mpg == 15.8 , "15.8",
        mean_mpg > 15 , "14",
        "44"))
    @filter(mean_mpg > 10 && sum_mpg < 200^2)
    #@show_query
    @collect
end

@chain start_query_meta(db, :mtcars2) begin
    @filter(gear > 4 )
    @group_by(cyl)
    @summarize(mpg_mean = mean(mpg))
    @mutate( rounded = round(mpg_mean), sqaured2 = mpg_mean^2)
    @filter(mpg_mean >22 )
    #@select(rounded)
   # @select(rounded:sqaured2)
   # @show_query
   # @collect
end

@chain start_query_meta(db, :mtcars2) begin
  #  @select(mpg, cyl, drat)
   # @group_by(cyl, gear)
    #@summarize(mpg = mean(mpg))
   # @mutate(sqaured = mpg^2 )
    @mutate(ifelse = if_else(carb>=4, "wow", "ok"))
    @mutate(ifelse = if_else(carb>=4, 10, 2))
    @mutate(efficiency = case_when(
        mpg >= 21 , "12",
        mpg == 15.8 , "15.8",
        mpg > 15 , "14",
        "44"  # 'true' acts as the default or 'else' case
    ))
    @filter(efficiency=="12")
    #@show_query
    @collect 
    #@pull(efficiency)
end 

@chain start_query_meta(db, :mtcars2) begin
    @mutate(efficiency = case_when(
        mpg >= 21 , "12",
        mpg > 15 , "14",
        "44"  # 'true' acts as the default or 'else' case
    ))
    @mutate(mpg2 = mpg^2, mpg3 = mpg^3)
   #@show_query
    @collect
end

@chain start_query_meta(db, :mtcars2) begin
   # @mutate4(carb = replace_missing(carb, 9))
   # @show_query
   @mutate(mpg = mpg^2, mpg2 = mpg^3)
  # @mutate(mpg3 = mpg^3)

    #@distinct
    #@show_query
    @collect
end 

using TidierData: @pull
@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @summarize(mpg = mean(mpg))
    @mutate(sqaured = mpg^2, 
               rounded = round(mpg), 
               efficiency = case_when(
                             mpg^2 >= 500 , 12,
                             mpg < 15.2 , 14,
                              44))
    #@mutate(ifelse = if_else(carb>=4, "wow", "ok"))
                          
    #@mutate(test = efficiency+2)
  #  @arrange(rounded)
    #@filter(efficiency<44)
    @collect
   # @show_query
end
@chain start_query_meta(db, :mtcars2) begin
  #  @select(mpg, cyl, drat)
    @group_by(cyl)
    @summarize(mpg = mean(mpg))
   # @mutate2(sqaured = mpg^2 )
    @mutate(rounded = round(mpg), test = mpg+4)
   # @filter(mpg > 19)
   # @arrange(desc(rounded))
    #@select(rounded)
    @collect
   # @show_query
end
test = start_query_meta(db, :mtcars2)
@chain start_query_meta(db, :mtcars2) begin
  #  @select(mpg, cyl, drat)
    @group_by(cyl,gear)
    @summarize(mpg = mean(mpg), sum = sum(mpg))
    @filter(sum > 40)
   # @mutate(sqaured = mpg^.5 )
    @mutate(rounded = round(mpg), test = mpg+4)
   # @filter(sum > 220)
    #@arrange(rounded)
   # @select(rounded)
    @arrange(sum, desc(rounded))
    #@collect
    @show_query
end
query=start_query_meta(db, :mtcars2)
@chain start_query_meta(db, :mtcars2) begin
   #@select(mpg, cyl)
   @filter(mpg>22)

    @group_by(!(Column1:mpg), !(disp:am), !carb)
    @summarize(mpg = mean(mpg))
    @mutate(rounded = round(mpg), test = mpg+4)
    @mutate(sqaured = mpg^2 )
   # @filter(test>31)

   # @select(mpg^2)
    @collect
    #@show_query
end 

@chain start_query_meta(db, :mtcars2) begin
   # @select(mpg, cyl)
   # @group_by(cyl, gear)
   # @summarize(mpg = mean(mpg))
    @mutate(sqaured = mpg^2 )
    @mutate(rounded = round(mpg))
    @filter(mpg > 19)
   # @arrange(rounded)
   #@select(rounded)
 #  @collect
    @show_query
end
@chain start_query_meta(db, :mtcars2) begin
    #@select(mpg:drat)
    #@show_query
    @group_by(cyl, gear)
    @mutate(sum = sum(mpg))
    @collect
end
@chain start_query_meta(db, :mtcars2) begin
    #@select(mpg:drat)
    #@show_query
   # @group_by(cyl, gear)
   # @summarize(mpg = mean(mpg))
   # @mutate(sqaured = mpg^2 )
   # @mutate(rounded = round(mpg))
    @filter(mpg > 23)
    @arrange(desc(mpg))
   # @distinct
   # @show_query
    @collect
end 

@chain start_query_meta(db, :mtcars2) begin
    #@select(mpg:drat)
    #@show_query
    @group_by(cyl, gear)
    @summarize(mpg = mean(mpg))
    @mutate(sqaured = mpg^2 )
    @mutate(rounded = round(mpg))
    #@show_query
    @collect
end



@chain start_query_meta(db, :mtcars2) begin
    #@select(mpg:drat)
   #@show_query
    @collect
end

@chain start_query_meta(db, :mtcars2) begin
    #@select(mpg, cyl)
    @group_by(cyl)
    @window_order(mpg)
    @window_frame(3, 0)
    @mutate(z = cumsum(drat))
    @filter(z >3)
    @show_query
    #@collect
end

@chain start_query_meta(db, :mtcars2) begin
    #@select(mpg, cyl)
    @group_by(!(Column1:mpg),!(disp:carb))
   # @window_order(mpg)
    #@window_frame(3, 0)

  #  @mutate2(z = sum(drat))
   # @show_query()

   @show_query
   # @collect
end

@chain start_query_meta(db, :mtcars2) begin
    #@select(mpg, cyl)
    @group_by(cyl)
    @window_order(mpg)
    @window_frame(3, 0)
    @mutate(cumum = cumsum(drat))
   #@mutate(z = sum(drat))
   # @show_query()

   @show_query
   # @collect
end


@chain start_query_meta(db, :mtcars2) begin
    @select(!(mpg:drat))
    @collect
   #@show_query
end

@chain start_query_meta(db, :mtcars2) begin
    @select(!mpg, !cyl)
    @collect
   #@show_query
end

@chain start_query_meta(db, :mtcars2) begin
    @select(!mpg, !cyl)
    @arrange(desc(drat))
   #@distinct()
     @collect
   #@show_query
end

@chain start_query_meta(db, :mtcars2) begin
    @left_join(:join_test2, :(mtcars2.Column1 = join_test2.Column1))
    @show_query
end


@chain start_query_meta(db, :mtcars2) begin
    @group_by(Column1)
    @summarize(test = mean(mpg))
    @left_join(:join_test3, join_test3.ID=mtcars2.Column1)
   # @select(:Column1, "join_test2.Column2")  # Assuming you want to select specific columns post-join
    #@select(Column1, !mpg)
   @show_query  # Assuming this is how you intended to finalize and show the query
    #@collect
end
start_query_meta(db, :join_test3)
@chain start_query_meta(db, :mtcars2) begin
    @left_join(:join_test3, join_test3.ID=mtcars2.Column1)
    @select(mpg, vs:ID) ## also supports `starts_with`, `ends_with`, `contains`
    @collect
end

@chain start_query_meta(db, :mtcars2) begin
   # @select(!Column1)
    @left_join(:join_test2,  :Column1)
   # @select(:Column1, "join_test2.Column2")  # Assuming you want to select specific columns post-join
#    @show_query  # Assuming this is how you intended to finalize and show the query

    @collect
end


@chain start_query_meta(db, :mtcars2) begin
    @left_join(:join_test3, ID=Column1)
   # @select(mpg, vs:ID)
    @collect
end

@chain start_query_meta(db, :mtcars2) begin
    @filter(!starts_with(Column1, "M"))
    @left_join(:join_test3, ID, Column1)
    @select(carb:wt2)
    #@collect
    @show_query

end

@chain start_query_meta(db, :mtcars2) begin
    @filter(starts_with(Column1, "M"))
    @left_join(:join_test3, ID, Column1) ## autodetects the table/cte to apply to ID and, more importantly, column
    @select(mpg, vs:ID) ## also supports `starts_with`, `ends_with`, `contains`
    @show_query
end 
@chain start_query_meta(db, :mtcars2) begin
    @filter(!starts_with(Column1, "M"))
    @left_join(:join_test3, join_test3.ID=mtcars2.Column1)
    #@select(ends_with("t"))
    @show_query
 #   @collect
end
@chain start_query_meta(db, :mtcars2) begin
  #  @left_join(:join_test3, join_test3.ID=mtcars2.Column1)
    @select(!Column1)
    #@count(gear)
    @collect
end

@chain start_query_meta(db, :mtcars2) begin
    @group_by(cyl)
    @summarize(mpg = mean(mpg))
    @mutate(sqaured = mpg^2, 
               rounded = round(mpg), 
               efficiency = case_when(
                             mpg >= cyl^2 , 12,
                             mpg < 15.2 , 14,
                              44))
    @filter(efficiency>12)                       
    @arrange(rounded)
   # @filter(efficiency>12)
   # @show_query
   #@select(sqaured:efficiency)
#    @collect
@show_query
end


@chain start_query_meta(db, :mtcars2) begin
    @filter(str_detect(Column1, "M"))
    @group_by(cyl)
    @summarize(mpg = mean(mpg))
    @mutate(sqaured = mpg^2, 
               rounded = round(mpg), 
               efficiency = case_when(
                             mpg >= cyl^2 , 12,
                             mpg < 15.2 , 14,
                              44))            
    @filter(efficiency>12)  
    @mutate(TEST = efficiency + 4^2)                     
    @arrange(rounded)
    #@show_query
    @collect
end

@chain start_query_meta(db, :mtcars2) begin
    @filter(Column1 == starts_with( "M"))
    @group_by(cyl)
    @summarize(mpg = mean(mpg))
    @mutate(sqaured = mpg^2, 
               rounded = round(mpg), 
               efficiency = case_when(
                             mpg >= cyl^2 , 12,
                             mpg < 15.2 , 14,
                              44))            
    @filter(efficiency>12)  
    @mutate(TEST = efficiency + 4^2)                     
    @arrange(rounded)
   # @show_query
    @collect
end



@chain start_query_meta(db, :mtcars2) begin
  @filter(str_detect(Column1, "X"))
  @group_by(cyl, gear)  
  @summarize(N = n()) 
  @slice_max(N, n=1) 
  #collect()
#  arrange(mpg) %>% 

  #mutate(z = sum(drat)) %>%
  #@show_query()
  @collect()
end
@chain start_query_meta(db, :mtcars2) begin
  @group_by(cyl)  
  #filter(str_detect(Column1, "X"))
  #summarise(N = n()) %>% 
  @slice_max(mpg, n =2) 
  
  #collect()
#  arrange(mpg) %>% 

  #mutate(z = sum(drat)) %>%
  #@show_query()
  @collect()
end
set_sql_mode(:lite)
@chain start_query_meta(db, :mtcars2) begin
  #@group_by(cyl, gear)  
  @slice_min(mpg, n =5) 
 # @show_query
  @collect2()
end

@chain start_query_meta(db, :mtcars2) begin
 # @group_by(cyl, gear)  
  #@summarize(mpg)
  #@slice_max(mpg, n =2) 
  @group_by(cyl)
  @summarize(test = mean(mpg))
  #@show_query()
  @collect()
end

@chain start_query_meta(db, :mtcars2) begin
  @group_by(cyl, gear)  
  #@summarize(mpg)
  @slice_min(mpg, n =2) 
  #@show_query()
  @collect()
end


@chain start_query_meta(db, :mtcars2) begin
  @group_by(cyl, gear)  
  @summarize(n=n())
  @slice_min(n, n =1) 
  #@show_query()
  @collect()
end

@chain start_query_meta(db, :mtcars2) begin
  @mutate(test = as_float(cyl))
  @show_query
  #@collect()
end


@chain start_query_meta(db, :mtcars2) begin
    @filter(Column1 == starts_with("M"))
    #@group_by(cyl)
    @left_join(:join_test3, ID, Column1) ## autodetects the table/cte to apply to ID and, more importantly, column
   # @select(mpg, vs:ID) ## also supports `starts_with`, `ends_with`, `contains`
    @collect
end 

start_query_meta(db, :join_test3)



my_var = "gear"
my_var = :gear
my_val = 3.7
@chain start_query_meta(db, :mtcars2) begin
    @filter(Column1 != starts_with("M"))
    @group_by(cyl)
    @summarize(mpg = mean(mpg))
    @mutate(test = !!my_val * !!my_var,
        sqaured = (!!my_var)^2, 
               rounded = round(!!my_var), 
               efficiency = case_when(
                             mpg >= (!!my_var)^2 , 12,
                             mpg < !!my_val , 14,
                              44))            
    @filter(efficiency>12)                       
    @arrange(rounded)
    @show_query
    #@collect
end
other_var
super = [:cyl, :gear]
@chain start_query_meta(db, :mtcars2)  begin
  #  @rename(test = !!other_var)
   # @count(!!super)
    @slice_min()
    @collect
end


@chain start_query_meta(db, :df_mem) begin
      # @window_frame(3, 0)
       @window_order(groups)
       @mutate(cumulative = cumsum(percent))
       #@collect
       @show_query
end




using DataFrames
df = DataFrame(id = [string('A' + i ÷ 26, 'A' + i % 26) for i in 0:9], 
                        groups = [i % 2 == 0 ? "aa" : "bb" for i in 1:10], 
                        value = repeat(1:5, 2), 
                        percent = 0.1:0.1:1.0);
using SQLite
db = SQLite.DB();

SQLite.load!(df, db, "df_mem");
using Chain
@chain start_query_meta(db, :df_mem) begin
       @group_by(groups)
       @slice_sample(n = 2)
       @show_query
       end