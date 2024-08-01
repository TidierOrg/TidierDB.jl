using TidierData

con = duckdb_connect("memory"); # opening DuckDB database connection.
db = duckdb_open("con");
copy_to(db, "https://raw.githubusercontent.com/rahkum96/Olympics-Dataset-by-sql/main/noc_regions.csv", "noc_regions")
copy_to(db, "https://raw.githubusercontent.com/rahkum96/Olympics-Dataset-by-sql/main/olympic_event.csv", "athlete_events")


# Below is a series of queries based on the questions here to serve as a proof of concept, but also a way to catch bugs. 
# https://techtfq.com/blog/practice-writing-sql-queries-using-real-dataset
# I did not do all 20, but all of the below yield the same results as the website
# If you are aware of a place with more complex queries that I can test with, please pass it along. 

#1 How many olympics games have been held?
@chain db_table(db, :athlete_events) begin
    @distinct(Games)
    @count
    @collect
end
```
1×1 DataFrame
 Row │ count 
     │ Int64 
─────┼───────
   1 │    51
```

#2  List all Olympics games held so far
@chain db_table(db, :athlete_events) begin
    @distinct(Games, Season, City)
    @collect
end
```
52×3 DataFrame
 Row │ Games        Season  City                   
     │ String       String  String                 
─────┼─────────────────────────────────────────────
   1 │ 1992 Summer  Summer  Barcelona
   2 │ 2012 Summer  Summer  London
   3 │ 1920 Summer  Summer  Antwerpen
   4 │ 1900 Summer  Summer  Paris
   5 │ 1988 Winter  Winter  Calgary
  ⋮  │      ⋮         ⋮               ⋮
  49 │ 1932 Winter  Winter  Lake Placid
  50 │ 1936 Winter  Winter  Garmisch-Partenkirchen
  51 │ 1896 Summer  Summer  Athina
  52 │ 1956 Summer  Summer  Stockholm
                                    43 rows omitted

```

#3  total no of nations who participated in each olympics game?
@chain db_table(db, :athlete_events) begin
    @rename(tf = NOC)
    @left_join(:noc_regions, NOC, tf)
    @distinct(Games, region)
    @group_by(Games)
    @summarize(num_countries=n())
    @collect
end

```
51×2 DataFrame
 Row │ Games        num_countries 
     │ String       Int64         
─────┼────────────────────────────
   1 │ 1896 Summer             12
   2 │ 1900 Summer             31
   3 │ 1904 Summer             14
   4 │ 1906 Summer             20
   5 │ 1908 Summer             22
  ⋮  │      ⋮             ⋮
  48 │ 2010 Winter             81
  49 │ 2012 Summer            204
  50 │ 2014 Winter             88
  51 │ 2016 Summer            205
                   42 rows omitted
```


#5 Which nations have participated in all of the olympic games
@chain db_table(db, :athlete_events) begin
    @rename(noc = NOC)
    @left_join(:noc_regions, NOC, noc)
    @distinct(Games, region)
    @group_by(region)
    @summarize(num_games = n())
    @filter(num_games == 51)
    @collect
end
```
4×2 DataFrame
 Row │ region       num_games 
     │ String       Int64     
─────┼────────────────────────
   1 │ France              51
   2 │ Italy               51
   3 │ Switzerland         51
   4 │ UK                  51
```


#6  Identify the sport which was played in all summer olympics.
@chain db_table(db, :athlete_events) begin
    @filter(Season == "Summer")
    @distinct(Games, Sport)
    @group_by(Sport)
    @summarize(total_games=n())
    @slice_max(total_games)
    @collect
    #@show_query
end
```
5×3 DataFrame
 Row │ Sport       total_games  rank_col 
     │ String      Int64        Int64    
─────┼───────────────────────────────────
   1 │ Athletics            29         1
   2 │ Cycling              29         1
   3 │ Fencing              29         1
   4 │ Gymnastics           29         1
   5 │ Swimming             29         1
```
set_sql_mode(:lite)
# 7 Which Sports were just played only once in the olympics.
@chain db_table(db, :athlete_events) begin
    @distinct(Games, Sport)
    @group_by(Sport)
    @summarize(n=n())
   # @slice_min(n)
    @filter(n == 1) ## alternative 
    @collect
    #@show_query
end
```
10×3 DataFrame
 Row │ Sport                n      rank_col 
     │ String               Int64  Int64    
─────┼──────────────────────────────────────
   1 │ Aeronautics              1         1
   2 │ Basque Pelota            1         1
   3 │ Cricket                  1         1
   4 │ Croquet                  1         1
   5 │ Jeu De Paume             1         1
   6 │ Military Ski Patrol      1         1
   7 │ Motorboating             1         1
   8 │ Racquets                 1         1
   9 │ Roque                    1         1
  10 │ Rugby Sevens             1         1
```

#8 Fetch the total no of sports played in each olympic games.
@chain db_table(db, :athlete_events) begin
    @distinct(Games, Sport)
    @group_by(Games)
    @summarize(n = n())
    @arrange(desc(n))
    @collect
    #@show_query
end

```
51×2 DataFrame
 Row │ Games        n     
     │ String       Int64 
─────┼────────────────────
   1 │ 2016 Summer     34
   2 │ 2008 Summer     34
   3 │ 2004 Summer     34
   4 │ 2000 Summer     34
   5 │ 2012 Summer     32
  ⋮  │      ⋮         ⋮
  48 │ 1952 Winter      8
  49 │ 1936 Winter      8
  50 │ 1928 Winter      8
  51 │ 1932 Winter      7
           42 rows omitted
```
set_sql_mode(:lite)

#9 Fetch oldest athletes to win a gold medal
@chain db_table(db, :athlete_events) begin
    @filter(Medal == "Gold", age != "NA")
    #@mutate(age = missing_if("NA", age))
    @mutate(age_int = as_integer(Age))
    @slice_max(age_int)
    @select(!(Height:City))
    #@show_query
    @collect
end
```
2×17 DataFrame
 Row │ ID      Name               Sex     Age     Height  Weight  Team           NOC     Games        Year   Season  City       Sport     Event                              Medal   age_int  rank_col 
     │ Int64   String             String  String  String  String  String         String  String       Int64  String  String     String    String                             String  Int64    Int64    
─────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   1 │  53238  Charles Jacobus    M       64      NA      NA      United States  USA     1904 Summer   1904  Summer  St. Louis  Roque     Roque Men's Singles                Gold         64         1
   2 │ 117046  Oscar Gomer Swahn  M       64      NA      NA      Sweden         SWE     1912 Summer   1912  Summer  Stockholm  Shooting  Shooting Men's Running Target, S…  Gold         64         1
```

#11 Fetch the top 5 athletes who have won the most gold medals.
@chain db_table(db, :athlete_events) begin
    @filter(Medal == "Gold")    
    @group_by(Name)
    @summarize(numb_gold= n())
    @slice_max(numb_gold, n = 5)
    #@arrange(desc(numb_gold))
   # @filter(numb_gold > 8)
    #@show_query
    @collect
end
```
6×3 DataFrame
 Row │ Name                               numb_gold  rank_col 
     │ String                             Int64      Int64    
─────┼────────────────────────────────────────────────────────
   1 │ Michael Fred Phelps, II                   23         1
   2 │ Raymond Clarence "Ray" Ewry               10         2
   3 │ Paavo Johannes Nurmi                       9         3
   4 │ Mark Andrew Spitz                          9         3
   5 │ Larysa Semenivna Latynina (Diriy…          9         3
   6 │ Frederick Carlton "Carl" Lewis             9         3
```


#12. Fetch the top 5 athletes who have won the most medals (gold/silver/bronze).
@chain db_table(db, :athlete_events) begin
    @filter(Medal != "NA")    
    @group_by(Name)
    @summarize(numb_medals = n())
    @arrange(desc(numb_medals))
    @slice_max(numb_medals, n = 5)
   # @filter(numb_medals > 8)
   # @show_query
    @collect
end

```
7×3 DataFrame
 Row │ Name                               numb_medals  rank_col 
     │ String                             Int64        Int64    
─────┼──────────────────────────────────────────────────────────
   1 │ Michael Fred Phelps, II                     28         1
   2 │ Larysa Semenivna Latynina (Diriy…           18         2
   3 │ Nikolay Yefimovich Andrianov                15         3
   4 │ Takashi Ono                                 13         4
   5 │ Ole Einar Bjrndalen                         13         4
   6 │ Edoardo Mangiarotti                         13         4
   7 │ Borys Anfiyanovych Shakhlin                 13         4
```

#13 Fetch the top 5 most successful countries in olympics. Success is defined by no of medals won.
@chain db_table(db, :athlete_events) begin
    @rename(tf = NOC)
    @mutate(any_medal = if_else(Medal != "NA", 1, 0))
    @left_join(:noc_regions, NOC, tf)
    @group_by(region)
    @summarize(total_medals = sum(any_medal))
    @slice_max(total_medals, n = 5)
    @arrange(desc(total_medals))
    #@show_query
    @collect
end
```
5×3 DataFrame
 Row │ region   total_medals  rank_col 
     │ String   Int64         Int64    
─────┼─────────────────────────────────
   1 │ USA              5637         1
   2 │ Russia           3947         2
   3 │ Germany          3756         3
   4 │ UK               2068         4
   5 │ France           1777         5
```

#14 List  total gold, silver and bronze medals won by each country.
@chain db_table(db, :athlete_events) begin
    @rename(tf = NOC)
    @mutate(Gold = if_else(Medal == "Gold", 1, 0),
               Silver = if_else(Medal == "Silver", 1, 0),
               Bronze = if_else(Medal == "Bronze", 1, 0))
    @left_join(:noc_regions, NOC, tf)
    @group_by(region)
    @summarize(Gold_sum = sum(Gold),
                Silver_sum = sum(Silver),
                 Bronze_sum = sum(Bronze))
    @arrange(desc(Gold_sum))
    #@show_query
    @collect
end
```
207×4 DataFrame
 Row │ region          Gold_sum  Silver_sum  Bronze_sum 
     │ String?         Int64     Int64       Int64      
─────┼──────────────────────────────────────────────────
   1 │ USA                 2638        1641        1358
   2 │ Russia              1599        1170        1178
   3 │ Germany             1301        1195        1260
   4 │ UK                   678         739         651
   5 │ Italy                575         531         531
  ⋮  │       ⋮            ⋮          ⋮           ⋮
 204 │ Andorra                0           0           0
 205 │ American Samoa         0           0           0
 206 │ Albania                0           0           0
 207 │ Afghanistan            0           0           2
                                        198 rows omitted
```


#15 List  total gold, silver and bronze medals won by each country corresponding to each olympic games.
@chain db_table(db, :athlete_events) begin
    @rename(tf = NOC)
    @mutate(Gold = if_else(Medal == "Gold", 1, 0),
               Silver = if_else(Medal == "Silver", 1, 0),
               Bronze = if_else(Medal == "Bronze", 1, 0))
    @left_join(:noc_regions, NOC, tf)
    @group_by(Games, region)
    @summarize(Gold_sum = sum(Gold),
                  Silver_sum = sum(Silver),
                  Bronze_sum = sum(Bronze))
    #@show_query
    #@filter(Games == "1896 Summer")
    @collect
end

```
WITH cte_1 AS (
SELECT ID, Name, Sex, Age, Height, Weight, Team, NOC AS tf, Games, Year, Season, City, Sport, Event, Medal
        FROM athlete_events),
cte_3 AS (
SELECT  *, CASE WHEN Medal == "Gold" THEN 1 ELSE 0 END AS Gold, CASE WHEN Medal == "Silver" THEN 1 ELSE 0 END AS Silver, CASE WHEN Medal == "Bronze" THEN 1 ELSE 0 END AS Bronze
        FROM cte_1),
cte_4 AS (
SELECT  cte_3.*, noc_regions.*
        FROM cte_3
        LEFT JOIN noc_regions ON noc_regions.NOC = cte_3.tf)  
SELECT Games, region, SUM(Gold) AS Gold_sum, SUM(Silver) AS Silver_sum, SUM(Bronze) AS Bronze_sum
        FROM cte_4
        GROUP BY Games, region
```

```
3806×5 DataFrame
  Row │ Games        region                   Gold_sum  Silver_sum  Bronze_sum 
      │ String       String?                  Int64     Int64       Int64      
──────┼────────────────────────────────────────────────────────────────────────
    1 │ 1896 Summer  Australia                       2           0           1
    2 │ 1896 Summer  Austria                         2           1           2
    3 │ 1896 Summer  Denmark                         1           2           3
    4 │ 1896 Summer  France                          5           4           2
    5 │ 1896 Summer  Germany                        25           5           2
  ⋮   │      ⋮                  ⋮                ⋮          ⋮           ⋮
 3803 │ 2016 Summer  Virgin Islands, US              0           0           0
 3804 │ 2016 Summer  Yemen                           0           0           0
 3805 │ 2016 Summer  Zambia                          0           0           0
 3806 │ 2016 Summer  Zimbabwe                        0           0           0
                                                              3797 rows omitted
```
groups = ["region", "Sport"]
#19 In which Sport/event, has India has won the most medals?
@chain db_table(db, :athlete_events) begin
    @rename(tf = NOC)
    @mutate(Gold = if_else(Medal == "Gold", 1, 0),
               Silver = if_else(Medal == "Silver", 1, 0),
               Bronze = if_else(Medal == "Bronze", 1, 0))
    @mutate(total_medals = Gold + Silver + Bronze)
    @left_join(:noc_regions, NOC, tf)
    @group_by(!!groups)
    @summarize(total_sum = sum(total_medals))
    @filter(region == "India" )
    #@mutate(test = total_sum)
    @slice_max(total_sum)
    #@show_query
    @collect
end

```
WITH cte_1 AS (
SELECT ID, Name, Sex, Age, Height, Weight, Team, NOC AS tf, Games, Year, Season, City, Sport, Event, Medal
        FROM athlete_events),
cte_3 AS (
SELECT  *, CASE WHEN Medal == "Gold" THEN 1 ELSE 0 END AS Gold, CASE WHEN Medal == "Silver" THEN 1 ELSE 0 END AS Silver, CASE WHEN Medal == "Bronze" THEN 1 ELSE 0 END AS Bronze
        FROM cte_1),
cte_5 AS (
SELECT  *, Gold + Silver + Bronze AS total_medals
        FROM cte_3),
cte_6 AS (
SELECT  cte_5.*, noc_regions.*
        FROM cte_5
        LEFT JOIN noc_regions ON noc_regions.NOC = cte_5.tf),
cte_7 AS (
SELECT region, Sport, SUM(total_medals) AS total_sum
        FROM cte_6
        GROUP BY region, Sport 
        HAVING region == "India"),
cte_8 AS (
SELECT *, RANK() OVER (
        ORDER BY total_sum DESC) AS rank_col
        FROM cte_7),
cte_9 AS (
SELECT  *
        FROM cte_8
        WHERE rank_col <= 1)  
SELECT *
        FROM cte_9
        GROUP BY region, Sport
```

```
1×4 DataFrame
 Row │ region  Sport   total_sum  rank_col 
     │ String  String  Int64      Int64    
─────┼─────────────────────────────────────
   1 │ India   Hockey        173         1
```

#20
@chain db_table(db, :athlete_events) begin
    @rename(tf = NOC)
    @mutate(Gold = if_else(Medal == "Gold", 1, 0),
               Silver = if_else(Medal == "Silver", 1, 0),
               Bronze = if_else(Medal == "Bronze", 1, 0))#,
    @mutate(total_medals = Gold + Silver + Bronze)
    @left_join(:noc_regions, NOC, tf)
    @group_by(region, Sport, Games)
    @summarize(total_sum = sum(total_medals))
    @filter region == "India" && Sport == "Hockey"
    @arrange(desc(total_sum))
    @collect
   #@show_query
end

```
20×4 DataFrame
 Row │ region  Sport   Games        total_sum 
     │ String  String  String       Int64     
─────┼────────────────────────────────────────
   1 │ India   Hockey  1948 Summer         20
   2 │ India   Hockey  1936 Summer         19
   3 │ India   Hockey  1956 Summer         17
   4 │ India   Hockey  1968 Summer         16
   5 │ India   Hockey  1980 Summer         16
  ⋮  │   ⋮       ⋮          ⋮           ⋮
  17 │ India   Hockey  2000 Summer          0
  18 │ India   Hockey  2004 Summer          0
  19 │ India   Hockey  2012 Summer          0
  20 │ India   Hockey  2016 Summer          0
```