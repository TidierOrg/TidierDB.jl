using DuckDB
conxn = open(":memory:")

db = connect(conxn)
copy_to(db, "https://raw.githubusercontent.com/rahkum96/Olympics-Dataset-by-sql/main/noc_regions.csv" "noc_regions")
copy_to(db, "https://raw.githubusercontent.com/rahkum96/Olympics-Dataset-by-sql/main/olympic_event.csv", "athlete_events")

@chain start_query_meta(con, :athlete_events) begin
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

@chain start_query_meta(con, :athlete_events) begin
    @group_by(Games)
    @mutate(test = minimum(Year))
    @select (test)
    @collect
    #@show_query
end

@chain start_query_meta(con, :athlete_events) begin
    @group_by(Games)
    @summarise(test = maximum(Year))
    @show_query
end