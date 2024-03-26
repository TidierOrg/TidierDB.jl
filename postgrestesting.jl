
using LibPQ
using Tables
# run libpq on a local docker w this line in terminal. then set up connection
# docker run -d --name postgres -e POSTGRES_PASSWORD=test -p 5432:5432 postgres
conn = LibPQ.Connection("host=localhost port=5432 dbname=postgres user=postgres password=test")


create_table_sql = """
CREATE TABLE athlete_events (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    event_time TIMESTAMP,
    event_end TIMESTAMP,
    int_col1 INT,
    int_col2 INT,
    int_col3 INT,
    grouping VARCHAR(255)
);"""
LibPQ.execute(conn, create_table_sql)

insert_sql = """
INSERT INTO athlete_events (name, event_time, event_end, int_col1, int_col2, int_col3, grouping) VALUES
('Tony Soprano', '2023-01-01 00:53:12', '2023-04-03 00:53:12', 1, 100, 200, 'A'),
('Omar Little', '2023-01-01 01:12:54', '2023-06-01 00:53:12', 2, 101, 201, 'A'),
('Bunk', '2023-01-01 02:12:34', '2023-11-11 00:53:12', 3, 102, 202,  'A'),
('Tony the tiger', '2023-01-01 03:51:43', '2023-01-31 00:53:12', 4, 103, 203, 'A'),
('Lester Freeman', '2023-01-01 04:32:54', '2023-10-21 00:53:12', 5, 104, 204, 'B'),
('Stringer Bell', '2023-01-01 05:12:59', '2023-11-11 00:53:12', 6, 105, 205, 'B'),
('Avon Barksdale', '2023-01-01 06:54:11', '2023-11-21 00:53:12', 7, 106, 206, 'B'),
('Larry David', '2023-01-01 07:35:56', '2023-01-01 00:53:12', 8, 107, 207, 'A'),
('Tony Tony', '2023-01-01 08:23:12', '2023-01-11 00:53:12', 9, 108, 208, 'B'),
('Brother Moves On', '2023-01-01 09:21:12', '2023-01-10 00:53:12', 10, 109, 209, 'A');
"""
LibPQ.execute(conn, insert_sql)
LibPQ.execute(conn, test) |> DataFrame

test = """ 
WITH cte_1 AS (
SELECT  DISTINCT name, event_time, event_end, int_col1, int_col2, int_col3, grouping
        FROM athlete_events)  
SELECT grouping, AVG(int_col1) AS mean_int_col1
        FROM cte_1
        GROUP BY grouping

"""

set_sql_mode(:postgres)
@chain start_query_meta(conn, :athlete_events) begin
    #@distinct 
   # @group_by(grouping)
    @filter(int_col1 >=8)
    #@select(name, event_time, event_end, int_col1)
    #@distinct 
   # @mutate(test2 = int_col1 + 3, test3= as_float(int_col1), floored = floor_date("minute", event_time), new = replace_missing(int_col1, 9))
    #@distinct 
    @group_by(grouping, name)
    #@mutate(name = str_replace_all(name, "Tony", "Tiger"))
    #@mutate(replaced = str_replace_all(name, "Tiger", "ASDFFFF"))
    @summarize(correl = cor(int_col1, int_col2), sd = std(int_col3))
    @filter (grouping = "A")
    #@mutate(sd2 = round(sd))
    #@select(sd:sd2)
   # @slice_max(sd)
    #@filter(sd >2)
    #@show_query
    #@collect
end

@chain start_query_meta(conn, :athlete_events) begin
   # @group_by(groupin)
    @select(starts_with("int"))
    #@mutate(duration = event_end - event_time)
   # @show_query
    @collect
end

@chain start_query_meta(conn, :athlete_events) begin
    #@distinct 
   # @group_by(grouping)
    #@filter(int_col1 >=8)
    @select !id !event_end
    @distinct 
  
    @mutate( int_col2 = int_col2 *3/5)
    @mutate(name = str_replace_all(name, "Tony", "Anthony"), hour = hour(event_time))
   # @select(int_col2)
   # @show_query
    @collect
end

@chain start_query_meta(conn, :athlete_events) begin
  @select !id
  @distinct
  @group_by(grouping)
  @summarize(n=n(), mean = mean(int_col1))
 # @filter9( str_detect(mean, "Tony"), mean>3, grouping == "B")
  #@filter5 (mean > 3)
  @filter(grouping = "B")
#  @collect
  @show_query
end

@chain start_query_meta(conn, :athlete_events) begin
  @select !id
  @distinct
 # @group_by(grouping)
  @summarize(across((starts_with("int")), (mean)))
  @show_query
#  @collect
end

@chain start_query_meta(conn, :athlete_events) begin
  @select !id
  @distinct
  @group_by(grouping)
  @summarize(across((starts_with("int")), (mean)), sd = std(int_col2))
 # @filter9( str_detect(mean, "Tony"), mean>3, grouping == "B")
  #@filter5 (mean > 3)
  #@filter(grouping = "B")
  @collect
# @show_query
end

@chain start_query_meta(conn, :athlete_events) begin
 # @select !id
 # @distinct
  #@group_by(grouping)
  #@summarize(across((starts_with("int")), (mean)), sd = std(int_col2))
  @slice_sample(2)
  # @filter9( str_detect(mean, "Tony"), mean>3, grouping == "B")
  #@filter5 (mean > 3)
  #@filter(grouping = "B")
  @collect
# @show_query
end
