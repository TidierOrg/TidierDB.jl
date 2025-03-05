# > [Note]
# > unnesting is an experimental feature for TidierDB

# TidierDB now supports unnesting both arrays and structs

using TidierDB
db = connect(duckdb())

DuckDB.query(db, "
        CREATE OR REPLACE TABLE nested_table (
            id INTEGER,
            coord ROW(lat DOUBLE, lon DOUBLE),
            loc ROW(city STRING, country STRING),
            info ROW(continent STRING, climate STRING)
        );
        INSERT INTO nested_table VALUES
            (1, ROW(40.7128, -74.0060), ROW('New York', 'USA'), ROW('North America', 'Temperate')),
            (2, ROW(48.8566, 2.3522), ROW('Paris', 'France'), ROW('Europe', 'Temperate')),
            (3, ROW(35.6895, 139.6917), ROW('Tokyo', 'Japan'), ROW('Asia', 'Humid Subtropical'));");

# ## `@unnest_wider`
# `@unnest_wider` at this time only supports unnesting 
@chain db_table(db, "nested_table") begin
    @unnest_wider(coord:info)
    @collect
end

# Single elements can be extracted a new column like so, or using any of exisiting backend fucntion as well.
@chain db_table(db, "nested_table") begin
    @mutate(city = loc.city)
    @collect
end


DuckDB.query(db, "
    CREATE TABLE nt (
        id INTEGER,
        data ROW(a INTEGER[], b INTEGER[])
        );
    INSERT INTO nt VALUES
        (1, (ARRAY[1,2], ARRAY[3,4])),
        (2, (ARRAY[5,6], ARRAY[7,8,9])),
        (3, (ARRAY[10,11], ARRAY[12,13]));");

# ## `@unnest_longer`
# In this example, we will first `@unnest_wider` data column into 2 columns `a` and `b`, before flattening the arrays within them with `@unnest_longer`
@chain db_table(db, "nt") begin
    @unnest_wider(data)
    @unnest_longer(a, b)
    @collect
end