using TidierDB

DuckDB.query(db, "
        CREATE OR REPLACE TABLE df3 (
            id INTEGER,
            pos ROW(lat DOUBLE, lon DOUBLE),
            new ROW(lat2 DOUBLE, lon2 DOUBLE),
            new2 ROW(lat3 DOUBLE, lon3 DOUBLE)
        );
        INSERT INTO df3 VALUES
            (1, ROW(10.1, 30.3), ROW(10.1, 30.3), ROW(10.1, 30.3)),
            (2, ROW(10.2, 30.2), ROW(10.1, 30.3), ROW(10.1, 30.3)),
            (3, ROW(10.3, 30.1), ROW(10.1, 30.3), ROW(10.1, 30.3));");

@chain DB.db_table(db, :df3) begin
    DB.@unnest_wider(pos:new)
    #DB.@unnest_wider(pos, new)
    DB.@collect
end