# Leveraging DuckDB, TidierDB works with multiple file types. In most cases, `dt` (or `db_table`) will automatically
# detect the file type to read in the table metadata. 
# > [!NOTE]
# > `dt` does _not_ copy anything into memory beyond the table metadata. 
# A non exhaustive list of file types include:
# - csv, tsv, txt:  `dt(db, "https://file/path/to.csv")`
# - parquet: `dt(db, "or/a/local/file/path/to.parquet")`
# - json: `dt(db, "any/file/path/to.json")`
# - S3 buckets
# - iceberg and delta - require additional args `delta` or `iceberg` to be set to `true`
# - Google Sheets (first run `connect(db, :gsheets)`)
#

# `dt` allso supports directly using any DuckDB file reading function. This allows for easily reading in compressed files
# When reading in a compresssed path, adding an `alias` is recommended. 
# - `dt(db, "read_csv('/Volumes/Untitled/phd_*_genlab.txt', ignore_errors=true)", alias = "genlab")`

# ## File Writing
# TidierDB also supports writing querys to local files via the DuckDB backend with `write_file`, which simply accepts a path with the file type ending
# - `write_file(sql_query, "path/to/lcal/file.parquet")`