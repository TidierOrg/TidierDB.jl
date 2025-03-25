# TidierDB supports working many of the functions in TidierDates, and identical syntax to parts of Dates.jl. 

using TidierDB
dates_df = DataFrame(date_strings = ["2024-01-01", "2025-02-01", "2023-03-01", "2022-04-01"]);
db = connect(duckdb());
dates = dt(db, dates_df, "dates_df");

# TidierDB supports `ymd`, `dmy` and and `mdy` to convert strings in that format to dates.
# - To extract a date part use the name of that date part with in lower case. 
#   - `year(date_col)`
# - To add date intervals, similar to Dates.jl, use the date part but with a capital first letter.
#   - `+ Year(4)`
@chain dates begin 
    @mutate(dates2 = ymd(date_strings) + Month(4) + Year(1) - Day(10))
    @mutate begin 
        months = month(dates2)
        day = day(dates2)
    end
    @filter year(dates2) == 2024 
    @aside @show_query _
    @collect
end



