@testset "Compare TidierData and TidierDB" verbose = true begin
    @testset "Select and Tidy Selection" begin
        TDF_1 = @chain test_df @select(contains("e"))
        TDB_1 = @chain DB.t(test_db) DB.@select(contains("e")) DB.@collect
        TDF_2 = @chain test_df @select(id:value)
        TDB_2 = @chain DB.t(test_db) DB.@select(id:value) DB.@collect
        TDF_3 = @chain test_df @select(!(groups:value))
        TDB_3 = @chain DB.t(test_db) DB.@select(!(groups:value)) DB.@collect
        @test all(Array(TDF_1 .== TDB_1))
        @test all(Array(TDF_2 .== TDB_2))
        @test all(Array(TDF_3 .== TDB_3))
    end
    @testset "Group By Summarize" begin
        TDF_1 = @chain test_df @group_by(groups) @summarize(value = sum(value))
        TDB_1 = @chain DB.t(test_db) DB.@group_by(groups) DB.@summarize(value = sum(value)) DB.@collect
        TDF_2 = @chain test_df @group_by(groups) @summarize(across(value,(mean, minimum, maximum)))
        TDB_2 = @chain DB.t(test_db) DB.@group_by(groups) DB.@summarize(across(value, (mean, minimum, maximum))) DB.@collect
        TDF_3 = @chain test_df @group_by(groups) @summarize(across(value,(mean, minimum, maximum))) @mutate(value_mean = value_mean + 4 * 4)
        TDB_3 = @chain DB.t(test_db) DB.@group_by(groups) DB.@summarize(across(value, (mean, minimum, maximum))) DB.@mutate(value_mean = value_mean + 4 * 4) DB.@collect
        @test all(Array(TDF_1 .== TDB_1))
        @test all(Array(TDF_2 .== TDB_2))
        @test all(Array(TDF_3 .== TDB_3))
    end
    @testset "Filter" begin
        TDF_1 = @chain test_df @filter(groups == "aa")
        TDB_1 = @chain DB.t(test_db) DB.@filter(groups == "aa") DB.@collect
        TDF_2 = @chain test_df @filter(groups == "aa" && value > 3)
        TDB_2 = @chain DB.t(test_db) DB.@filter(groups == "aa" && value > 3) DB.@collect
        TDF_3 = @chain test_df @filter(groups == "aa" || value > 3) @arrange(percent)
        TDB_3 = @chain DB.t(test_db) DB.@filter(groups == "aa" || value > 3) DB.@arrange(percent) DB.@collect
        TDF_4 = @chain test_df @filter(!str_detect(id, "F") && value > 3) @arrange(percent)
        TDB_4 = @chain DB.t(test_db) DB.@filter(!str_detect(id, "F") && value > 3) DB.@arrange(percent) DB.@collect
        TDF_5 = @chain test_df @filter(!str_detect(id, "F")) @arrange(percent)
        TDB_5 = @chain DB.t(test_db) DB.@filter(!str_detect(id, "F")) DB.@arrange(percent) DB.@collect
        TDF_6 = @chain test_df @filter( value > percent + 3)
        TDB_6 = @chain DB.t(test_db) DB.@filter(value > percent + 3 ) DB.@collect
        TDF_7 = @chain test_df @filter( starts_with(groups, "a"))
        TDB_7 = @chain DB.t(test_db) DB.@filter(starts_with(groups, "a")) DB.@collect
        TDF_8 = @chain test_df @filter( 3 < value  && value< 5)
        TDB_8 = @chain DB.t(test_db) DB.@filter( 3 < value && value < 5) DB.@collect
        TDF_9 = @chain test_df @filter(id in ["AA", "AI", "AC"]) @arrange(percent)
        TDB_9 = @chain DB.t(test_db) DB.@filter(id in ["AA", "AI", "AC"]) DB.@arrange(percent) DB.@collect
        TDF_10 = @chain test_df @filter( starts_with(groups, "a") || ends_with(id, "C")) @arrange(percent)
        TDB_10 = @chain DB.t(test_db) DB.@filter( starts_with(groups, "a") || ends_with(id, "C")) DB.@arrange(percent) DB.@collect
        TDF_11 = @chain test_df @filter( value >= 3)
        TDB_11 = @chain DB.t(test_db) DB.@filter(value >= 3 ) DB.@collect
        TDF_12 = @chain test_df @filter(id in ["AA", "AI", "AC"]|| value > 3) @arrange(percent)
        TDB_12 = @chain DB.t(test_db) DB.@filter(id in ["AA", "AI", "AC"] || value > 3) DB.@arrange(percent) DB.@collect

        @test all(Array(TDF_1 .== TDB_1))
        @test all(Array(TDF_2 .== TDB_2))
        @test all(Array(TDF_3 .== TDB_3))
        @test all(Array(TDF_4 .== TDB_4))
        @test all(Array(TDF_5 .== TDB_5))
        @test all(Array(TDF_6 .== TDB_6))
        @test all(Array(TDF_6 .== TDB_6))
        @test all(Array(TDF_7 .== TDB_7))
        @test all(Array(TDF_8 .== TDB_8))
        @test all(Array(TDF_9 .== TDB_9))
        @test all(Array(TDF_10 .== TDB_10))
        @test all(Array(TDF_11 .== TDB_11))
        @test all(Array(TDF_12 .== TDB_12))
    end
    @testset "Arrange (Order)" begin
        TDF_1 = @chain test_df @arrange(value, desc(percent))
        TDB_1 = @chain DB.t(test_db) DB.@arrange(value, desc(percent)) DB.@collect
        @test all(Array(TDF_1 .== TDB_1))
    end
    @testset "Joins, Unions, Post Wrangle Joins" begin
        TDF_1 = @left_join(test_df, df2, id = id2)
        TDB_1 = @chain DB.t(test_db) DB.@left_join("df_join", id2, id) DB.@select(!id2) DB.@collect
        query = DB.@chain DB.t(join_db) DB.@filter(score > 85)
        TDF_2 = @left_join(test_df, @filter(df2, score > 85), id = id2)
        TDB_2 = @chain DB.t(test_db) DB.@left_join(DB.t(query), id2, id) DB.@select(!id2) DB.@collect
        query = DB.@chain DB.t(join_db) DB.@filter(str_detect(id2, "C") && score > 85) 
        TDF_3 = @left_join(test_df, @filter(df2, score > 85 && str_detect(id2, "C")), id = id2)
        TDB_3 = @chain DB.t(test_db) DB.@left_join(DB.t(query), id2, id) DB.@select(!id2) DB.@collect
        query = DB.@chain DB.t(test_db) DB.@mutate(value = value *2) DB.@filter(value > 5)
        TDF_4 = @bind_rows(test_df, (@chain test_df @mutate(value = value *2) @filter(value > 5)))
        TDB_4 = @chain DB.t(test_db) DB.@union(DB.t(query)) DB.@collect
        query = DB.@chain DB.t(join_db) DB.@filter(str_detect(id2, "C") && score > 85) 
        TDF_5 = TidierData.@inner_join(test_df, @filter(df2, score > 85 && str_detect(id2, "C")), id = id2)
        TDB_5 = @chain DB.t(test_db) DB.@inner_join(DB.t(query), id2, id) DB.@select(!id2) DB.@collect
        TDF_6 = @semi_join(test_df, @filter(df2, score > 85 && str_detect(id2, "C")), id = id2)
        TDB_6 = @chain DB.t(test_db) DB.@semi_join(DB.t(query), id2, id)  DB.@collect
        TDF_7 = @anti_join(test_df, @filter(df2, score > 85 && str_detect(id2, "C")), id = id2)
        TDB_7 = @chain DB.t(test_db) DB.@anti_join(DB.t(query), id2, id)  DB.@collect
        TDF_8 = @right_join(test_df, @filter(df2, score > 85 && str_detect(id2, "C")), id = id2)
        TDB_8 = @chain DB.t(test_db) DB.@right_join(DB.t(query), id2, id) DB.@select(!id2)  DB.@collect
        @test all(isequal.(Array(TDF_1), Array(TDB_1)))
        @test all(isequal.(Array(TDF_2), Array(TDB_2)))
        @test all(isequal.(Array(TDF_3), Array(TDB_3)))
        @test all(isequal.(Array(TDF_4), Array(TDB_4)))
        @test all(isequal.(Array(TDF_5), Array(TDB_5)))
        @test all(isequal.(Array(TDF_6), Array(TDB_6)))
        @test all(isequal.(Array(TDF_7), Array(TDB_7)))
        @test all(isequal.(Array(TDF_8), Array(TDB_8)))
    end
    @testset "Mutate" begin
        # simple arithmetic mutates
        TDF_1 = @chain test_df @mutate(value = value * 2)
        TDB_1 = @chain DB.t(test_db) DB.@mutate(value = value * 2) DB.@collect
        TDF_2 = @chain test_df @mutate(value = value * 2, percent2 = percent ^ 2 + percent)
        TDB_2 = @chain DB.t(test_db) DB.@mutate(value = value * 2, percent2 = percent ^ 2 + percent) DB.@collect
        # filtering than mutating
        TDF_3 = @chain test_df @filter(groups == "aa" || value > 3) @arrange(percent) @mutate(percent = percent * 10)
        TDB_3 = @chain DB.t(test_db) DB.@filter(groups == "aa" || value > 3) DB.@arrange(percent) DB.@mutate(percent = percent * 10) DB.@collect
        #mutating after summarizing and with cumsum
        TDF_4 = @chain test_df @group_by(groups) @summarize(across(value,(mean, minimum))) @mutate(new = value_mean - value_minimum)
        TDB_4 = @chain DB.t(test_db) DB.@group_by(groups) DB.@summarize(across(value, (mean, minimum))) DB.@mutate(new = value_mean - value_minimum) DB.@collect
        TDF_5 = @chain test_df @group_by(groups) @mutate(value = cumsum(value)) @ungroup() @arrange(desc(groups), value)
        TDB_5 = @chain DB.t(test_db) DB.@group_by(groups) DB.@mutate(value = cumsum(value)) DB.@arrange(desc(groups), value) DB.@collect
        TDF_6 = @chain test_df @mutate(id = lowercase(id), groups = uppercase(groups))
        TDB_6 = @chain DB.t(test_db)  DB.@mutate(id = lower(id), groups = upper(groups)) DB.@collect
        #mutating with agg function across groups, then filtering
        TDF_7 = @chain test_df @group_by(groups) @mutate(min_value = minimum(value)) @ungroup() @filter(value > min_value) @arrange(percent)
        TDB_7 = @chain DB.t(test_db) DB.@group_by(groups) DB.@mutate(min_value = minimum(value)) DB.@filter(value > min_value) DB.@arrange(percent) DB.@collect
        #mutating with string functions
        TDF_8 = @chain test_df @mutate(groups = str_replace_all(groups,"a", "TEST"))
        TDB_8 = @chain DB.t(test_db) DB.@mutate(groups = str_replace_all(groups,"a", "TEST")) DB.@collect
        TDF_8 = @chain test_df @mutate(groups = str_replace(groups,"a", "TEST"))
        TDB_8 = @chain DB.t(test_db) DB.@mutate(groups = str_replace(groups,"a", "TEST")) DB.@collect
        TDF_9 = @chain test_df @mutate(groups = str_remove(groups,"a"))
        TDB_9 = @chain DB.t(test_db) DB.@mutate(groups = str_remove(groups,"a")) DB.@collect
        TDF_9 = @chain test_df @mutate(groups = str_remove_all(groups,"a"))
        TDB_9 = @chain DB.t(test_db) DB.@mutate(groups = str_remove_all(groups,"a")) DB.@collect
        # using missing_if, replace_missing as well as joins
        TDF_10 = @chain test_df @mutate(groups = missing_if(groups,"aa"))
        TDB_10 = @chain DB.t(test_db) DB.@mutate(groups = missing_if(groups,"aa")) DB.@collect
        # full join with mutate and filter in newly joined table
        TDF_11 = @chain test_df @full_join(@filter(df2, score > 85 && str_detect(id2, "C")), id = id2) @mutate(score = replace_missing(score, 0))
        query = DB.@chain DB.t(join_db) DB.@filter(str_detect(id2, "C") && score > 85) 
        TDB_11 = @chain DB.t(test_db) DB.@full_join(DB.t(query), id2, id) DB.@select(!id2) DB.@mutate(score = replace_missing(score, 0)) DB.@collect
        TDF_12 = @chain test_df @mutate(value = value * 2, new_col = (value + percent)/2)
        TDB_12 = @chain DB.t(test_db) DB.@mutate(value = value * 2, new_col = (value + percent)/2) DB.@collect
        # testing as_string, as_float, as_integer
        TDF_13 = @chain test_df @mutate(value = as_string(value)) @mutate(value2 = as_float(value), value3 = as_integer(value)) @filter(value2 > 4 && value3 < 10)
        TDB_13 = @chain DB.t(test_db) DB.@mutate(value = as_string(value)) DB.@mutate(value2 = as_float(value), value3 = as_integer(value)) DB.@filter(value2 > 4 && value3 < 10) DB.@collect
        @test all(isequal.(Array(TDF_1), Array(TDB_1)))
        @test all(isequal.(Array(TDF_2), Array(TDB_2)))
        @test all(isequal.(Array(TDF_3), Array(TDB_3)))
        @test all(isequal.(Array(TDF_4), Array(TDB_4)))
        @test all(isequal.(Array(TDF_5), Array(TDB_5)))
        @test all(isequal.(Array(TDF_6), Array(TDB_6)))
        @test all(isequal.(Array(TDF_7), Array(TDB_7)))
        @test all(isequal.(Array(TDF_8), Array(TDB_8)))
        @test all(isequal.(Array(TDF_9), Array(TDB_9)))
        @test all(isequal.(Array(TDF_10), Array(TDB_10)))
        @test all(isequal.(Array(TDF_11), Array(TDB_11)))
        @test all(isequal.(Array(TDF_12), Array(TDB_12)))
        @test all(isequal.(Array(TDF_13), Array(TDB_13)))

    end
    @testset "Mutate with Conditionals, Strings and then Filter" begin
        # mutating with if_else then filtering on missing values 
        TDF_1 = @chain test_df @mutate(new = if_else(percent > .8, missing, percent)) @arrange(percent) @filter(ismissing(new))
        TBD_1 = @chain DB.t(test_db) DB.@mutate(new = if_else(percent > .8, missing, percent)) DB.@arrange(percent) DB.@filter(ismissing(new)) DB.@collect
        # mutating with case_when then filtering on missing values 
        TDF_2 = @chain test_df @mutate(new = case_when(percent > .8 => "high", percent > .5 => "medium",true => missing)) @arrange(percent) @filter(ismissing(new))
        TBD_2 = @chain DB.t(test_db) DB.@mutate(new = case_when(percent > .8, "high", percent > .5, "medium",true, missing)) DB.@arrange(percent) DB.@filter(ismissing(new)) DB.@collect
        TDF_3 = @chain test_df @mutate(new = case_when(percent > .8 => "high", percent > .5 => "medium",true => missing)) @arrange(percent) @filter(!ismissing(new))
        TBD_3 = @chain DB.t(test_db) DB.@mutate(new = case_when(percent > .8, "high", percent > .5, "medium",true, missing)) DB.@arrange(percent) DB.@filter(!ismissing(new)) DB.@collect
        TDF_4 = @chain test_df @mutate(new = case_when(percent > .8 => "high", percent > .5 => "medium",true => missing)) @arrange(percent) @filter(ismissing(new) && groups == "aa")
        TBD_4 = @chain DB.t(test_db) DB.@mutate(new = case_when(percent > .8, "high", percent > .5, "medium",true, missing)) DB.@arrange(percent) DB.@filter(ismissing(new) && groups == "aa") DB.@collect
        TDF_5 = @chain test_df @mutate(new = case_when(percent > .8 => "high", percent > .5 => "medium",true => missing)) @arrange(percent) @filter(!ismissing(new) && groups == "aa")
        TBD_5 = @chain DB.t(test_db) DB.@mutate(new = case_when(percent > .8, "high", percent > .5, "medium",true, missing)) DB.@arrange(percent) DB.@filter(!ismissing(new) && groups == "aa") DB.@collect
        # using case when with str_detect
        TDF_6 = @chain test_df @mutate(new = case_when(str_detect(id, "F") => "has F", str_detect(id, "C") => "has C", true => missing)) @arrange(percent) @filter(!ismissing(new))
        TBD_6 = @chain DB.t(test_db) DB.@mutate(new = case_when(str_detect(id, "F"), "has F", str_detect(id, "C"), "has C", true, missing)) DB.@arrange(percent) DB.@filter(!ismissing(new)) DB.@collect
        TDF_7 = @chain test_df @mutate(new = if_else(percent > .8, 1.0, percent)) @arrange(percent) @filter(new == 1)
        TBD_7 = @chain DB.t(test_db) DB.@mutate(new = if_else(percent > .8, 1, percent)) DB.@arrange(percent) DB.@filter(new == 1 ) DB.@collect
        @test all(isequal.(Array(TDF_1), Array(TBD_1)))
        @test all(isequal.(Array(TDF_2), Array(TBD_2)))
        @test all(isequal.(Array(TDF_3), Array(TBD_3)))
        @test all(isequal.(Array(TDF_4), Array(TBD_4)))
        @test all(isequal.(Array(TDF_5), Array(TBD_5)))
        @test all(isequal.(Array(TDF_6), Array(TBD_6)))
        @test all(isequal.(Array(TDF_7), Array(TBD_7)))
    end
    @testset "Count" begin
        TDF_1 = @chain test_df @count(groups)
        TBD_1 = @chain DB.t(test_db) DB.@count(groups) DB.@collect
        TDF_2 = @chain test_df @count(groups, id) @arrange(groups, id)
        TBD_2 = @chain DB.t(test_db) DB.@count(groups, id) DB.@arrange(groups, id) DB.@collect
        @test all(isequal.(Array(TDF_1), Array(TBD_1)))
        @test all(isequal.(Array(TDF_2), Array(TBD_2)))
    end
end
