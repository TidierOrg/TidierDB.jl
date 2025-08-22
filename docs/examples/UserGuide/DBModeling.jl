# The DBModeling extension lets users bring local models to SQL databases.

# This extension is directly inspired by the orbital package in R and Python. 

# It is experimental. 

# The goal the DBModel extension is to a local model to be used on directly on the data in a database. 
# In short, a model that is trained in Julia using MLJ or EvoTrees can be translated to sql and executed on a database.
# Currrently Support Models 
#   - MLJ: Linear and Logistic Regression (proability or class)
#   - EvoTrees for Gradient Boosted Decision Trees 

using TidierDB

db = connect(duckdb())

cards = dt(db, "https://raw.githubusercontent.com/NavjotDS/Heart-Disease-Detection/refs/heads/main/heart_disease_uci.csv")

@chain cards @summary() @collect() _.column_name ,_.null_percentage |> DataFrame

@chain cards begin 
    @drop_missing(id:oldpeak)
    @slice_sample(n = 180) 
    @aside test_data_ids = @select(_, id)
    @create_view test_data true
end

@eval @chain cards begin
    @drop_missing(id:oldpeak)
   # @filter(!(id in ($test_data_ids)))
   # @select(id, ends_with("ismissing"))
   @collect

end
card_df = @collect cards