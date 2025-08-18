module DBModel

using TidierDB
using DataFrames
using MLJ
using DuckDB
using EvoTrees
import MLJModelInterface: target_scitype
import ScientificTypesBase: Finite, Continuous
        
__init__() = println("DBModel extension loaded")
include("DBM_decision_tree.jl")
include("DBM_logreg.jl")
# -------------------------
# Internal helpers
# -------------------------
export RawSQL
# Peek model/pipeline to find linear params and optional Standardizer
function _extract_params(obj)
    # 1) Machine -> fitted_params
    if obj isa Machine
        return _extract_params(fitted_params(obj))
    end

    # 2) NamedTuple from fitted_params of a pipeline
    if obj isa NamedTuple
        # Standardizer may be present as :standardizer or :standardizer_model etc.
        std = if hasproperty(obj, :standardizer)
            obj.standardizer
        elseif hasproperty(obj, :standardizer_model)
            obj.standardizer_model
        else
            nothing
        end

        # linear_regressor path (kept for your linear case)
        if hasproperty(obj, :linear_regressor)
            lin_nt = _icpt_coefs(obj.linear_regressor)
            return (lin = lin_nt, std = std)
        end

        # logistic_classifier path (this was the missing case)
        if hasproperty(obj, :logistic_classifier)
            lin_nt = _icpt_coefs(obj.logistic_classifier)
            return (lin = lin_nt, std = std)
        end

        # sometimes the NamedTuple *is already* the coefs/intercept
        if hasproperty(obj, :intercept) && hasproperty(obj, :coefs)
            return (lin = (intercept=float(obj.intercept), coefs=_normalize_coefs(obj.coefs)),
                    std = std)
        end
    end

    # 3) Bare fitted model struct with fields
    if hasproperty(obj, :intercept) && hasproperty(obj, :coefs)
        return (lin = (intercept=float(getfield(obj,:intercept)),
                       coefs=_normalize_coefs(getfield(obj,:coefs))),
                std = nothing)
    end

    error("Unsupported object. Pass an MLJ Machine, fitted_params(mach), or a linear/logistic model with :intercept and :coefs.")
end

# Extract Standardizer fields across MLJModels versions
function _std_fields(std)
    if std === nothing
        return (feats=Symbol[], means=Float64[], stds=Float64[])
    end

    feats = if hasproperty(std, :features_fit)
        Symbol.(getfield(std, :features_fit))
    elseif hasproperty(std, :features)
        Symbol.(getfield(std, :features))
    elseif hasproperty(std, :feature_names)
        Symbol.(getfield(std, :feature_names))
    else
        Symbol[]
    end

    means = if hasproperty(std, :means)
        Float64.(getfield(std, :means))
    elseif hasproperty(std, :μ)
        Float64.(getfield(std, :μ))
    else
        Float64[]
    end

    stds = if hasproperty(std, :stds)
        Float64.(getfield(std, :stds))
    elseif hasproperty(std, :std)
        Float64.(getfield(std, :std))
    elseif hasproperty(std, :σ)
        Float64.(getfield(std, :σ))
    else
        Float64[]
    end

    return (feats=feats, means=means, stds=stds)
end

# Build SQL string (auto: two-stage if Standardizer present; single-stage otherwise)
function _linear_sql(obj; table::AbstractString,
                     pred_alias::AbstractString = "pred",
                     quotechar = "",
                     lowercase_cols::Bool = true)

    p = _extract_params(obj)
    lin, std = p.lin, p.std

    fmtname = s -> begin
        name = String(s)
        name = lowercase_cols ? lowercase(name) : name
        string(quotechar, name, quotechar)
    end
    fmtnum = x -> string(round(float(x); digits=15))

    if std === nothing
        terms = [fmtnum(lin.intercept)]
        for (col, w) in sort(collect(lin.coefs); by = x -> string(x[1]))
            push!(terms, "$(fmtname(col)) * $(fmtnum(w))")
        end
        return "SELECT (" * join(terms, " + ") * ") AS " * fmtname(pred_alias) * " FROM " * table
    end

    sf = _std_fields(std)
    feats, means, stds = sf.feats, sf.means, sf.stds

    inner_terms = String[]
    for (i, f) in enumerate(feats)
        push!(inner_terms, "($(fmtname(f)) - $(fmtnum(means[i]))) / $(fmtnum(stds[i])) AS $(fmtname(f))")
    end
    inner_sql = "SELECT " * join(inner_terms, ",\n    ") * "\n  FROM $table"

    outer_terms = [fmtnum(lin.intercept)]
    for (col, w) in sort(collect(lin.coefs); by = x -> string(x[1]))
        push!(outer_terms, "$(fmtname(col)) * $(fmtnum(w))")
    end
    outer_expr = "(" * join(outer_terms, " + ") * ") AS " * fmtname(pred_alias)

    return "SELECT $outer_expr\nFROM (\n  $inner_sql\n) AS " * fmtname("q01")
end

# -------------------------
# DB-bound RawSQL wrapper
# -------------------------
struct RawSQL
    db
    sql::String
end

# Let TidierDB execute RawSQL via @collect
function TidierDB.final_collect(x::RawSQL, ::Type)
    result = DBInterface.execute(x.db, x.sql)
    return DataFrame(result)
end

# -------------------------
# Public API
# -------------------------
end # module
