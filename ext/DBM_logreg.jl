
# Wrap a linear predictor SQL expression into probability or class SQL
# mode = :prob or :class; threshold only used for :class
function _logit_wrap(expr_sql::AbstractString; mode::Symbol=:prob, threshold::Float64=0.5,
            pos_label::Union{Int,String}=1, neg_label::Union{Int,String}=0,
            quotechar="")
        # format labels for SQL
        fmtlab(x) = x isa String ? string(quotechar, x, quotechar) : string(x)

        prob = "1.0 / (1.0 + EXP(-($expr_sql)))"

        if mode === :prob
         return prob
        elseif mode === :class
            return "CASE WHEN $prob >= $(threshold) THEN $(fmtlab(pos_label)) ELSE $(fmtlab(neg_label)) END"
        else
        error("Unsupported mode: $mode. Use :prob or :class.")
    end
end

# Build SQL for linear OR logistic; kind = :regression or :logistic
function _generalized_linear_sql(
    obj;
    table::AbstractString,
    pred_alias::AbstractString = "pred",
    quotechar = "",
    lowercase_cols::Bool = true,
    kind::Symbol = :regression,
    mode::Symbol = :prob,
    threshold::Float64 = 0.5,
    pos_label::Union{Int, String} = 1,
    neg_label::Union{Int, String} = 0
)
    p = _extract_params(obj)
    lin, std = p.lin, p.std

    fmtname = s -> begin
        name = String(s)
        name = lowercase_cols ? lowercase(name) : name
        string(quotechar, name, quotechar)
    end
    fmtnum = x -> string(round(float(x); digits=15))

    # If no Standardizer, build linear predictor directly
    if std === nothing
        # linear predictor terms
        terms = [fmtnum(lin.intercept)]
        for (col, w) in sort(collect(lin.coefs); by = x -> string(x[1]))
            push!(terms, "$(fmtname(col)) * $(fmtnum(w))")
        end
        linpred = "(" * join(terms, " + ") * ")"

        out_expr = if kind === :regression
            "$linpred"
        elseif kind === :logistic
            _logit_wrap(
                linpred;
                mode = mode,
                threshold = threshold,
                pos_label = pos_label,
                neg_label = neg_label,
                quotechar = quotechar
            )
        else
            error("Unknown kind: $kind")
        end

        return "SELECT $out_expr AS $(fmtname(pred_alias)) FROM $table"
    end

    # With Standardizer
    sf = _std_fields(std)
    feats, means, stds = sf.feats, sf.means, sf.stds

    inner_terms = String[]
    for (i, f) in enumerate(feats)
        push!(
            inner_terms,
            "($(fmtname(f)) - $(fmtnum(means[i]))) / $(fmtnum(stds[i])) AS $(fmtname(f))"
        )
    end
    inner_sql = "SELECT " * join(inner_terms, ",\n    ") * "\n  FROM $table"

    outer_terms = [fmtnum(lin.intercept)]
    for (col, w) in sort(collect(lin.coefs); by = x -> string(x[1]))
        push!(outer_terms, "$(fmtname(col)) * $(fmtnum(w))")
    end
    linpred = "(" * join(outer_terms, " + ") * ")"

    out_expr = if kind === :regression
        "$linpred"
    elseif kind === :logistic
        _logit_wrap(
            linpred;
            mode = mode,
            threshold = threshold,
            pos_label = pos_label,
            neg_label = neg_label,
            quotechar = quotechar
        )
    else
        error("Unknown kind: $kind")
    end

    return "SELECT $out_expr AS $(fmtname(pred_alias))\nFROM (\n  $inner_sql\n) AS " * fmtname("q01")
end

# Public API for regression remains the same
function predict_db(db, obj; table::AbstractString,
   pred_alias::AbstractString = "pred",
   quotechar = "",
   lowercase_cols::Bool = true)
sql = _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
                 quotechar=quotechar, lowercase_cols=lowercase_cols,
                 kind=:regression, mode=:prob) # mode ignored for regression
return RawSQL(db, sql)
end

function predict_db(obj; table::AbstractString,
   pred_alias::AbstractString = "pred",
   quotechar = "",
   lowercase_cols::Bool = true)
return _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
                  quotechar=quotechar, lowercase_cols=lowercase_cols,
                  kind=:regression, mode=:prob)
end

# New: logistic probability (P(y = pos_label))
function TidierDB.predict_db_logistic_proba(db, obj; table::AbstractString,
                                   pred_alias::AbstractString = ".pred_1",
                                   quotechar = "",
                                   lowercase_cols::Bool = true,
                                   pos_label::Union{Int,String} = 1,
                                   neg_label::Union{Int,String} = 0)
    sql = _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
                                  quotechar=quotechar, lowercase_cols=lowercase_cols,
                                  kind=:logistic, mode=:prob,
                                  pos_label=pos_label, neg_label=neg_label)
    return RawSQL(db, sql)
end

function TidierDB.predict_db_logistic_class(db, obj; table::AbstractString,
                                   pred_alias::AbstractString = ".pred_class",
                                   quotechar = "",
                                   lowercase_cols::Bool = true,
                                   threshold::Float64 = 0.5,
                                   pos_label::Union{Int,String} = 1,
                                   neg_label::Union{Int,String} = 0)
    sql = _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
                                  quotechar=quotechar, lowercase_cols=lowercase_cols,
                                  kind=:logistic, mode=:class, threshold=threshold,
                                  pos_label=pos_label, neg_label=neg_label)
    return RawSQL(db, sql)
end

# Optional: string-only versions (no DB binding)
function predict_db_logistic_proba(obj; table::AbstractString,
                                   pred_alias::AbstractString = ".pred_1",
                                   quotechar = "",
                                   lowercase_cols::Bool = true,
                                   pos_label::Union{Int,String} = 1,
                                   neg_label::Union{Int,String} = 0)
    return _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
                                   quotechar=quotechar, lowercase_cols=lowercase_cols,
                                   kind=:logistic, mode=:prob,
                                   pos_label=pos_label, neg_label=neg_label)
end

function TidierDB.predict_db_logistic_class(obj; table::AbstractString,
                                   pred_alias::AbstractString = ".pred_class",
                                   quotechar = "",
                                   lowercase_cols::Bool = true,
                                   threshold::Float64 = 0.5,
                                   pos_label::Union{Int,String} = 1,
                                   neg_label::Union{Int,String} = 0)
    return _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
                                   quotechar=quotechar, lowercase_cols=lowercase_cols,
                                   kind=:logistic, mode=:class, threshold=threshold,
                                   pos_label=pos_label, neg_label=neg_label)
end


_getfield_any(obj, syms::Tuple) = begin
    for s in syms
        if hasproperty(obj, s)
            return getfield(obj, s)
        end
    end
    nothing
end

_symbolize(k) = k isa Symbol ? k : Symbol(k)

function _normalize_coefs(coefs_raw)
    if coefs_raw isa Dict
        return Dict(_symbolize(k) => float(v) for (k, v) in coefs_raw)
    elseif coefs_raw isa NamedTuple
        return Dict(_symbolize(k) => float(v) for (k, v) in pairs(coefs_raw))
    elseif coefs_raw isa AbstractVector{<:Pair}
        # handles Vector{Pair{Symbol,Float64}} and similar
        return Dict(_symbolize(first(p)) => float(last(p)) for p in coefs_raw)
    elseif coefs_raw isa AbstractVector{<:Tuple} && all(length(t) == 2 for t in coefs_raw)
        # handles Vector{Tuple{name, value}}
        return Dict(_symbolize(t[1]) => float(t[2]) for t in coefs_raw)
    elseif coefs_raw isa AbstractVector{<:Real}
        # no names; can’t build safe SQL
        throw(ArgumentError("Coefficient vector lacks names; cannot build SQL safely."))
    else
        throw(ArgumentError("Unrecognized coefficient container: $(typeof(coefs_raw))"))
    end
end

# extract intercept and coefs from any "linear-like" fitted object
function _icpt_coefs(obj)
    icpt = _getfield_any(obj, (:intercept, :β0, :b0, :bias))
    cofs = _getfield_any(obj, (:coefs, :coef, :β, :betas))
    icpt === nothing && throw(ArgumentError("Missing intercept on fitted model."))
    cofs === nothing && throw(ArgumentError("Missing coefficients on fitted model."))

    icpt = float(icpt)
    cofs = _normalize_coefs(cofs)
    return (intercept = icpt, coefs = cofs)
end


_fmtname(lowercase_cols, quotechar) = (s -> begin
    name = String(s)
    name = lowercase_cols ? lowercase(name) : name
    string(quotechar, name, quotechar)
end)
_fmtnum(x) = string(round(float(x); digits=15))

# Build inner standardization SELECT (or pass-through if no std)
function _build_inner_sql(table, std, fmtname, fmtnum)
    if std === nothing
        return nothing  # means: select directly from table
    end
    sf = _std_fields(std)
    feats, means, stds = sf.feats, sf.means, sf.stds
    inner = String[]
    for (i, f) in enumerate(feats)
        push!(inner, "($(fmtname(f)) - $(fmtnum(means[i]))) / $(fmtnum(stds[i])) AS $(fmtname(f))")
    end
    return "SELECT " * join(inner, ",\n    ") * "\n  FROM $table"
end

# Build the linear predictor Σ (coef_j * x_j) + intercept
function _build_linpred(lin, fmtname, fmtnum)
    terms = [fmtnum(lin.intercept)]
    for (col, w) in sort(collect(lin.coefs); by = x -> string(x[1]))
        push!(terms, "$(fmtname(col)) * $(fmtnum(w))")
    end
    return "(" * join(terms, " + ") * ")"
end

# ---------- fixed “both probs” emitter, orbital-style ----------

function TidierDB.predict_db_logistic_both(db, obj; table::AbstractString,
                                  pred0_alias::AbstractString = "pred_0",
                                  pred1_alias::AbstractString = "pred_1",
                                  quotechar = "",
                                  lowercase_cols::Bool = true)

    p = _extract_params(obj)
    lin, std = p.lin, p.std

    fmtname = _fmtname(lowercase_cols, quotechar)
    fmtnum  = _fmtnum

    inner_sql = _build_inner_sql(table, std, fmtname, fmtnum)
    linpred   = _build_linpred(lin, fmtname, fmtnum)

    # Probabilities; use the same algebra orbital prints
    prob  = "1.0 / (1.0 + EXP(-($linpred)))"  # σ(z)
    pred0 = "1.0 - $prob"   # P(y="0")
    pred1 = "$prob"         # P(y="1")

    select_clause = "SELECT $pred0 AS $pred0_alias,\n       $pred1 AS $pred1_alias\n"

    if inner_sql === nothing
        # no standardization stage
        sql = select_clause * "FROM $table"
    else
        sql = select_clause * "FROM (\n  $inner_sql\n) AS " * fmtname("q01")
    end
    return RawSQL(db, sql)
end

# Replace your DecisionTree detector with this more robust version.
_is_decisiontree_regressor(x) = try
    fp = x isa Machine ? fitted_params(x) : x
    t = hasproperty(fp, :tree) ? getproperty(fp, :tree) :
        hasproperty(fp, :root) ? getproperty(fp, :root) : nothing
    t === nothing && return false
    _unwrap_tree_regtree(t)  # will throw if it's not a DecisionTree-style node
    true
catch
    false
end

function TidierDB.predict_db(db, obj; table::AbstractString,
                             output::Symbol = :auto,           # :auto, :numeric, :prob, :class, :both
                             pred_alias::AbstractString = "pred",
                             pred0_alias::AbstractString = "pred_0",
                             pred1_alias::AbstractString = "pred_1",
                             threshold::Float64 = 0.5,
                             pos_label::Union{Int,String} = "1",
                             neg_label::Union{Int,String} = "0",
                             quotechar = "",
                             lowercase_cols::Bool = true, 
                             feat_names = nothing)

    # unwrap MLJ machine if present
    model = obj isa Machine ? fitted_params(obj) : obj

    # EvoTrees multiclass fast-path
    if _is_evotrees_classifier(model)
        labels = _evotrees_labels(model)
        # if the table's columns are lowercase, lower-case the feature names used by the exporter
        fn_override = lowercase_cols && haskey(model.info, :feature_names) ?
                      lowercase.(String.(model.info[:feature_names])) : nothing

        # normalize desired output mode
        # for multiclass, treat :numeric like :prob (one or more probability columns)
        mode = output === :auto ? :both :
               (output === :numeric ? :prob : output)
        mode in (:prob, :class, :both) ||
            error("For EvoTrees classifiers, output must be :prob, :class, :both, or :auto; got :$output.")

        # build probability SQL
        prob_sql = evotrees_softmax_sql(model; table=table, labels=labels,
                                        feature_names_override=fn_override)

        if mode === :prob
            return RawSQL(db, prob_sql)
        elseif mode === :class
            # wrap subquery and compute argmax label
            subq = "(" * prob_sql * ") AS p"
            pred_case = _sql_argmax_label_expr(pred_alias, labels, "p")
            sql = "SELECT $pred_case FROM $subq"
            return RawSQL(db, sql)
        else # :both
            subq = "(" * prob_sql * ") AS p"
            pred_case = _sql_argmax_label_expr(pred_alias, labels, "p")
            # select predicted class + all probability columns
            cols = ["p.\"$lbl\"" for lbl in labels]
            sql = "SELECT $pred_case, " * join(cols, ", ") * " FROM $subq"
            return RawSQL(db, sql)
        end
    end

    # --- existing logic for linear/logistic models (unchanged) -------------
    kind = _infer_kind(obj)
    mode = output === :auto ? (kind === :logistic ? :both : :numeric) : output
    if obj isa Machine{MLJDecisionTreeInterface.DecisionTreeRegressor, MLJDecisionTreeInterface.DecisionTreeRegressor, true}
        println("HERe")
        sql = regression_tree_sql_regtree(obj, Symbol.(lowercase_cols ? lowercase.(String.(feat_names)) : String.(feat_names)); table=table, pred_alias=pred_alias)
        return RawSQL(db, sql)
    elseif obj isa Machine{MLJDecisionTreeInterface.DecisionTreeClassifier, MLJDecisionTreeInterface.DecisionTreeClassifier, true}
        case_expr = _dt_case_autolabel(model; pred_alias=pred_alias,
                                               quotechar=quotechar, lowercase_cols=lowercase_cols)
        return RawSQL(db, "SELECT *, $case_expr FROM $table")
    end
    if kind === :regression
        sql = _linear_sql(obj; table=table, pred_alias=pred_alias,
                          quotechar=quotechar, lowercase_cols=lowercase_cols)
        return RawSQL(db, sql)
    end

    if mode === :both
        return TidierDB.predict_db_logistic_both(db, obj; table=table,
               pred0_alias=pred0_alias, pred1_alias=pred1_alias,
               quotechar=quotechar, lowercase_cols=lowercase_cols)
    elseif mode === :prob
        sql = _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
              quotechar=quotechar, lowercase_cols=lowercase_cols,
              kind=:logistic, mode=:prob,
              pos_label=pos_label, neg_label=neg_label)
        return RawSQL(db, sql)
    elseif mode === :class
        sql = _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
              quotechar=quotechar, lowercase_cols=lowercase_cols,
              kind=:logistic, mode=:class, threshold=threshold,
              pos_label=pos_label, neg_label=neg_label)
        return RawSQL(db, sql)
    else
        error("Unsupported output=:$(mode). Use :numeric, :prob, :class, :both, or :auto.")
    end
end


#=
function TidierDB.predict_db(db, obj; table::AbstractString,
                             output::Symbol = :auto,           # :auto, :numeric, :prob, :class, :both
                             pred_alias::AbstractString = "pred",
                             pred0_alias::AbstractString = "pred_0",
                             pred1_alias::AbstractString = "pred_1",
                             threshold::Float64 = 0.5,
                             pos_label::Union{Int,String} = "1",
                             neg_label::Union{Int,String} = "0",
                             quotechar = "",
                             lowercase_cols::Bool = true,
                             feat_names::Union{Nothing,Vector{Symbol},Vector{String}} = nothing)

    model = obj isa Machine ? fitted_params(obj) : obj
    # --- DecisionTree / RandomForest auto-dispatch ---
    #=
    if (obj isa Machine) && (hasproperty(model,:raw_tree) || hasproperty(model,:tree) || hasproperty(model,:forest))
        # if X,y provided, use your existing exporters; else, fall back to autolabel (no X/y)
        if hasproperty(model,:forest)
            if feat_names !== nothing 
                sql = rf_sql_from_mlj(obj, feat_names, y; table=table, pred_alias=pred_alias)
                return RawSQL(db, sql)
            else
                case_expr = _rf_vote_case_autolabel(model; pred_alias=pred_alias,
                                                    quotechar=quotechar, lowercase_cols=lowercase_cols)
                return RawSQL(db, "SELECT *, $case_expr FROM $table")
            end
        else
            if feat_names !== nothing #&& y !== nothing
                println("HERE")
                sql = tree_sql_from_mlj(obj, feat_names, "trage"; table=table, pred_alias=pred_alias)
                return RawSQL(db, sql)
            else
                case_expr = _dt_case_autolabel(model; pred_alias=pred_alias,
                                               quotechar=quotechar, lowercase_cols=lowercase_cols)
                return RawSQL(db, "SELECT *, $case_expr FROM $table")
            end
        end
    end 
    =#
    # EvoTrees multiclass fast-path (unchanged)
    if _is_evotrees_classifier(model)
        labels = _evotrees_labels(model)
        fn_override = lowercase_cols && haskey(model.info, :feature_names) ?
                      lowercase.(String.(model.info[:feature_names])) : nothing

        mode = output === :auto ? :both :
               (output === :numeric ? :prob : output)
        mode in (:prob, :class, :both) ||
            error("For EvoTrees classifiers, output must be :prob, :class, :both, or :auto; got :$output.")

        prob_sql = evotrees_softmax_sql(model; table=table, labels=labels,
                                        feature_names_override=fn_override)
        if mode === :prob
            return RawSQL(db, prob_sql)
        elseif mode === :class
            subq = "(" * prob_sql * ") AS p"
            pred_case = _sql_argmax_label_expr(pred_alias, labels, "p")
            sql = "SELECT $pred_case FROM $subq"
            return RawSQL(db, sql)
        else
            subq = "(" * prob_sql * ") AS p"
            pred_case = _sql_argmax_label_expr(pred_alias, labels, "p")
            cols = ["p.\"$lbl\"" for lbl in labels]
            sql = "SELECT $pred_case, " * join(cols, ", ") * " FROM $subq"
            return RawSQL(db, sql)
        end
    end

    kind = _infer_kind(obj)
    mode = output === :auto ? (kind === :logistic ? :both : :numeric) : output
    if obj isa Machine{MLJDecisionTreeInterface.DecisionTreeRegressor, MLJDecisionTreeInterface.DecisionTreeRegressor, true}
        println("HERe")
        sql = regression_tree_sql_regtree(obj, Symbol.(lowercase_cols ? lowercase.(String.(feat_names)) : String.(feat_names)); table=table, pred_alias=pred_alias)
        return RawSQL(db, sql)
    elseif obj isa Machine{MLJDecisionTreeInterface.DecisionTreeClassifier, MLJDecisionTreeInterface.DecisionTreeClassifier, true}
        case_expr = _dt_case_autolabel(model; pred_alias=pred_alias,
                                               quotechar=quotechar, lowercase_cols=lowercase_cols)
        return RawSQL(db, "SELECT *, $case_expr FROM $table")
    end
        #=
    if kind === :regression
        # DecisionTree regressor → nested CASE SQL
        if _is_decisiontree_regressor(model)
            fnames = if feat_names !== nothing
                Symbol.(lowercase_cols ? lowercase.(String.(feat_names)) : String.(feat_names))
            elseif obj isa Machine
                # Minimal, safe requirement: caller provides names to preserve training order
                error("DecisionTree regressor detected. Provide feat_names = names(X) used for training.")
            else
                error("DecisionTree regressor detected, but feature names are unknown. Pass feature_names_override = [:col1, :col2, ...].")
            end
            sql = regression_tree_sql_regtree(obj, fnames; table=table, pred_alias=pred_alias)
            return RawSQL(db, sql)
        end

        # Fallback: linear-style regression (unchanged)
        sql = _linear_sql(obj; table=table, pred_alias=pred_alias,
                          quotechar=quotechar, lowercase_cols=lowercase_cols)
        return RawSQL(db, sql)
    end
    =#

    kind = _infer_kind(obj)
    mode = output === :auto ? (kind === :logistic ? :both : :numeric) : output

    if kind === :regression
        sql = _linear_sql(obj; table=table, pred_alias=pred_alias,
                          quotechar=quotechar, lowercase_cols=lowercase_cols)
        return RawSQL(db, sql)
    end

    if mode === :both
        return TidierDB.predict_db_logistic_both(db, obj; table=table,
               pred0_alias=pred0_alias, pred1_alias=pred1_alias,
               quotechar=quotechar, lowercase_cols=lowercase_cols)
    elseif mode === :prob
        sql = _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
              quotechar=quotechar, lowercase_cols=lowercase_cols,
              kind=:logistic, mode=:prob,
              pos_label=pos_label, neg_label=neg_label)
        return RawSQL(db, sql)
    elseif mode === :class
        sql = _generalized_linear_sql(obj; table=table, pred_alias=pred_alias,
              quotechar=quotechar, lowercase_cols=lowercase_cols,
              kind=:logistic, mode=:class, threshold=threshold,
              pos_label=pos_label, neg_label=neg_label)
        return RawSQL(db, sql)
    else
        error("Unsupported output=:$(mode). Use :numeric, :prob, :class, :both, or :auto.")
    end
end
=#


function _normalize_output(kind::Symbol, output::Symbol)
    # aliases
    output === :regression    && (output = :numeric)
    output === :num           && (output = :numeric)
    output === :classification && (output = :both)

    # :auto defaults
    if output === :auto
        return kind === :logistic ? :both : :numeric
    end

    # kind-specific allowances (+ mapping :numeric -> :prob for logistic)
    if kind === :regression
        if output === :numeric
            return :numeric
        else
            error("For regression models, output must be :numeric (or :auto). Got :$output.")
        end
    else # :logistic
        if output === :numeric
            return :prob  # make :numeric act like "one probability column"
        end
        if output in (:prob, :class, :both)
            return output
        else
            error("For logistic models, output must be :prob, :class, :both, or :auto. Got :$output.")
        end
    end
end

function _infer_kind(obj)
    if obj isa Machine
        T = target_scitype(obj.model)
        if T <: AbstractVector{<:Finite};     return :logistic;    end
        if T <: AbstractVector{<:Continuous};  return :regression;  end
    end
    fp = obj isa Machine ? fitted_params(obj) : obj
    if fp isa NamedTuple
        if hasproperty(fp, :logistic_classifier); return :logistic;   end
        if hasproperty(fp, :linear_regressor);    return :regression; end
    end
    :regression
end
