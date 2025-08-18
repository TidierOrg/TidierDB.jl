using EvoTrees
# --- tolerant edge loaders ------------------------------------------------

_to_strings(v) = v isa Vector{String} ? v : String.(collect(v))

# turn one feature's "edges" into Vector{Float64}
_as_floatvec(x) = begin
    if x === nothing || x === missing
        Float64[]
    elseif x isa AbstractVector
        Float64.(collect(x))
    elseif x isa AbstractRange
        Float64.(collect(x))
    elseif x isa Tuple
        Float64.(collect(x))
    elseif x isa NamedTuple
        if hasproperty(x, :edges)
            Float64.(collect(getproperty(x, :edges)))
        elseif hasproperty(x, :cutpoints)
            Float64.(collect(getproperty(x, :cutpoints)))
        else
            error("NamedTuple without :edges/:cutpoints not supported: \$(propertynames(x))")
        end
    else
        error("Unsupported per-feature edge element type: $(typeof(x))")
    end
end

# edges may be Vector{Any}, Vector{<:AbstractVector}, Matrix, or Dict keyed by idx/name
function _load_edges!(model)
    @assert haskey(model.info, :edges) "model.info[:edges] missing"
    raw = model.info[:edges]
    fnames = haskey(model.info, :feature_names) ? _to_strings(model.info[:feature_names]) : String[]
    nfeat = length(fnames)

    if raw isa Vector{<:AbstractVector}
        return [Float64.(collect(e)) for e in raw]
    elseif raw isa Vector{Any}
        return [_as_floatvec(e) for e in raw]
    elseif raw isa Matrix
        return [Float64.(collect(raw[:, j])) for j in axes(raw, 2)]
    elseif raw isa Dict
        # support Dict{Int=>edges}, Dict{String=>edges}, Dict{Symbol=>edges}
        if nfeat == 0
            error("Cannot map Dict edges without model.info[:feature_names].")
        end
        out = Vector{Vector{Float64}}(undef, nfeat)
        if all(k -> k isa Int, keys(raw))
            for (j, _) in enumerate(fnames)
                out[j] = _as_floatvec(get(raw, j, Float64[]))
            end
        elseif all(k -> k isa String, keys(raw))
            for (j, nm) in enumerate(fnames)
                out[j] = _as_floatvec(get(raw, nm, Float64[]))
            end
        elseif all(k -> k isa Symbol, keys(raw))
            for (j, nm) in enumerate(fnames)
                out[j] = _as_floatvec(get(raw, Symbol(nm), Float64[]))
            end
        else
            error("Unsupported Dict key type for :edges: $(eltype(collect(keys(raw))))")
        end
        return out
    else
        error("Unsupported :edges type: $(typeof(raw))")
    end
end

function _load_featbins(model, edges)
    if haskey(model.info, :featbins)
        fb = model.info[:featbins]
        return [Int(fb[j]) for j in 1:length(edges)]
    else
        # nbins per feature is (#edges + 1); keep for sanity checks if you need it
        return [length(e) + 1 for e in edges]
    end
end

# map (feature f, cond_bin b) to exact numeric threshold using edges
_get_thresh(edges::Vector{Vector{Float64}}, f::Int, b::Int) = begin
    if b <= 0
        return (isempty(edges[f]) ? -Inf : first(edges[f]) - eps(Float64))
    else
        @assert b <= length(edges[f]) "bin $b > edges length $(length(edges[f])) for feature $f"
        return edges[f][b]
    end
end


function _make_tree_sql_emitter(tree; feature_names, alias::Union{Nothing,String},
                                edges::Vector{Vector{Float64}}, class_ix::Int)
    feat   = _get(tree, [:feat])        ::Vector{Int}
    split  = _get(tree, [:split])       ::Vector{Bool}
    cond   = _get(tree, [:cond_bin])    # Vector{UInt8}/Int
    pred   = _get(tree, [:pred])        ::Matrix{Float32}

    n_nodes = length(feat)
    @assert size(pred, 2) == n_nodes
    is_leaf_at(i) = !split[i]
    root_idx = 1
    fn = _to_strings(feature_names)
    colref(name::AbstractString) = alias === nothing ? "\"$name\"" : "$(alias).\"$name\""
    left_ix(i) = 2i
    right_ix(i)= 2i+1

    function _emit_pred(i::Int)
        if is_leaf_at(i)
            return string(pred[class_ix, i])
        else
            fidx = feat[i]
            thr  = _get_thresh(edges, fidx, Int(cond[i]))
            lsql = _emit_pred(left_ix(i))
            rsql = _emit_pred(right_ix(i))
            return "(CASE WHEN $(colref(fn[fidx])) <= $(thr) THEN $lsql ELSE $rsql END)"
        end
    end
    return () -> _emit_pred(root_idx)
end

_get(tree, alts::Vector{Symbol}) = begin
    for nm in alts
        if Base.hasfield(typeof(tree), nm)
            return getfield(tree, nm)
        end
    end
    error("None of the expected fields $(alts) exist on $(typeof(tree)). " *
          "Run fieldnames(typeof(tree)) and add the right name.")
end


function evotrees_to_sql_logits(model; labels::Union{Nothing,Vector}=nothing,
                                alias::Union{Nothing,String}=nothing,
                                feature_names_override=nothing)
    lbls = labels === nothing ? _to_strings(model.info[:levels]) : _to_strings(labels)
    K = length(lbls)
    trees = model.trees
    @assert !isempty(trees)
    fnames = feature_names_override === nothing ? _to_strings(model.info[:feature_names]) : _to_strings(feature_names_override)

    edges = _load_edges!(model)
    _ = _load_featbins(model, edges)   # optional sanity

    out = Dict{String,String}()
    for (k, lbl) in enumerate(lbls)
        parts = String[]
        for t in trees
            emit = _make_tree_sql_emitter(t; feature_names=fnames, alias=alias, edges=edges, class_ix=k)
            push!(parts, emit())
        end
        out[lbl] = "(" * join(parts, " + ") * ")"
    end
    return out
end

function evotrees_softmax_sql(model; table::AbstractString="my_table",
                              alias::Union{Nothing,String}=nothing,
                              labels::Union{Nothing,Vector}=nothing,
                              feature_names_override=nothing)
    logits = evotrees_to_sql_logits(model; labels=labels, alias=alias, feature_names_override=feature_names_override)
    labs = collect(keys(logits))
    numer = Dict(l => "EXP($(logits[l]))" for l in labs)
    denom = "(" * join(values(numer), " + ") * ")"
    cols = [ "($(numer[l]) / $denom) AS \"$l\"" for l in labs ]
    fromref = alias === nothing ? table : "$table AS $alias"
    "SELECT " * join(cols, ", ") * " FROM $fromref"
end

_is_evotrees_classifier(obj) = try
    m = obj isa Machine ? fitted_params(obj) : obj
    if (:trees ∈ fieldnames(typeof(m))) && (:info ∈ fieldnames(typeof(m)))
        ts = getfield(m, :trees)
        !isempty(ts) || return false
        first_tree = ts[1]
        # multiclass preds are Matrix{Float32} (K×n_nodes) or (K, n_nodes)
        (:pred ∈ fieldnames(typeof(first_tree))) || return false
        pr = getfield(first_tree, :pred)
        return pr isa AbstractMatrix  # multiclass (e.g., MLogLoss)
    end
    false
catch
    false
end

function _evotrees_labels(model)
    info = model.info
    if haskey(info, :levels)
        return String.(collect(info[:levels]))
    elseif haskey(info, :target_levels)
        return String.(collect(info[:target_levels]))
    else
        error("EvoTrees model info lacks :levels / :target_levels; pass labels explicitly.")
    end
end

# quote SQL string literal safely
_sql_quote_literal(s::AbstractString) = "'" * replace(s, "'" => "''") * "'"

# CASE … END expression that returns the argmax label among the prob cols
function _sql_argmax_label_expr(col_alias::AbstractString, labels::Vector{String}, subq_alias::AbstractString)
    # col_alias is the name to give the predicted-class column (e.g., pred_alias)
    # labels are also the column names in the subquery
    terms = String[]
    for (i, li) in enumerate(labels)
        # li is compared against all others
        conds = String[]
        for (j, lj) in enumerate(labels)
            j == i && continue
            push!(conds, "$(subq_alias).\"$li\" >= $(subq_alias).\"$lj\"")
        end
        push!(terms, "WHEN " * join(conds, " AND ") * " THEN " * _sql_quote_literal(li))
    end
    return "(CASE " * join(terms, " ") * " END) AS \"$col_alias\""
end