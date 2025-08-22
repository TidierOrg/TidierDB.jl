# === Tree (raw_tree) → rules → SQL ===
# Works with MLJDecisionTreeInterface + DecisionTree.jl
# Assumes you already have: mach, X::DataFrame, y

# --- tiny helpers ---
_hasfieldrf(T::Type, s::Symbol) = any(==(s), fieldnames(T))
_getrf(o, s::Symbol) = getfield(o, s)
_qidentrf(s::AbstractString) = "\"" * replace(s, "\"" => "\"\"") * "\""

# Split info (feature index and threshold) from a node.
# Tries common names first; otherwise heuristics. Accepts 0-based or 1-based indices.
function _split_info_from_node(n, nfeats::Int)
    T = typeof(n)
    # Named fast path
    for fkey in (:feature, :var, :feat, :feature_i, :feature_idx)
        if _hasfieldrf(T, fkey)
            rawfi = Int(_getrf(n, fkey))
            fi = (1 <= rawfi <= nfeats) ? rawfi :
                 (0 <= rawfi <= nfeats-1 ? rawfi + 1 : nothing)
            if fi !== nothing
                for tkey in (:threshold, :cutoff, :thr, :cut)
                    if _hasfieldrf(T, tkey)
                        thr = float(_getrf(n, tkey))
                        return fi, thr
                    end
                end
            end
        end
    end
    # Heuristic: any in-range integer field as fi + any real (non-integer) as thr
    cand_f = Int[]
    cand_t = Float64[]
    for s in fieldnames(T)
        v = _getrf(n,s)
        v isa Integer && push!(cand_f, Int(v))
        (v isa Real) && !(v isa Integer) && push!(cand_t, float(v))
    end
    for rawfi in cand_f
        fi = (1 <= rawfi <= nfeats) ? rawfi :
             (0 <= rawfi <= nfeats-1 ? rawfi + 1 : nothing)
        fi === nothing && continue
        !isempty(cand_t) && return fi, first(cand_t)
    end
    error("Could not find split info on $(T)")
end

# Extract rules as (conds::Vector{Tuple{Symbol,Float64,Bool}}, label::String, support::Int)
# Each cond is (feature_symbol, threshold, is_left) with is_left=true for "<= thr", false for "> thr".
function extract_tree_rules_from_raw(mach, X::DataFrame, y)
    fp = fitted_params(mach)
    raw = fp.raw_tree
    root = _root_node_from_raw_tree(raw)
    feats = hasproperty(fp, :features) ? Symbol.(fp.features) : Symbol.(propertynames(X))
    nfeats = length(feats)
    n = nrow(X)

    rules = NamedTuple[]
    function walk(node, mask::BitVector, conds::Vector{Tuple{Symbol,Float64,Bool}})
        if _is_leaf_node(node)
            lab = _majority_label(y[mask])
            lab === nothing && (lab = _majority_label(y))
            push!(rules, (conds=copy(conds), label=lab, support=count(mask)))
            return
        end
        fi, thr = _split_info_from_node(node, nfeats)
        kids = _child_nodes_from_node(node)
        length(kids) == 2 || error("Expected binary split; found $(length(kids))")
        col = feats[fi]
        colv = X[!, col]

        # Convention: kids[1] handles "<= thr", kids[2] handles "> thr"
        maskL = mask .& (colv .<= thr)
        push!(conds, (col, thr, true))
        walk(kids[1], maskL, conds)
        pop!(conds)

        maskR = mask .& (colv .>  thr)
        push!(conds, (col, thr, false))
        walk(kids[2], maskR, conds)
        pop!(conds)
    end

    walk(root, trues(n), Tuple{Symbol,Float64,Bool}[])
    return rules
end

# Convert rules to a SQL CASE expression and full SELECT
function rules_to_sql(rules; pred_alias::AbstractString="pred")
    whens = String[]
    for r in rules
        parts = String[]
        for (var, thr, is_left) in r.conds
            op = is_left ? "<=" : ">"
            push!(parts, string(_qidentrf(String(var)), " ", op, " ", thr))
        end
        cond_sql = isempty(parts) ? "1=1" : join(parts, " AND ")
        push!(whens, "WHEN $(cond_sql) THEN '" * replace(r.label, "'" => "''") * "'")
    end
    return "CASE " * join(whens, " ") * " END AS " * _qidentrf(pred_alias)
end

function tree_sql_from_mlj(mach, X, y; table::AbstractString, pred_alias::AbstractString="pred")
    rules = extract_tree_rules_from_raw(mach, X, y)
    case_expr = rules_to_sql(rules; pred_alias=pred_alias)
    return "SELECT *, $case_expr FROM $table"
end

