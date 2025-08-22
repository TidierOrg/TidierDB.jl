# === Tree (raw_tree) → rules → SQL ===
# Works with MLJDecisionTreeInterface + DecisionTree.jl
# Assumes you already have: mach, X::DataFrame, y

# --- tiny helpers ---
_hasfield(T::Type, s::Symbol) = any(==(s), fieldnames(T))
_get(o, s::Symbol) = getfield(o, s)
_qident(s::AbstractString) = "\"" * replace(s, "\"" => "\"\"") * "\""

# majority vote label from a subset of y
function _majority_label(y_sub)
    isempty(y_sub) && return nothing
    counts = Dict{String, Int}()
    for v in y_sub
        counts[string(v)] = get(counts, string(v), 0) + 1
    end
    best = maximum(values(counts))
    labs = sort([lab for (lab,c) in counts if c == best])
    return first(labs)
end

# Get the root node from the raw_tree (which DecisionTree.print_tree traverses)
function _root_node_from_raw_tree(raw_tree)
    # Common field where the root lives
    for key in (:root, :node, :tree)
        if _hasfield(typeof(raw_tree), key)
            v = _get(raw_tree, key)
            return v
        end
    end
    # Otherwise scan for any field that "looks like" a Node
    for s in fieldnames(typeof(raw_tree))
        v = _get(raw_tree, s)
        v === nothing && continue
        if occursin("Node", string(typeof(v)))
            return v
        end
        if v isa AbstractVector
            for u in v
                occursin("Node", string(typeof(u))) && return u
            end
        elseif v isa Tuple
            for u in v
                occursin("Node", string(typeof(u))) && return u
            end
        end
    end
    # If the raw_tree itself is a node, return it
    return raw_tree
end

# Child nodes from a node (handles :left/:right, :l/:r, :children/:childs; or generic scan)
function _child_nodes_from_node(n)
    T = typeof(n)
    for (a,b) in ((:left,:right), (:l,:r))
        if _hasfield(T,a) || _hasfield(T,b)
            L = _hasfield(T,a) ? _get(n,a) : nothing
            R = _hasfield(T,b) ? _get(n,b) : nothing
            return filter(!isnothing, (L,R))
        end
    end
    for key in (:children, :childs)
        if _hasfield(T,key)
            v = _get(n,key)
            v isa AbstractVector && return Vector{Any}(v)
            v isa Tuple          && return collect(v)
        end
    end
    # fallback: any fields whose type name contains "Node"
    kids = Any[]
    for s in fieldnames(T)
        v = _get(n,s)
        v === nothing && continue
        tvs = string(typeof(v))
        if occursin("Node", tvs)
            push!(kids, v)
        elseif v isa AbstractVector
            for u in v
                occursin("Node", string(typeof(u))) && push!(kids, u)
            end
        elseif v isa Tuple
            for u in v
                occursin("Node", string(typeof(u))) && push!(kids, u)
            end
        end
    end
    return kids
end

_is_leaf_node(n) = isempty(_child_nodes_from_node(n))

# Split info (feature index and threshold) from a node.

_looks_like_node(x) = x !== nothing &&
    (occursin("Node", string(typeof(x))) || occursin("Leaf", string(typeof(x))))

function _root_node_any(x)
    x === nothing && return nothing
    if _looks_like_node(x)
        return x
    end
    for key in (:root, :node, :tree)
        if hasproperty(x, key)
            r = _root_node_any(getfield(x, key))
            r !== nothing && return r
        end
    end
    for s in fieldnames(typeof(x))
        v = getfield(x, s)
        _looks_like_node(v) && return v
    end
    return nothing
end

function _dt_case_autolabel(fp; pred_alias::AbstractString, quotechar, lowercase_cols)
    enc = hasproperty(fp, :encoding) ? fp.encoding : Dict{UInt8,String}()
    feats = hasproperty(fp, :features) ? String.(fp.features) :
            error("DecisionTree fitted params missing :features")

    # root
    raw  = hasproperty(fp,:raw_tree) ? fp.raw_tree :
           (hasproperty(fp,:tree) ? fp.tree : fp)
    root = _root_node_any(raw)
    root === nothing && error("Could not locate root node")

    nfeats = length(feats)
    whens = String[]
    function walk(node, conds::Vector{String})
        if _is_leaf_node(node)
            lab = _leaf_label_from_node(node, enc)
            cond_sql = isempty(conds) ? "1=1" : join(conds, " AND ")
            push!(whens, "WHEN $cond_sql THEN '" * replace(lab, "'" => "''") * "'")
            return
        end
        fi, thr = _split_info_from_node(node, nfeats)
        l, r = _child_nodes_from_node(node)
        col = _qid(feats[fi], quotechar, lowercase_cols)
        push!(conds, string(col, " <= ", thr)); walk(l, conds); pop!(conds)
        push!(conds, string(col, " > ",  thr)); walk(r, conds); pop!(conds)
    end
    walk(root, String[])
    return "CASE " * join(whens, " ") * " END AS " * _qid(pred_alias, quotechar, lowercase_cols)
end
_qid(s::AbstractString, quotechar, lowercase_cols) = begin
    name = lowercase_cols ? lowercase(s) : s
    quotechar == "" ? "\"" * replace(name, "\"" => "\"\"") * "\"" :
                      string(quotechar, replace(name, quotechar => quotechar*quotechar), quotechar)
end

function _leaf_label_from_node(n, enc)::String
    for s in (:prediction, :majority, :leaf, :class, :label, :target, :leafclass, :k, :value)
        if hasproperty(n, s)
            v = getfield(n, s)
            if v isa UInt8 && !isempty(enc)        ; return string(enc[v]) end
            if v isa Integer && !isempty(enc) && haskey(enc, UInt8(v))
                                                  ; return string(enc[UInt8(v)]) end
            if !(v isa Number)                     ; return string(v) end
        end
    end
    # probabilities/counts → argmax
    for s in (:probabilities, :probs, :p, :counts, :class_counts, :hist)
        if hasproperty(n, s)
            v = getfield(n, s)
            if v isa AbstractVector{<:Real} && !isempty(v)
                k = findmax(v)[2]
                return !isempty(enc) ? string(enc[UInt8(k)]) : string(k)
            end
        end
    end
    return "NA"
end





#### REGRESSION TREE
_fields_regtree(x) = fieldnames(typeof(x))
_has_regtree(x, syms::Tuple) = any(k -> k in _fields_regtree(x), syms)

# A node is a leaf if it lacks left/right fields and carries a leaf-ish payload
function _is_leaf_regtree(n)
    has_lr = _has_regtree(n, (:left, :l, :true, :t)) || _has_regtree(n, (:right, :r, :false, :f))
    return !has_lr && (_has_regtree(n, (:majority, :values, :prediction, :value, :leaf_value)))
end

# Extract the numeric prediction at a leaf
function _leaf_value_regtree(n)
    if :prediction in _fields_regtree(n)
        return Float64(getfield(n, :prediction))
    elseif :value in _fields_regtree(n)
        return Float64(getfield(n, :value))
    elseif :leaf_value in _fields_regtree(n)
        return Float64(getfield(n, :leaf_value))
    elseif :majority in _fields_regtree(n)
        maj = getfield(n, :majority)
        return Float64(maj)
    elseif :values in _fields_regtree(n)
        vals = getfield(n, :values)
        if isa(vals, AbstractVector{<:Real}) && !isempty(vals)
            return Float64(sum(vals) / length(vals))
        elseif isa(vals, AbstractVector) && !isempty(vals) && vals[1] isa Real
            return Float64(vals[1])
        else
            error("Leaf has :values but not numeric; fields = $(_fields_regtree(n))")
        end
    else
        error("Unrecognized leaf; fields = $(_fields_regtree(n))")
    end
end

# Get children safely (returns `nothing` if field absent)
_get_child_regtree(n, keys::Tuple) = begin
    for k in keys
        if k in _fields_regtree(n)
            return getfield(n, k)
        end
    end
    nothing
end

# Extract split parts from a split node
function _split_parts_regtree(n)
    f = nothing
    for k in (:featid, :feature, :var, :feat, :f)
        if k in _fields_regtree(n)
            f = getfield(n, k); break
        end
    end
    f === nothing && error("Split node missing feature id; fields = $(_fields_regtree(n))")

    t = nothing
    for k in (:featval, :threshold, :thresh, :cut, :split)
        if k in _fields_regtree(n)
            t = getfield(n, k); break
        end
    end
    t === nothing && error("Split node missing threshold; fields = $(_fields_regtree(n))")

    left  = _get_child_regtree(n, (:left, :l, :true, :t))
    right = _get_child_regtree(n, (:right, :r, :false, :f))
    (left === nothing || right === nothing) && error("Split node missing children; fields = $(_fields_regtree(n))")

    fidx = Int(f)
    fidx < 1 && (fidx += 1)  # normalize 0-based
    return fidx, Float64(t), left, right
end

# Some wrappers (e.g., Root) directly hold split fields; otherwise dig into obvious holders
function _unwrap_tree_regtree(n)
    if _is_leaf_regtree(n) || _has_regtree(n, (:featid, :feature, :var, :feat, :f)) || _has_regtree(n, (:featval, :threshold, :thresh, :cut, :split))
        return n
    end
    for k in (:tree, :root, :node, :n)
        if k in _fields_regtree(n)
            return _unwrap_tree_regtree(getfield(n, k))
        end
    end
    for k in _fields_regtree(n)
        v = getfield(n, k)
        if v !== nothing && (_is_leaf_regtree(v) || _has_regtree(v, (:featid, :feature, :var, :feat, :f)) || _has_regtree(v, (:featval, :threshold, :thresh, :cut, :split)))
            return _unwrap_tree_regtree(v)
        end
    end
    error("Could not unwrap tree node; fields = $(_fields_regtree(n))")
end

# ------------ SQL generation ------------
function _node_to_sql_regtree(node, fnames::Vector{Symbol}, depth::Int=1)
    n = _unwrap_tree_regtree(node)
    indent = repeat("  ", depth)

    if _is_leaf_regtree(n)
        val = _leaf_value_regtree(n)
        return indent * string(val)
    else
        fidx, thr, left, right = _split_parts_regtree(n)
        1 <= fidx <= length(fnames) || error("Feature index $fidx out of bounds for names $(fnames)")
        col = string(fnames[fidx])

        left_sql  = _node_to_sql_regtree(left,  fnames, depth + 2)
        right_sql = _node_to_sql_regtree(right, fnames, depth + 2)

        return """
$(indent)CASE
$(indent)  WHEN \"$col\" < $(thr) THEN
$left_sql
$(indent)  ELSE
$right_sql
$(indent)END"""
    end
end

function regression_tree_sql_regtree(mach, X; table::AbstractString="iris", pred_alias::AbstractString="predicted_PetalLength")
    fp = fitted_params(mach)
    tree = if :tree in _fields_regtree(fp)
        getfield(fp, :tree)
    elseif :root in _fields_regtree(fp)
        getfield(fp, :root)
    else
        found = nothing
        for k in _fields_regtree(fp)
            v = getfield(fp, k)
            try
                _unwrap_tree_regtree(v)
                found = v
                break
            catch
            end
        end
        found === nothing && error("Could not locate a tree inside fitted_params; fields = $(_fields_regtree(fp))")
        found
    end
    
    fnames = collect(X)
    body = _node_to_sql_regtree(tree, fnames, 1)
    return """
            SELECT *, 
            $body AS \"$pred_alias\"
            FROM $table;"""
end