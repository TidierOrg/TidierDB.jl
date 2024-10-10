
type_map = Dict(
    "STRING"    => String,
    "FLOAT"     => Float64,
    "INTEGER"   => Int64,
    "DATE"      => Date,
    "DATETIME"  => DateTime,
    "BOOLEAN"   => Bool,
    "JSON"      => Any  # Map JSON to Any
)

# Function to get Julia type from BigQuery type string
function get_julia_type(type_str::String)
    if startswith(type_str, "ARRAY<") && endswith(type_str, ">")
        element_type_str = type_str[7:end-1]
        element_type = get(type_map, element_type_str, Any)
        return Vector{element_type}
    else
        return get(type_map, type_str, Any)
    end
end

# Helper function to parse scalar values
function parse_scalar_value(x, target_type; type_str="")
    if target_type == Date
        return Date(x)
    elseif target_type == DateTime
        return DateTime(x)
    elseif target_type == String
        return String(x)
    elseif target_type <: Number
        return parse(target_type, x)
    elseif target_type == Bool
        return x in ("true", "1", 1, true)
    elseif type_str == "JSON"
        try
            # Ensure x is a String or Vector{UInt8}
            if isa(x, AbstractString) || isa(x, Vector{UInt8})
                return JSON3.read(x)
            else
                # Convert x to String if possible
                x_str = String(x)
                return JSON3.read(x_str)
            end
        catch e
            println("Failed to parse JSON value '$x' of type $(typeof(x)): ", e)
            return missing
        end
    else
        return convert(target_type, x)
    end
end


# Helper function to parse array elements
function parse_array_elements(x::JSON3.Array, target_type)
    element_type = eltype(target_type)
    return [parse_scalar_value(v["v"], element_type) for v in x]
end

function convert_df_types!(df::DataFrame, new_names::Vector{String}, new_types::Vector{String})
    for (name, type_str) in zip(new_names, new_types)
        # Get the corresponding Julia type
        target_type = get_julia_type(type_str)
        
        # Check if the DataFrame has the column
        if hasproperty(df, name)
            # Get the column data
            column_data = df[!, name]
            
            # Replace `nothing` with `missing`
            column_data = replace(column_data, nothing => missing)
            
            # Check if the data is an array of values
            if !isempty(column_data) && isa(column_data[1], JSON3.Array)
                # Handle arrays
                df[!, name] = [ismissing(x) ? missing : parse_array_elements(x, target_type) for x in column_data]
            else
                # Handle scalar values
                df[!, name] = [ismissing(x) ? missing : parse_scalar_value(x, target_type; type_str=type_str) for x in column_data]
            end
        else
            println("Warning: Column $name not found in DataFrame.")
        end
    end
    return df
end