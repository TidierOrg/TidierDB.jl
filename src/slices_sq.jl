"""
$docstring_slice_min
"""
macro slice_min(sqlquery, column, n=1)
    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            cte_name = "cte_" * string(sq.cte_count + 1)

            if sq.is_aggregated
               select_expressions = !isempty(sq.select) ? [sq.select] : ["*"]
               #select_expressions = " * "
               cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
               if sq.is_aggregated && !isempty(sq.groupBy)
                   cte_sql *= " " * sq.groupBy
                   sq.groupBy = " "
               end
               if !isempty(sq.where)
                   cte_sql *= " WHERE " * sq.where
                   sq.where= ""
               end
               if !isempty(sq.having)
                    cte_sql *= "  " * sq.having
                    sq.having = "" 
               end
   
               new_cte = CTE(name=string(cte_name), select=cte_sql)
               push!(sq.ctes, new_cte)
               sq.cte_count += 1
               sq.from = string(cte_name)
            end
            sq.select = "*"
            rank_cte_name = "cte_" * string(sq.cte_count + 1)
            sq.cte_count += 1
           # most_recent_groupBy = ""

            
            #rank_clause = "RANK() OVER (ORDER BY " * $(string(column)) *" ASC) AS rank_col"
            partition_by_clause = !isempty(sq.groupBy) && !sq.is_aggregated ? "PARTITION BY " * sq.groupBy : ""
           # partition_by_clause = isempty(sq.groupBy) && !sq.is_aggregated ? "PARTITION BY " * most_recent_groupBy : ""
            if isempty(sq.groupBy) && !sq.is_aggregated
                for cte in reverse(sq.ctes)
                    if !isempty(cte.groupBy)
                        most_recent_groupBy = cte.groupBy
                       # println(most_recent_groupBy)
                        partition_by_clause =  "PARTITION BY " * most_recent_groupBy

                        break
                    #else 
                    end
                end
              #  partition_by_clause =  "PARTITION BY " * most_recent_groupBy
            else
                nothing
            end
            if !isempty(partition_by_clause) 
                sq.groupBy = ""
            end

            # Update rank_clause to correctly order by column in ASCENDING order for slice_min
            rank_clause = "RANK() OVER (" * partition_by_clause * " ORDER BY " * $(string(column)) * " ASC) AS rank_col"
            # Construct the select clause for the ranking CTE
            select_clause_for_rank = !isempty(sq.select) && sq.select != "*" ?  " " * rank_clause : "*, " * rank_clause
            #select_clause_for_rank = " * " * rank_clause 
            # Create the ranking CTE
            rank_cte = CTE(name=rank_cte_name, select=select_clause_for_rank, from=sq.from)
            push!(sq.ctes, rank_cte)
            
            # Second CTE for applying the WHERE clause based on rank
            sq.cte_count += 1
            filter_cte_name = "cte_" * string(sq.cte_count)
            
            # This CTE selects everything from the previous CTE and applies the WHERE condition
            select_clause_for_filter = " * FROM " * rank_cte_name * " WHERE rank_col <= " * string($n) * " ORDER BY rank_col DESC"
            
            # Create the filter CTE
            filter_cte = CTE(name=filter_cte_name, select=select_clause_for_filter, from=rank_cte_name)
            push!(sq.ctes, filter_cte)
            
            # Update the FROM clause to reference the new filter CTE
            sq.from = filter_cte_name
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_slice_max
"""
macro slice_max(sqlquery, column, n=1)
    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            cte_name = "cte_" * string(sq.cte_count + 1)

            if sq.is_aggregated

               select_expressions = !isempty(sq.select) ? [sq.select] : ["*"]
               #select_expressions = " * "
               cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
               if sq.is_aggregated && !isempty(sq.groupBy)
                   cte_sql *= " " * sq.groupBy
                   sq.groupBy = " "
               end
               if !isempty(sq.where)
                cte_sql *= " WHERE " * sq.where
                sq.where= ""
               end
               if !isempty(sq.having)
                cte_sql *= "  " * sq.having
                sq.having = "" 
                end
   
               # Create and add the new CTE
               new_cte = CTE(name=string(cte_name), select=cte_sql)
               push!(sq.ctes, new_cte)
               sq.cte_count += 1
               sq.from = string(cte_name)
            end
            sq.select = "*"
            rank_cte_name = "cte_" * string(sq.cte_count + 1)
            sq.cte_count += 1
            partition_by_clause = !isempty(sq.groupBy) && !sq.is_aggregated ? "PARTITION BY " * sq.groupBy : ""
           # partition_by_clause = isempty(sq.groupBy) && !sq.is_aggregated ? "PARTITION BY " * most_recent_groupBy : ""
            if isempty(sq.groupBy) && !sq.is_aggregated
                for cte in reverse(sq.ctes)
                    if !isempty(cte.groupBy)
                        most_recent_groupBy = cte.groupBy
                       # println(most_recent_groupBy)
                        partition_by_clause =  "PARTITION BY " * most_recent_groupBy

                        break
                    #else 
                    end
                end
              #  partition_by_clause =  "PARTITION BY " * most_recent_groupBy
            else
                nothing
            end
            if !isempty(partition_by_clause) 
                sq.groupBy = ""
            end

            # Update rank_clause to correctly order by column in ASCENDING order for slice_min
            rank_clause = "RANK() OVER (" * partition_by_clause * " ORDER BY " * $(string(column)) * " DESC) AS rank_col"
            # Construct the ranking window function clause
            #rank_clause = "RANK() OVER (ORDER BY " * $(string(column)) *" ) AS rank_col"
            
            # Construct the select clause for the ranking CTE
            select_clause_for_rank = !isempty(sq.select) && sq.select != "*" ?  " " * rank_clause : "*, " * rank_clause
            #select_clause_for_rank = " * " * rank_clause 
            # Create the ranking CTE
            rank_cte = CTE(name=rank_cte_name, select=select_clause_for_rank, from=sq.from)
            push!(sq.ctes, rank_cte)
            
            # Second CTE for applying the WHERE clause based on rank
            sq.cte_count += 1
            filter_cte_name = "cte_" * string(sq.cte_count)
            
            
            # This CTE selects everything from the previous CTE and applies the WHERE condition
            select_clause_for_filter = " * FROM " * rank_cte_name * " WHERE rank_col <= " * string($n)
            
            # Create the filter CTE
            filter_cte = CTE(name=filter_cte_name, select=select_clause_for_filter, from=rank_cte_name)
            push!(sq.ctes, filter_cte)
            
            # Update the FROM clause to reference the new filter CTE
            sq.from = filter_cte_name
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end

"""
$docstring_slice_sample
"""
macro slice_sample(sqlquery, n=1)
    return quote
        sq = $(esc(sqlquery))
        if isa(sq, SQLQuery)
            cte_name = "cte_" * string(sq.cte_count + 1)

            if sq.is_aggregated
               select_expressions = !isempty(sq.select) ? [sq.select] : ["*"]
               #select_expressions = " * "
               cte_sql = " " * join(select_expressions, ", ") * " FROM " * sq.from
               if sq.is_aggregated && !isempty(sq.groupBy)
                   cte_sql *= " " * sq.groupBy
                   sq.groupBy = " "
               end
               if !isempty(sq.where)
                cte_sql *= " WHERE " * sq.where
                sq.where= ""
               end
               if !isempty(sq.having)
                cte_sql *= "  " * sq.having
                sq.having = "" 
                end
   
               # Create and add the new CTE
               new_cte = CTE(name=string(cte_name), select=cte_sql)
               push!(sq.ctes, new_cte)
               sq.cte_count += 1
               sq.from = string(cte_name)
            end
            sq.select = "*"
            sample_cte_name = "cte_" * string(sq.cte_count + 1)
            sq.cte_count += 1

            partition_by_clause = !isempty(sq.groupBy) ? "PARTITION BY " * sq.groupBy : ""
            if !isempty(partition_by_clause) 
                sq.groupBy = ""
            end
            # Define the sampling clause using RANDOM() for ordering
            sample_clause = "ROW_NUMBER() OVER (" * partition_by_clause * " ORDER BY RANDOM()) AS row_num"

            # Construct the select clause for the sampling CTE
            #select_clause_for_sample = !isempty(sq.select) && sq.select != "*" ? sq.select * ", " * sample_clause : "*," * sample_clause
            select_clause_for_sample = !isempty(sq.select) && sq.select != "*" ?  " " * sample_clause : "*, " * sample_clause

            # Create the sampling CTE
            sample_cte = CTE(name=sample_cte_name, select=select_clause_for_sample, from=sq.from)
            push!(sq.ctes, sample_cte)

            # Construct the final query selecting from the sampling CTE where row numbers are within the sample size
            sq.from = sample_cte_name
            sq.where = "WHERE row_num <= " * string($n)

            # Reset sq.select to select everything from the final CTE
            sq.select = "*"
        else
            error("Expected sqlquery to be an instance of SQLQuery")
        end
        sq
    end
end
