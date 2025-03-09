package edu.buffalo.cse562.aggregation;

import edu.buffalo.cse562.Table;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A functional implementation of SQL aggregate operations.
 */
public class AggregateOperations {
    private static final int SCALE = 10;
    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;
    
    private final Table table;
    
    public AggregateOperations(Table table) {
        this.table = Objects.requireNonNull(table, "table must not be null");
    }
    
    /**
     * Computes an aggregation on a single column.
     */
    public AggregateResult aggregate(String columnName, AggregateFunction function) {
        int columnIndex = table.getColumnIndex(columnName);
        return aggregate(columnIndex, function);
    }
    
    /**
     * Computes an aggregation on a single column by index.
     */
    public AggregateResult aggregate(int columnIndex, AggregateFunction function) {
        List<String> values = getColumnValues(columnIndex);
        
        switch (function) {
            case SUM:
                return AggregateResult.of(function, sum(values));
            case MIN:
                return AggregateResult.of(function, min(values));
            case MAX:
                return AggregateResult.of(function, max(values));
            case COUNT:
                return AggregateResult.ofCount(count(values));
            case COUNT_DISTINCT:
                return AggregateResult.ofCount(countDistinct(values));
            case AVG:
                return AggregateResult.of(function, average(values));
            default:
                throw new IllegalArgumentException("Unsupported aggregate function: " + function);
        }
    }
    
    /**
     * Performs a GROUP BY operation with aggregations.
     * 
     * @param groupByColumns List of column names to group by
     * @param aggregations List of column name and aggregation function pairs
     * @return The group by result containing all aggregations
     */
    public GroupByResult groupBy(List<String> groupByColumns, List<Map.Entry<String, AggregateFunction>> aggregations) {
        // Get column indices for group by columns
        List<Integer> groupByIndices = groupByColumns.stream()
            .map(table::getColumnIndex)
            .collect(Collectors.toList());
            
        // Get the data from the table
        List<String[]> rows = table.getTuples().stream()
            .map(tuple -> tuple.split("\\|"))
            .collect(Collectors.toList());
            
        // Group the data
        Map<GroupByKey, List<String[]>> groups = rows.stream()
            .collect(Collectors.groupingBy(
                row -> createGroupKey(row, groupByIndices),
                LinkedHashMap::new,
                Collectors.toList()
            ));
            
        // Compute aggregations for each group
        Map<GroupByKey, List<AggregateResult>> results = new LinkedHashMap<>();
        
        groups.forEach((key, groupRows) -> {
            List<AggregateResult> groupResults = new ArrayList<>();
            
            for (Map.Entry<String, AggregateFunction> agg : aggregations) {
                String column = agg.getKey();
                AggregateFunction func = agg.getValue();
                
                int colIndex = table.getColumnIndex(column);
                List<String> colValues = groupRows.stream()
                    .map(row -> row[colIndex])
                    .collect(Collectors.toList());
                    
                // Process the specified aggregate function for the current column values.
                // Each case handles a different type of aggregation and wraps the result
                // in an appropriate AggregateResult instance.
                switch (func) {
                    case SUM:
                        // Calculate sum of numeric values, handling null values appropriately
                        // Uses regular AggregateResult since sum returns a numeric value
                        groupResults.add(AggregateResult.of(func, sum(colValues)));
                        break;
                    case MIN:
                        // Find minimum value in the column, preserving null if all values are null
                        // Uses regular AggregateResult for the numeric minimum
                        groupResults.add(AggregateResult.of(func, min(colValues)));
                        break;
                    case MAX:
                        // Find maximum value in the column, preserving null if all values are null
                        // Uses regular AggregateResult for the numeric maximum
                        groupResults.add(AggregateResult.of(func, max(colValues)));
                        break;
                    case COUNT:
                        // Count all non-null values in the column
                        // Uses special ofCount factory method since counts are always long values
                        groupResults.add(AggregateResult.ofCount(count(colValues)));
                        break;
                    case COUNT_DISTINCT:
                        // Count unique non-null values in the column
                        // Uses special ofCount factory method for the long count result
                        groupResults.add(AggregateResult.ofCount(countDistinct(colValues)));
                        break;
                    case AVG:
                        // Calculate arithmetic mean, handling null values and empty sets
                        // Uses regular AggregateResult since average returns a numeric value
                        groupResults.add(AggregateResult.of(func, average(colValues)));
                        break;
                }
            }
            
            results.put(key, groupResults);
        });
        
        return GroupByResult.of(groupByColumns, results);
    }
    
    /**
     * Backward compatibility method for single aggregation per column.
     * @deprecated Use {@link #groupBy(List, List)} instead
     */
    @Deprecated
    public GroupByResult groupBy(List<String> groupByColumns, Map<String, AggregateFunction> aggregations) {
        List<Map.Entry<String, AggregateFunction>> aggList = aggregations.entrySet().stream()
            .collect(Collectors.toList());
        return groupBy(groupByColumns, aggList);
    }
    
    // Helper methods for aggregations
    
    private BigDecimal sum(List<String> values) {
        return values.stream()
            .map(BigDecimal::new)
            .reduce(BigDecimal.ZERO, BigDecimal::add)
            .setScale(SCALE, ROUNDING_MODE);
    }
    
    private BigDecimal min(List<String> values) {
        return values.stream()
            .map(BigDecimal::new)
            .min(BigDecimal::compareTo)
            .orElse(BigDecimal.ZERO)
            .setScale(SCALE, ROUNDING_MODE);
    }
    
    private BigDecimal max(List<String> values) {
        return values.stream()
            .map(BigDecimal::new)
            .max(BigDecimal::compareTo)
            .orElse(BigDecimal.ZERO)
            .setScale(SCALE, ROUNDING_MODE);
    }
    
    private long count(List<String> values) {
        return values.size();
    }
    
    private long countDistinct(List<String> values) {
        return values.stream().distinct().count();
    }
    
    private BigDecimal average(List<String> values) {
        if (values.isEmpty()) {
            return BigDecimal.ZERO.setScale(SCALE, ROUNDING_MODE);
        }
        
        return sum(values)
            .divide(BigDecimal.valueOf(values.size()), SCALE, ROUNDING_MODE);
    }
    
    private List<String> getColumnValues(int columnIndex) {
        return table.getTuples().stream()
            .map(tuple -> tuple.split("\\|")[columnIndex])
            .collect(Collectors.toList());
    }
    
    private GroupByKey createGroupKey(String[] row, List<Integer> groupByIndices) {
        List<String> keyValues = groupByIndices.stream()
            .map(i -> row[i])
            .collect(Collectors.toList());
        return GroupByKey.of(keyValues);
    }
}