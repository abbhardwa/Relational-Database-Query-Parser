package edu.buffalo.cse562;

import edu.buffalo.cse562.aggregation.*;

import java.util.*;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.stream.Collectors;

import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/**
 * Legacy wrapper around the new functional aggregate operations implementation.
 * This class provides backward compatibility for existing code while using the new
 * functional implementation internally.
 */
@Deprecated
public class AggregateOperations {
    private final Table table;
    private final Table ProjectedTable;
    
    @Deprecated
    public AggregateOperations(Table ProjectedTable) {
        this.table = ProjectedTable;
        this.ProjectedTable = ProjectedTable;
    }
    
    /**
     * Legacy method to handle GROUP BY operations.
     * @deprecated Use the new functional API in edu.buffalo.cse562.aggregation package instead.
     */
    @Deprecated
    public LinkedHashMap<String, Object> getGroupBy(Table newTable, 
                                                  String groupBy,
                                                  String columnName,
                                                  int columnNo,
                                                  String aggregateFunc) throws IOException {
        // Convert legacy string groupBy format to list
        String editedGroupBy = groupBy.substring(1, groupBy.lastIndexOf("]"));
        List<String> groupByColumns = Arrays.asList(editedGroupBy.split(","))
            .stream()
            .map(String::trim)
            .map(String::toLowerCase)
            .collect(Collectors.toList());
            
        // Convert legacy string aggregate function to enum
        AggregateFunction function;
        if (aggregateFunc.trim().toLowerCase().contains("sum")) {
            function = AggregateFunction.SUM;
        } else if (aggregateFunc.trim().toLowerCase().contains("min")) {
            function = AggregateFunction.MIN;
        } else if (aggregateFunc.trim().toLowerCase().contains("max")) {
            function = AggregateFunction.MAX;
        } else if (aggregateFunc.trim().toLowerCase().contains("count")) {
            function = aggregateFunc.trim().toLowerCase().contains("distinct") ? 
                AggregateFunction.COUNT_DISTINCT : AggregateFunction.COUNT;
        } else {
            function = AggregateFunction.AVG;
        }
        
        // Create map for aggregation
        Map<String, AggregateFunction> aggregations = new HashMap<>();
        aggregations.put(columnName.toLowerCase(), function);
        
        // Use new implementation
        AggregateOperations newAgg = new edu.buffalo.cse562.aggregation.AggregateOperations(newTable);
        GroupByResult result = newAgg.groupBy(groupByColumns, aggregations);
        
        // Convert back to legacy format
        LinkedHashMap<String, Object> legacyResult = new LinkedHashMap<>();
        result.getResults().forEach((key, aggResults) -> {
            String groupKey = key.toString();
            Object value = aggResults.get(0).getNumericResult()
                .map(v -> (Object)v)
                .orElseGet(() -> aggResults.get(0).getCount().get());
            legacyResult.put(groupKey, value);
        });
        
        return legacyResult;
    }

    /**
     * Legacy method to handle GROUP BY with multiple aggregations.
     * @deprecated Use the new functional API in edu.buffalo.cse562.aggregation package instead.
     */
    @Deprecated
    public Table executeGroupByOperation(Table newTable, String check,
            String[] selectList, String[] orderByList, List<String> arrListResult)
            throws IOException, ParseException {

        if (check != null) {
            Table GroupByTable = new Table("GroupByTable");
            List<String> arrList = new ArrayList<>();

            // Parse the select list to determine aggregations
            Map<String, AggregateFunction> aggregations = new LinkedHashMap<>();
            for (String select : selectList) {
                String columnName;
                String alias = null;
                AggregateFunction function;

                if (select.toLowerCase().contains(" as ")) {
                    String[] parts = select.split("(?i) as ");
                    select = parts[0];
                    alias = parts[1].trim();
                }
                
                if (select.toLowerCase().contains("sum(")) {
                    function = AggregateFunction.SUM;
                    columnName = extractColumnName(select, "sum");
                } else if (select.toLowerCase().contains("min(")) {
                    function = AggregateFunction.MIN;
                    columnName = extractColumnName(select, "min");
                } else if (select.toLowerCase().contains("max(")) {
                    function = AggregateFunction.MAX;
                    columnName = extractColumnName(select, "max");
                } else if (select.toLowerCase().contains("count(")) {
                    function = select.toLowerCase().contains("distinct") ?
                        AggregateFunction.COUNT_DISTINCT : AggregateFunction.COUNT;
                    columnName = extractColumnName(select, "count");
                } else if (select.toLowerCase().contains("avg(")) {
                    function = AggregateFunction.AVG;
                    columnName = extractColumnName(select, "avg");
                } else {
                    continue;
                }
                
                aggregations.put(columnName, function);
                arrList.add(alias != null ? alias : columnName);
            }
            
            // Extract group by columns from check string
            String editedGroupBy = check.substring(1, check.lastIndexOf("]"));
            List<String> groupByColumns = Arrays.asList(editedGroupBy.split(","))
                .stream()
                .map(String::trim)
                .collect(Collectors.toList());
                
            // Use new implementation
            AggregateOperations newAgg = new edu.buffalo.cse562.aggregation.AggregateOperations(newTable);
            GroupByResult result = newAgg.groupBy(groupByColumns, aggregations);
            
            // Convert result to legacy Table format
            Table resultTable = LegacyConverter.toLegacyTable(result, "GroupByTable");
            
            // Handle ORDER BY if present
            if (orderByList != null) {
                return OrderByOperation.orderBy(resultTable, orderByList, orderByList.length);
            }
            
            return resultTable;
        }
        
        return ProjectedTable;
    }

    /**
     * Helper method to extract column name from aggregation expression
     */
    private String extractColumnName(String expr, String funcName) {
        int start = expr.toLowerCase().indexOf(funcName + "(") + funcName.length() + 1;
        int end = expr.indexOf(")");
        return expr.substring(start, end).trim();
    }

    /**
     * Legacy method for LIMIT operation.
     * @deprecated Use stream operations or custom LIMIT implementation instead.
     */
    @Deprecated
    public static void LimitOnTable(Table tableToApplyLimitOn, int tupleLimit) 
            throws IOException {
        tableToApplyLimitOn.getTuples()
            .stream()
            .limit(tupleLimit)
            .forEach(System.out::println);
    }
}