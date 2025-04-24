package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements GROUP BY operations with aggregation functions.
 */
public class GroupByOperation extends AbstractDatabaseOperation {
    private final List<Expression> groupByColumns;
    private final List selectItems;

    /**
     * Creates a new GroupByOperation.
     * 
     * @param groupByColumns The GROUP BY columns
     * @param selectItems The SELECT items
     */
    public GroupByOperation(List groupByColumns, List selectItems) {
        this.groupByColumns = groupByColumns;
        this.selectItems = selectItems;
    }

    @Override
    public Table execute(Table... inputs) throws IOException {
        if (inputs.length != 1) {
            throw new IllegalArgumentException("Group by operation requires exactly one input table");
        }
        
        Table input = inputs[0];
        
        // If no GROUP BY, perform simple aggregation
        if (groupByColumns == null || groupByColumns.isEmpty()) {
            return executeSimpleAggregation(input);
        }
        
        // Otherwise do grouped aggregation
        return executeGroupedAggregation(input);
    }
    
    /**
     * Executes a simple aggregation without grouping.
     * 
     * @param input The input table
     * @return The aggregated table
     */
    private Table executeSimpleAggregation(Table input) {
        // Initialize aggregation result maps
        Map<String, BigDecimal> sums = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();
        Map<String, BigDecimal> mins = new HashMap<>();
        Map<String, BigDecimal> maxs = new HashMap<>();
        
        // Process each row
        for (String tuple : input.getTuples()) {
            String[] values = tuple.split("\\|");
            
            for (Object item : selectItems) {
                if (!(item instanceof SelectExpressionItem)) continue;
                
                SelectExpressionItem selectItem = (SelectExpressionItem) item;
                if (selectItem.getExpression() instanceof Function) {
                    Function func = (Function) selectItem.getExpression();
                    if (func.getParameters() == null) continue;
                    
                    Object param = func.getParameters().getExpressions().get(0);
                    if (!(param instanceof Column)) continue;
                    
                    Column col = (Column) param;
                    Integer colIndex = input.getColumnIndexMap().get(col.getColumnName().toLowerCase());
                    if (colIndex == null || colIndex >= values.length) continue;
                    
                    try {
                        BigDecimal value = new BigDecimal(values[colIndex].trim());
                        String key = func.toString();
                        
                        switch (func.getName().toLowerCase()) {
                            case "sum":
                                sums.merge(key, value, BigDecimal::add);
                                break;
                            case "count":
                                counts.merge(key, 1, Integer::sum);
                                break;
                            case "min":
                                if (!mins.containsKey(key) || value.compareTo(mins.get(key)) < 0) {
                                    mins.put(key, value);
                                }
                                break;
                            case "max":
                                if (!maxs.containsKey(key) || value.compareTo(maxs.get(key)) > 0) {
                                    maxs.put(key, value);
                                }
                                break;
                            case "avg":
                                sums.merge(key, value, BigDecimal::add);
                                counts.merge(key, 1, Integer::sum);
                                break;
                        }
                    } catch (NumberFormatException e) {
                        // Skip non-numeric values
                    }
                }
            }
        }
        
        // Build result table
        Table result = new Table(input.getTableName() + "_aggregated", 
                               selectItems.size(), 
                               null, 
                               input.getDataDirectory());
        
        ArrayList<String> resultTuples = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        
        for (Object item : selectItems) {
            if (!(item instanceof SelectExpressionItem)) continue;
            
            SelectExpressionItem selectItem = (SelectExpressionItem) item;
            if (selectItem.getExpression() instanceof Function) {
                Function func = (Function) selectItem.getExpression();
                String key = func.toString();
                
                switch (func.getName().toLowerCase()) {
                    case "sum":
                        sb.append(sums.getOrDefault(key, BigDecimal.ZERO));
                        break;
                    case "count":
                        sb.append(counts.getOrDefault(key, 0));
                        break;
                    case "min":
                        sb.append(mins.getOrDefault(key, BigDecimal.ZERO));
                        break;
                    case "max":
                        sb.append(maxs.getOrDefault(key, BigDecimal.ZERO));
                        break;
                    case "avg":
                        BigDecimal sum = sums.getOrDefault(key, BigDecimal.ZERO);
                        int count = counts.getOrDefault(key, 1);
                        sb.append(sum.divide(new BigDecimal(count), 2, RoundingMode.HALF_UP));
                        break;
                    default:
                        sb.append("0");
                        break;
                }
            } else {
                sb.append(selectItem.getExpression().toString());
            }
            
            sb.append("|");
        }
        
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1); // Remove trailing |
            resultTuples.add(sb.toString());
        }
        
        result.setTuples(resultTuples);
        return result;
    }
    
    /**
     * Executes an aggregation with grouping.
     * 
     * @param input The input table
     * @return The aggregated table
     */
    private Table executeGroupedAggregation(Table input) {
        // Map of group key -> aggregates
        Map<String, Map<String, Object>> groupAggregates = new HashMap<>();
        
        // Process each row
        for (String tuple : input.getTuples()) {
            String[] values = tuple.split("\\|");
            
            // Build group key
            StringBuilder keyBuilder = new StringBuilder();
            for (Expression groupExpr : groupByColumns) {
                if (!(groupExpr instanceof Column)) continue;
                
                Column col = (Column) groupExpr;
                Integer colIndex = input.getColumnIndexMap().get(col.getColumnName().toLowerCase());
                if (colIndex == null || colIndex >= values.length) continue;
                
                keyBuilder.append(values[colIndex]).append("|");
            }
            
            String groupKey = keyBuilder.toString();
            
            // Get or create group aggregate map
            Map<String, Object> aggregates = groupAggregates.computeIfAbsent(groupKey, k -> {
                Map<String, Object> newMap = new HashMap<>();
                newMap.put("_values", new HashMap<String, String>());
                newMap.put("_sums", new HashMap<String, BigDecimal>());
                newMap.put("_counts", new HashMap<String, Integer>());
                newMap.put("_mins", new HashMap<String, BigDecimal>());
                newMap.put("_maxs", new HashMap<String, BigDecimal>());
                return newMap;
            });
            
            // Store group column values
            Map<String, String> groupValues = (Map<String, String>) aggregates.get("_values");
            for (Expression groupExpr : groupByColumns) {
                if (!(groupExpr instanceof Column)) continue;
                
                Column col = (Column) groupExpr;
                Integer colIndex = input.getColumnIndexMap().get(col.getColumnName().toLowerCase());
                if (colIndex == null || colIndex >= values.length) continue;
                
                groupValues.put(col.getColumnName().toLowerCase(), values[colIndex]);
            }
            
            // Process aggregations
            for (Object item : selectItems) {
                if (!(item instanceof SelectExpressionItem)) continue;
                
                SelectExpressionItem selectItem = (SelectExpressionItem) item;
                if (selectItem.getExpression() instanceof Function) {
                    Function func = (Function) selectItem.getExpression();
                    if (func.getParameters() == null) continue;
                    
                    Object param = func.getParameters().getExpressions().get(0);
                    if (!(param instanceof Column)) continue;
                    
                    Column col = (Column) param;
                    Integer colIndex = input.getColumnIndexMap().get(col.getColumnName().toLowerCase());
                    if (colIndex == null || colIndex >= values.length) continue;
                    
                    try {
                        BigDecimal value = new BigDecimal(values[colIndex].trim());
                        String key = func.toString();
                        
                        switch (func.getName().toLowerCase()) {
                            case "sum":
                                Map<String, BigDecimal> sums = (Map<String, BigDecimal>) aggregates.get("_sums");
                                sums.merge(key, value, BigDecimal::add);
                                break;
                            case "count":
                                Map<String, Integer> counts = (Map<String, Integer>) aggregates.get("_counts");
                                counts.merge(key, 1, Integer::sum);
                                break;
                            case "min":
                                Map<String, BigDecimal> mins = (Map<String, BigDecimal>) aggregates.get("_mins");
                                if (!mins.containsKey(key) || value.compareTo(mins.get(key)) < 0) {
                                    mins.put(key, value);
                                }
                                break;
                            case "max":
                                Map<String, BigDecimal> maxs = (Map<String, BigDecimal>) aggregates.get("_maxs");
                                if (!maxs.containsKey(key) || value.compareTo(maxs.get(key)) > 0) {
                                    maxs.put(key, value);
                                }
                                break;
                            case "avg":
                                sums = (Map<String, BigDecimal>) aggregates.get("_sums");
                                sums.merge(key, value, BigDecimal::add);
                                counts = (Map<String, Integer>) aggregates.get("_counts");
                                counts.merge(key, 1, Integer::sum);
                                break;
                        }
                    } catch (NumberFormatException e) {
                        // Skip non-numeric values
                    }
                }
            }
        }
        
        // Build result table
        Table result = new Table(input.getTableName() + "_grouped", 
                               selectItems.size(), 
                               null, 
                               input.getDataDirectory());
        
        ArrayList<String> resultTuples = new ArrayList<>();
        
        // Generate result tuples
        for (Map<String, Object> groupData : groupAggregates.values()) {
            StringBuilder sb = new StringBuilder();
            Map<String, String> groupValues = (Map<String, String>) groupData.get("_values");
            Map<String, BigDecimal> sums = (Map<String, BigDecimal>) groupData.get("_sums");
            Map<String, Integer> counts = (Map<String, Integer>) groupData.get("_counts");
            Map<String, BigDecimal> mins = (Map<String, BigDecimal>) groupData.get("_mins");
            Map<String, BigDecimal> maxs = (Map<String, BigDecimal>) groupData.get("_maxs");
            
            for (Object item : selectItems) {
                if (!(item instanceof SelectExpressionItem)) continue;
                
                SelectExpressionItem selectItem = (SelectExpressionItem) item;
                if (selectItem.getExpression() instanceof Column) {
                    Column col = (Column) selectItem.getExpression();
                    sb.append(groupValues.getOrDefault(col.getColumnName().toLowerCase(), ""));
                } else if (selectItem.getExpression() instanceof Function) {
                    Function func = (Function) selectItem.getExpression();
                    String key = func.toString();
                    
                    switch (func.getName().toLowerCase()) {
                        case "sum":
                            sb.append(sums.getOrDefault(key, BigDecimal.ZERO));
                            break;
                        case "count":
                            sb.append(counts.getOrDefault(key, 0));
                            break;
                        case "min":
                            sb.append(mins.getOrDefault(key, BigDecimal.ZERO));
                            break;
                        case "max":
                            sb.append(maxs.getOrDefault(key, BigDecimal.ZERO));
                            break;
                        case "avg":
                            BigDecimal sum = sums.getOrDefault(key, BigDecimal.ZERO);
                            int count = counts.getOrDefault(key, 1);
                            sb.append(sum.divide(new BigDecimal(count), 2, RoundingMode.HALF_UP));
                            break;
                        default:
                            sb.append("0");
                            break;
                    }
                } else {
                    sb.append(selectItem.getExpression().toString());
                }
                
                sb.append("|");
            }
            
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1); // Remove trailing |
                resultTuples.add(sb.toString());
            }
        }
        
        result.setTuples(resultTuples);
        return result;
    }
}