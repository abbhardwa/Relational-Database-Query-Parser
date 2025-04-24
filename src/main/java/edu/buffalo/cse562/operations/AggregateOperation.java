package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of aggregate operations (COUNT, SUM, AVG, etc.).
 */
public class AggregateOperation extends AbstractDatabaseOperation {
    private final List<SelectExpressionItem> selectItems;
    private final List<Expression> groupByColumns;

    /**
     * Creates a new AggregateOperation.
     * 
     * @param selectItems SELECT items containing aggregate functions
     * @param groupByColumns GROUP BY columns (can be null if no grouping)
     */
    public AggregateOperation(List<SelectExpressionItem> selectItems, List<Expression> groupByColumns) {
        this.selectItems = selectItems;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public Table execute(Table... inputs) {
        if (inputs.length != 1) {
            throw new IllegalArgumentException("Aggregate operation requires exactly one input table");
        }

        Table input = inputs[0];
        
        // If no GROUP BY, perform simple aggregation
        if (groupByColumns == null || groupByColumns.isEmpty()) {
            return executeSimpleAggregation(input);
        }

        // Otherwise delegate to GroupByOperation
        GroupByOperation groupByOp = new GroupByOperation(groupByColumns, selectItems);
        try {
            return groupByOp.execute(input);
        } catch (Exception e) {
            throw new RuntimeException("Error in aggregate operation", e);
        }
    }

    /**
     * Executes a simple aggregation without grouping.
     * 
     * @param input The input table
     * @return The aggregated table
     */
    private Table executeSimpleAggregation(Table input) {
        // Initialize aggregation result arrays
        Map<String, BigDecimal> sums = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();
        Map<String, BigDecimal> mins = new HashMap<>();
        Map<String, BigDecimal> maxs = new HashMap<>();

        // Process each row
        for (String tuple : input.getTuples()) {
            String[] values = tuple.split("\\|");
            
            for (SelectExpressionItem item : selectItems) {
                if (item.getExpression() instanceof Function) {
                    Function func = (Function) item.getExpression();
                    if (func.getParameters() == null || func.getParameters().getExpressions().isEmpty()) {
                        continue;
                    }
                    
                    Expression expr = func.getParameters().getExpressions().get(0);
                    if (!(expr instanceof Column)) {
                        continue;
                    }
                    
                    Column col = (Column) expr;
                    Integer colIndex = input.getColumnIndexMap().get(col.getColumnName().toLowerCase());
                    if (colIndex == null || colIndex >= values.length) {
                        continue;
                    }
                    
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

        for (SelectExpressionItem item : selectItems) {
            if (item.getExpression() instanceof Function) {
                Function func = (Function) item.getExpression();
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
                sb.append(item.getExpression().toString());
            }
            
            sb.append("|");
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1); // Remove last '|'
            resultTuples.add(sb.toString());
        }

        result.setTuples(resultTuples);
        return result;
    }
}