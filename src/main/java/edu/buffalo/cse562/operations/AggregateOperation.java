/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of aggregate operations (COUNT, SUM, AVG, etc.).
 */
public class AggregateOperation implements DatabaseOperation {
    private final List<SelectExpressionItem> selectItems;
    private final List<Expression> groupByColumns;

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
        Table result = new Table(input.getTableName() + "_aggregated", 
                               selectItems.size(),
                               null,
                               input.getDataDirectory());

        // If no GROUP BY, perform simple aggregation
        if (groupByColumns == null || groupByColumns.isEmpty()) {
            return executeSimpleAggregation(input);
        }

        // Otherwise do grouped aggregation
        return executeGroupedAggregation(input);
    }

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
                    Column col = (Column) func.getParameters().getExpressions().get(0);
                    int colIndex = input.getColumnIndexMap().get(col.getColumnName().toLowerCase());
                    
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
                                mins.merge(key, value, BigDecimal::min);
                                break;
                            case "max":
                                maxs.merge(key, value, BigDecimal::max);
                                break;
                            case "avg":
                                sums.merge(key, value, BigDecimal::add);
                                counts.merge(key, 1, Integer::sum);
                                break;
                        }
                    } catch (NumberFormatException e) {
                        // Skip non-numeric values
                        continue;
                    }
                }
            }
        }

        // Build result table
        Table result = new Table(input.getTableName() + "_aggregated", selectItems.size(), null, input.getDataDirectory());
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
                        sb.append(sum.divide(new BigDecimal(count), 2, BigDecimal.ROUND_HALF_UP));
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

    private Table executeGroupedAggregation(Table input) {
        // TODO: Implement grouped aggregation
        // This would involve:
        // 1. Building group keys from the groupByColumns
        // 2. Creating separate aggregates for each group
        // 3. Combining results preserving group by columns
        return input;
    }
}