package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Implements ORDER BY operations on tables.
 */
public class OrderByOperation extends AbstractDatabaseOperation {
    private final List orderByElements;

    /**
     * Creates a new OrderByOperation.
     * 
     * @param orderByElements The ORDER BY elements
     */
    public OrderByOperation(List orderByElements) {
        this.orderByElements = orderByElements;
    }

    @Override
    public Table execute(Table... inputs) throws IOException {
        if (inputs.length != 1) {
            throw new IllegalArgumentException("Order by operation requires exactly one input table");
        }
        
        Table input = inputs[0];
        if (orderByElements == null || orderByElements.isEmpty()) {
            return input;
        }
        
        Table resultTable = new Table(input.getTableName() + "_ordered", 
                                    input.getColumnCount(), 
                                    null, 
                                    input.getDataDirectory());
        resultTable.setColumnDefinitions(input.getColumnDefinitions());
        resultTable.populateColumnIndexMap();
        
        ArrayList<String> tuples = new ArrayList<>(input.getTuples());
        
        // Sort tuples based on ORDER BY elements
        tuples.sort(createComparator(input));
        
        resultTable.setTuples(tuples);
        return resultTable;
    }
    
    /**
     * Creates a comparator based on ORDER BY elements.
     * 
     * @param table The table to sort
     * @return A comparator for sorting tuples
     */
    private Comparator<String> createComparator(Table table) {
        return (tuple1, tuple2) -> {
            String[] values1 = tuple1.split("\\|");
            String[] values2 = tuple2.split("\\|");
            
            for (Object elem : orderByElements) {
                OrderByElement orderByElem = (OrderByElement) elem;
                
                if (!(orderByElem.getExpression() instanceof Column)) {
                    continue;
                }
                
                Column column = (Column) orderByElem.getExpression();
                String columnName = column.getColumnName().toLowerCase();
                Integer columnIndex = table.getColumnIndexMap().get(columnName);
                
                if (columnIndex == null || columnIndex >= values1.length || columnIndex >= values2.length) {
                    continue;
                }
                
                String value1 = values1[columnIndex];
                String value2 = values2[columnIndex];
                
                int comparison;
                try {
                    // Try numeric comparison
                    double num1 = Double.parseDouble(value1);
                    double num2 = Double.parseDouble(value2);
                    comparison = Double.compare(num1, num2);
                } catch (NumberFormatException e) {
                    // Fall back to string comparison
                    comparison = value1.compareTo(value2);
                }
                
                if (comparison != 0) {
                    return orderByElem.isAsc() ? comparison : -comparison;
                }
            }
            
            return 0;
        };
    }
}