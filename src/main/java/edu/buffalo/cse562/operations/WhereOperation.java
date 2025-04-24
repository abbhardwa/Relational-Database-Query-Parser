package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Implements filtering operations based on WHERE clause expressions.
 */
public class WhereOperation extends AbstractDatabaseOperation {
    private final Expression whereExpression;

    /**
     * Creates a new WhereOperation.
     * 
     * @param whereExpression The WHERE clause expression
     */
    public WhereOperation(Expression whereExpression) {
        this.whereExpression = whereExpression;
    }

    @Override
    public Table execute(Table... inputs) throws IOException {
        if (inputs.length != 1) {
            throw new IllegalArgumentException("Where operation requires exactly one input table");
        }
        
        Table input = inputs[0];
        if (whereExpression == null) {
            return input;
        }
        
        Table resultTable = new Table(input.getTableName() + "_filtered", 
                                    input.getColumnCount(), 
                                    null, 
                                    input.getDataDirectory());
        resultTable.setColumnDefinitions(input.getColumnDefinitions());
        resultTable.populateColumnIndexMap();
        
        ArrayList<String> filteredTuples = new ArrayList<>();
        
        for (String tuple : input.getTuples()) {
            if (evaluateExpression(whereExpression, tuple, input)) {
                filteredTuples.add(tuple);
            }
        }
        
        resultTable.setTuples(filteredTuples);
        return resultTable;
    }
    
    /**
     * Evaluates an expression on a tuple.
     * 
     * @param expression The expression to evaluate
     * @param tuple The tuple to evaluate against
     * @param table The table containing the tuple
     * @return true if the tuple satisfies the expression, false otherwise
     */
    private boolean evaluateExpression(Expression expression, String tuple, Table table) {
        if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            return evaluateExpression(andExpression.getLeftExpression(), tuple, table) && 
                   evaluateExpression(andExpression.getRightExpression(), tuple, table);
        } else if (expression instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) expression;
            return evaluateExpression(orExpression.getLeftExpression(), tuple, table) || 
                   evaluateExpression(orExpression.getRightExpression(), tuple, table);
        } else if (expression instanceof EqualsTo) {
            return evaluateBinaryExpression((EqualsTo) expression, tuple, table, (a, b) -> a.equals(b));
        } else if (expression instanceof NotEqualsTo) {
            return evaluateBinaryExpression((NotEqualsTo) expression, tuple, table, (a, b) -> !a.equals(b));
        } else if (expression instanceof GreaterThan) {
            return evaluateBinaryExpression((GreaterThan) expression, tuple, table, (a, b) -> compareValues(a, b) > 0);
        } else if (expression instanceof GreaterThanEquals) {
            return evaluateBinaryExpression((GreaterThanEquals) expression, tuple, table, (a, b) -> compareValues(a, b) >= 0);
        } else if (expression instanceof MinorThan) {
            return evaluateBinaryExpression((MinorThan) expression, tuple, table, (a, b) -> compareValues(a, b) < 0);
        } else if (expression instanceof MinorThanEquals) {
            return evaluateBinaryExpression((MinorThanEquals) expression, tuple, table, (a, b) -> compareValues(a, b) <= 0);
        } else if (expression instanceof LikeExpression) {
            return evaluateLikeExpression((LikeExpression) expression, tuple, table);
        }
        
        // Default to true for unsupported expressions
        return true;
    }
    
    /**
     * Evaluates a binary expression on a tuple.
     * 
     * @param expression The expression to evaluate
     * @param tuple The tuple to evaluate against
     * @param table The table containing the tuple
     * @param comparator The comparison function
     * @return true if the tuple satisfies the expression, false otherwise
     */
    private boolean evaluateBinaryExpression(BinaryExpression expression, String tuple, Table table, BinaryComparator comparator) {
        String leftValue = getExpressionValue(expression.getLeftExpression(), tuple, table);
        String rightValue = getExpressionValue(expression.getRightExpression(), tuple, table);
        
        if (leftValue == null || rightValue == null) {
            return false;
        }
        
        return comparator.compare(leftValue, rightValue);
    }
    
    /**
     * Evaluates a LIKE expression on a tuple.
     * 
     * @param expression The LIKE expression to evaluate
     * @param tuple The tuple to evaluate against
     * @param table The table containing the tuple
     * @return true if the tuple satisfies the expression, false otherwise
     */
    private boolean evaluateLikeExpression(LikeExpression expression, String tuple, Table table) {
        String leftValue = getExpressionValue(expression.getLeftExpression(), tuple, table);
        String pattern = getExpressionValue(expression.getRightExpression(), tuple, table);
        
        if (leftValue == null || pattern == null) {
            return false;
        }
        
        // Convert SQL LIKE pattern to Java regex
        pattern = pattern.replace("%", ".*").replace("_", ".");
        
        return leftValue.matches(pattern);
    }
    
    /**
     * Gets the value of an expression for a tuple.
     * 
     * @param expression The expression to evaluate
     * @param tuple The tuple to evaluate against
     * @param table The table containing the tuple
     * @return The value of the expression
     */
    private String getExpressionValue(Expression expression, String tuple, Table table) {
        if (expression instanceof Column) {
            Column column = (Column) expression;
            String columnName = column.getColumnName().toLowerCase();
            Integer columnIndex = table.getColumnIndexMap().get(columnName);
            
            if (columnIndex == null) {
                return null;
            }
            
            String[] values = tuple.split("\\|");
            if (columnIndex >= values.length) {
                return null;
            }
            
            return values[columnIndex];
        } else if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        } else if (expression instanceof LongValue) {
            return String.valueOf(((LongValue) expression).getValue());
        }
        
        return expression.toString();
    }
    
    /**
     * Compares two string values.
     * 
     * @param a First value
     * @param b Second value
     * @return Negative if a < b, 0 if a == b, positive if a > b
     */
    private int compareValues(String a, String b) {
        try {
            double numA = Double.parseDouble(a);
            double numB = Double.parseDouble(b);
            return Double.compare(numA, numB);
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }
    
    /**
     * Interface for binary comparison operations.
     */
    private interface BinaryComparator {
        boolean compare(String a, String b);
    }
}