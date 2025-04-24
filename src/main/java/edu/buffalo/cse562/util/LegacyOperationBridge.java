package edu.buffalo.cse562.util;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.operations.WhereOperation;

import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Bridge between legacy operation classes and new modular implementations.
 * This class provides static methods that bridge the gap between old and new implementations.
 */
public class LegacyOperationBridge {
    
    /**
     * Performs selection operation on a table using a given expression.
     * 
     * @param whereExpression WHERE clause expression
     * @param table Table to filter
     * @return Filtered table
     * @throws IOException If an I/O error occurs
     */
    public static Table selectionOnTable(Expression whereExpression, Table table) throws IOException {
        // Try the new WhereOperation implementation
        try {
            WhereOperation whereOp = new WhereOperation(whereExpression);
            return whereOp.execute(table);
        } catch (Exception e) {
            System.err.println("Warning: Falling back to legacy selection: " + e.getMessage());
            // Fall back to old implementation if needed
            return edu.buffalo.cse562.WhereOperation.selectionOnTable(whereExpression, table);
        }
    }
    
    /**
     * Extracts non-join expressions from a WHERE clause.
     * 
     * @param whereExpression WHERE clause expression
     * @return List of non-join expressions
     */
    public static ArrayList<Expression> extractNonJoinExp(Expression whereExpression) {
        try {
            return edu.buffalo.cse562.WhereOperation.extractNonJoinExp(whereExpression);
        } catch (Exception e) {
            System.err.println("Error extracting non-join expressions: " + e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * Evaluates join condition between two tables.
     * 
     * @param table1 First table
     * @param table2 Second table
     * @param whereExpression WHERE clause expression
     * @return List of join attributes
     */
    public static ArrayList<String> evaluateJoinCondition(Table table1, Table table2, Expression whereExpression) {
        try {
            return edu.buffalo.cse562.WhereOperation.evaluateJoinCondition(table1, table2, whereExpression);
        } catch (Exception e) {
            System.err.println("Error evaluating join condition: " + e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * Performs index selection.
     * 
     * @param expressions List of expressions
     * @param tablesNameAndIndexesMap Map of table names to indexes
     * @param tablesNameAndBTreeMap Map of table names to BTree objects
     * @param isTpch7 Flag for TPCH7 case
     * @param isTpch12 Flag for TPCH12 case
     * @return List of tuples selected by index
     */
    public static ArrayList<String> indexSelection(ArrayList<Expression> expressions, 
                                                 HashMap<String, List> tablesNameAndIndexesMap,
                                                 HashMap<String, HashMap<String, Object>> tablesNameAndBTreeMap,
                                                 boolean isTpch7,
                                                 boolean isTpch12) {
        try {
            return edu.buffalo.cse562.WhereOperation.indexSelection(
                expressions, 
                tablesNameAndIndexesMap, 
                tablesNameAndBTreeMap,
                isTpch7,
                isTpch12
            );
        } catch (Exception e) {
            System.err.println("Error in index selection: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Performs index update operation.
     * 
     * @param expressions List of expressions
     * @param tablesNameAndIndexesMap Map of table names to indexes
     * @param tablesNameAndBTreeMap Map of table names to BTree objects
     * @param columnIndex Index of the column to update
     * @param newValue New value for column
     * @param tableName Name of the table
     * @return True if update successful, false otherwise
     */
    public static boolean indexUpdation(ArrayList<Expression> expressions,
                                      HashMap<String, List> tablesNameAndIndexesMap,
                                      HashMap<String, HashMap<String, Object>> tablesNameAndBTreeMap,
                                      int columnIndex,
                                      String newValue,
                                      String tableName) {
        try {
            edu.buffalo.cse562.WhereOperation.indexUpdation(
                expressions, 
                tablesNameAndIndexesMap, 
                tablesNameAndBTreeMap,
                columnIndex,
                newValue,
                tableName
            );
            return true;
        } catch (Exception e) {
            System.err.println("Error in index updation: " + e.getMessage());
            return false;
        }
    }
}
