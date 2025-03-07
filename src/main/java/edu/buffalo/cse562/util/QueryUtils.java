package edu.buffalo.cse562.util;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for SQL query processing.
 */
public class QueryUtils {
    private QueryUtils() {
        // Prevent instantiation
    }

    /**
     * Gets all table names referenced in a PlainSelect query.
     *
     * @param plainSelect The query to analyze
     * @return List of table names
     */
    public static List<String> getAllTableNames(PlainSelect plainSelect) {
        List<String> tableNames = new ArrayList<>();
        
        // Add the main table from FROM clause
        addTableName(tableNames, plainSelect.getFromItem());
        
        // Add tables from JOIN clauses
        @SuppressWarnings("unchecked")
        List<Join> joins = plainSelect.getJoins();
        if (joins != null) {
            for (Join join : joins) {
                addTableName(tableNames, join.getRightItem());
            }
        }
        
        return tableNames;
    }

    private static void addTableName(List<String> tableNames, FromItem fromItem) {
        if (fromItem == null) {
            return;
        }

        if (fromItem instanceof SubSelect) {
            // For subselects, recursively get table names
            if (((SubSelect) fromItem).getSelectBody() instanceof PlainSelect) {
                tableNames.addAll(getAllTableNames((PlainSelect) ((SubSelect) fromItem).getSelectBody()));
            }
        } else {
            // For regular tables, add the table name
            String tableName = fromItem.toString().toLowerCase();
            // Handle table aliases (e.g., "table AS alias" -> "table")
            if (tableName.contains(" ")) {
                tableName = tableName.split(" ")[0];
            }
            tableNames.add(tableName);
        }
    }

    /**
     * Checks if a query has any aggregate functions.
     *
     * @param selectItems List of select items from the query
     * @return true if any aggregate functions are found
     */
    public static boolean hasAggregates(List<?> selectItems) {
        if (selectItems == null) {
            return false;
        }
        
        for (Object item : selectItems) {
            String itemStr = item.toString().toLowerCase();
            if (itemStr.contains("sum(") || itemStr.contains("avg(") || 
                itemStr.contains("count(") || itemStr.contains("min(") || 
                itemStr.contains("max(")) {
                return true;
            }
        }
        
        return false;
    }
}