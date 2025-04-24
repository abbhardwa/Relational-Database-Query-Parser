package edu.buffalo.cse562.util;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.operations.DatabaseOperation;
import edu.buffalo.cse562.operations.SelectionOperation;
import edu.buffalo.cse562.service.QueryExecutionService;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to provide compatibility between old and new code structures.
 * This class bridges the gap between legacy code and the new modular design.
 */
public class CompatibilityHelper {
    private static QueryExecutionService queryService;
    private static final Map<String, Table> tableMap = new HashMap<>();
    
    /**
     * Initializes the compatibility layer with necessary services.
     */
    public static void initialize() {
        if (queryService == null) {
            queryService = new QueryExecutionService();
        }
    }
    
    /**
     * Gets or creates a QueryExecutionService.
     * 
     * @return The QueryExecutionService instance
     */
    public static QueryExecutionService getQueryService() {
        initialize();
        return queryService;
    }
    
    /**
     * Registers a table in both the old and new structures.
     * 
     * @param tableName The name of the table
     * @param table The table to register
     */
    public static void registerTable(String tableName, Table table) {
        tableMap.put(tableName.toLowerCase(), table);
        getQueryService().registerTable(tableName, table);
    }
    
    /**
     * Gets a table by name from the table map.
     * 
     * @param tableName The name of the table to retrieve
     * @return The requested table or null if not found
     */
    public static Table getTable(String tableName) {
        return tableMap.get(tableName.toLowerCase());
    }
    
    /**
     * Executes a query using the appropriate operation.
     * 
     * @param statement The SQL statement to execute
     * @return The result table
     * @throws Exception If execution fails
     */
    public static Table executeQuery(Statement statement) throws Exception {
        if (statement instanceof Select) {
            SelectionOperation operation = new SelectionOperation(statement, new HashMap<>(tableMap));
            return operation.execute();
        } else {
            return getQueryService().executeQuery(statement.toString());
        }
    }
    
    /**
     * Finds a table file in the given directory.
     * Checks for both .dat and .tbl extensions.
     * 
     * @param directory Directory to search in
     * @param tableName Name of the table without extension
     * @return The table file or null if not found
     */
    public static File findTableFile(File directory, String tableName) {
        return FileUtils.findTableFile(directory, tableName);
    }
    
    /**
     * Gets all tables in the registry.
     * 
     * @return Map of table names to tables
     */
    public static Map<String, Table> getAllTables() {
        return new HashMap<>(tableMap);
    }
}