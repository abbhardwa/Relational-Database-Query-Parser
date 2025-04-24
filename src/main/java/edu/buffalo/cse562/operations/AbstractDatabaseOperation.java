package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;

import java.io.IOException;
import java.util.HashMap;

/**
 * Abstract base class for database operations.
 * Provides common functionality for all operations.
 */
public abstract class AbstractDatabaseOperation implements DatabaseOperation {
    protected final HashMap<String, Table> tableMap;
    
    /**
     * Creates a new AbstractDatabaseOperation.
     */
    public AbstractDatabaseOperation() {
        this.tableMap = new HashMap<>();
    }
    
    /**
     * Creates a new AbstractDatabaseOperation with a table map.
     * 
     * @param tableMap Map of table names to Table objects
     */
    public AbstractDatabaseOperation(HashMap<String, Table> tableMap) {
        this.tableMap = tableMap;
    }
    
    @Override
    public abstract Table execute(Table... inputs) throws IOException;
    
    /**
     * Creates a deep copy of a table.
     * 
     * @param original The table to copy
     * @return A copy of the table
     */
    protected Table copyTable(Table original) {
        Table copy = new Table(original);
        return copy;
    }
    
    /**
     * Gets a table by name from the table map.
     * 
     * @param tableName The name of the table to retrieve
     * @return The requested table or null if not found
     */
    protected Table getTable(String tableName) {
        return tableMap.get(tableName.toLowerCase());
    }
    
    /**
     * Registers a table in the table map.
     * 
     * @param tableName The name to register the table under
     * @param table The table to register
     */
    protected void registerTable(String tableName, Table table) {
        tableMap.put(tableName.toLowerCase(), table);
    }
}