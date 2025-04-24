package edu.buffalo.cse562.service;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.operations.SelectionOperation;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Service responsible for executing SQL queries and managing database tables.
 */
public class QueryExecutionService {
    private final HashMap<String, Table> tableMap;

    /**
     * Creates a new QueryExecutionService.
     */
    public QueryExecutionService() {
        this.tableMap = new HashMap<>();
    }

    /**
     * Executes a SQL query and returns the result.
     *
     * @param sql The SQL query to execute
     * @return The resulting table after query execution
     * @throws Exception If an error occurs during query execution
     */
    public Table executeQuery(String sql) throws Exception {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(sql));
        Statement statement = parser.Statement();

        if (statement instanceof CreateTable) {
            return handleCreateTable((CreateTable) statement, null);
        } else if (statement instanceof Select) {
            return handleSelect(statement);
        } else if (statement instanceof Insert) {
            return handleInsert((Insert) statement);
        } else if (statement instanceof Delete) {
            return handleDelete((Delete) statement);
        } else if (statement instanceof Update) {
            return handleUpdate((Update) statement);
        } else {
            throw new UnsupportedOperationException("Unsupported SQL statement type");
        }
    }

    /**
     * Creates a new table based on a CREATE TABLE statement.
     *
     * @param createTable The CREATE TABLE statement
     * @param dataDir The data directory
     * @return The newly created table
     * @throws IOException If an I/O error occurs
     */
    public Table createTable(CreateTable createTable, File dataDir) throws IOException {
        return handleCreateTable(createTable, dataDir);
    }

    private Table handleCreateTable(CreateTable createTable, File dataDir) throws IOException {
        String tableName = createTable.getTable().getName().toLowerCase();
        
        File tableFile = null;
        if (dataDir != null) {
            tableFile = Paths.get(dataDir.getPath(), tableName + ".dat").toFile();
            if (!tableFile.exists()) {
                tableFile = Paths.get(dataDir.getPath(), tableName + ".tbl").toFile();
            }
        }
        
        Table table = new Table(tableName, 
                              createTable.getColumnDefinitions().size(),
                              tableFile,
                              dataDir);
        
        table.setColumnDefinitions(new ArrayList<>(createTable.getColumnDefinitions()));
        table.populateColumnIndexMap();
        
        if (tableFile != null && tableFile.exists()) {
            table.populateTable();
        }
        
        tableMap.put(tableName, table);
        return table;
    }

    private Table handleSelect(Statement statement) throws IOException {
        SelectionOperation operation = new SelectionOperation(statement, tableMap);
        return operation.execute();
    }
    
    private Table handleInsert(Insert insert) {
        // Implementation for handling INSERT statements
        // Will be implemented in a future update
        return null;
    }
    
    private Table handleDelete(Delete delete) {
        // Implementation for handling DELETE statements
        // Will be implemented in a future update
        return null;
    }
    
    private Table handleUpdate(Update update) {
        // Implementation for handling UPDATE statements
        // Will be implemented in a future update
        return null;
    }

    /**
     * Gets a table by name from the table map.
     *
     * @param tableName The name of the table to retrieve
     * @return The requested table or null if not found
     */
    public Table getTable(String tableName) {
        return tableMap.get(tableName.toLowerCase());
    }

    /**
     * Registers a table in the table map.
     *
     * @param tableName The name to register the table under
     * @param table The table to register
     */
    public void registerTable(String tableName, Table table) {
        tableMap.put(tableName.toLowerCase(), table);
    }
    
    /**
     * Gets all registered tables.
     *
     * @return Map of table names to Table objects
     */
    public HashMap<String, Table> getAllTables() {
        return tableMap;
    }
}