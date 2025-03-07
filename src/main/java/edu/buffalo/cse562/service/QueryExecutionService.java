package edu.buffalo.cse562.service;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.operations.SelectionOperation;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

/**
 * Service responsible for executing SQL queries.
 */
public class QueryExecutionService {
    private final HashMap<String, Table> tableMap;

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
            return handleCreateTable((CreateTable) statement);
        } else if (statement instanceof Select) {
            return handleSelect(statement);
        } else {
            throw new UnsupportedOperationException("Only CREATE TABLE and SELECT statements are supported");
        }
    }

    private Table handleCreateTable(CreateTable createTable) {
        String tableName = createTable.getTable().getName().toLowerCase();
        Table table = new Table(tableName, 
                              createTable.getColumnDefinitions().size(),
                              null,  // TODO: Set proper file path
                              null); // TODO: Set proper directory path
        table.setColumnDefinitions(new ArrayList<>(createTable.getColumnDefinitions()));
        table.populateColumnIndexMap();
        tableMap.put(tableName, table);
        return table;
    }

    private Table handleSelect(Statement statement) throws IOException {
        SelectionOperation operation = new SelectionOperation(statement, tableMap);
        return operation.execute();
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
}