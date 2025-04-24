package edu.buffalo.cse562.service;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Processes SQL files and executes statements.
 */
public class SqlFileProcessor {
    private final QueryExecutionService queryService;
    private final StatementHandlerFactory handlerFactory;

    /**
     * Creates a new SqlFileProcessor.
     * 
     * @param queryService The query execution service
     */
    public SqlFileProcessor(QueryExecutionService queryService) {
        this.queryService = queryService;
        this.handlerFactory = new StatementHandlerFactory(queryService);
    }

    /**
     * Processes a SQL file.
     * 
     * @param sqlFile The SQL file to process
     * @param dataDir The data directory
     * @throws Exception If an error occurs during processing
     */
    public void processSqlFile(File sqlFile, File dataDir) throws Exception {
        try (FileReader reader = new FileReader(sqlFile)) {
            CCJSqlParser parser = new CCJSqlParser(reader);
            Statement statement;

            while ((statement = parser.Statement()) != null) {
                StatementHandler handler = handlerFactory.getHandler(statement);
                if (handler != null) {
                    handler.handle(statement, dataDir);
                }
            }
        }
    }

    /**
     * Interface for statement handlers.
     */
    public interface StatementHandler {
        /**
         * Handles a SQL statement.
         * 
         * @param statement The statement to handle
         * @param dataDir The data directory
         * @throws Exception If an error occurs during handling
         */
        void handle(Statement statement, File dataDir) throws Exception;
    }

    /**
     * Factory class for creating statement handlers.
     */
    private class StatementHandlerFactory {
        private final HashMap<Class<?>, StatementHandler> handlers = new HashMap<>();

        /**
         * Creates a new StatementHandlerFactory.
         * 
         * @param queryService The query execution service
         */
        public StatementHandlerFactory(QueryExecutionService queryService) {
            handlers.put(CreateTable.class, new CreateTableHandler(queryService));
            handlers.put(Select.class, new SelectHandler(queryService));
            handlers.put(Insert.class, new InsertHandler(queryService));
            handlers.put(Delete.class, new DeleteHandler(queryService));
            handlers.put(Update.class, new UpdateHandler(queryService));
        }

        /**
         * Gets the appropriate handler for a statement.
         * 
         * @param statement The statement to handle
         * @return The appropriate handler for the statement type
         */
        public StatementHandler getHandler(Statement statement) {
            return handlers.get(statement.getClass());
        }
    }

    /**
     * Handles CREATE TABLE statements.
     */
    private class CreateTableHandler implements StatementHandler {
        private final QueryExecutionService queryService;

        /**
         * Creates a new CreateTableHandler.
         * 
         * @param queryService The query execution service
         */
        public CreateTableHandler(QueryExecutionService queryService) {
            this.queryService = queryService;
        }

        @Override
        public void handle(Statement statement, File dataDir) throws Exception {
            CreateTable createTable = (CreateTable) statement;
            String tableName = createTable.getTable().getName().toLowerCase();
            
            Table table = queryService.createTable(createTable, dataDir);
            if (table != null) {
                System.out.println("Created table: " + tableName);
            }
        }
    }

    /**
     * Handles SELECT statements.
     */
    private class SelectHandler implements StatementHandler {
        private final QueryExecutionService queryService;

        /**
         * Creates a new SelectHandler.
         * 
         * @param queryService The query execution service
         */
        public SelectHandler(QueryExecutionService queryService) {
            this.queryService = queryService;
        }

        @Override
        public void handle(Statement statement, File dataDir) throws Exception {
            Table result = queryService.executeQuery(statement.toString());
            if (result != null) {
                displayResults(result);
            }
        }

        private void displayResults(Table result) {
            if (result != null && result.getTuples() != null) {
                for (String tuple : result.getTuples()) {
                    System.out.println(tuple);
                }
            }
        }
    }

    /**
     * Handles INSERT statements.
     */
    private class InsertHandler implements StatementHandler {
        private final QueryExecutionService queryService;

        /**
         * Creates a new InsertHandler.
         * 
         * @param queryService The query execution service
         */
        public InsertHandler(QueryExecutionService queryService) {
            this.queryService = queryService;
        }

        @Override
        public void handle(Statement statement, File dataDir) throws Exception {
            // Implementation for handling INSERT statements
            // Will be implemented in a future update
        }
    }

    /**
     * Handles UPDATE statements.
     */
    private class UpdateHandler implements StatementHandler {
        private final QueryExecutionService queryService;

        /**
         * Creates a new UpdateHandler.
         * 
         * @param queryService The query execution service
         */
        public UpdateHandler(QueryExecutionService queryService) {
            this.queryService = queryService;
        }

        @Override
        public void handle(Statement statement, File dataDir) throws Exception {
            // Implementation for handling UPDATE statements
            // Will be implemented in a future update
        }
    }

    /**
     * Handles DELETE statements.
     */
    private class DeleteHandler implements StatementHandler {
        private final QueryExecutionService queryService;

        /**
         * Creates a new DeleteHandler.
         * 
         * @param queryService The query execution service
         */
        public DeleteHandler(QueryExecutionService queryService) {
            this.queryService = queryService;
        }

        @Override
        public void handle(Statement statement, File dataDir) throws Exception {
            // Implementation for handling DELETE statements
            // Will be implemented in a future update
        }
    }
}