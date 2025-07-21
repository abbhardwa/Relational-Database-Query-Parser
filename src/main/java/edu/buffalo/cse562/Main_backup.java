package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.service.QueryExecutionService;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Main entry point for the database query parser application.
 */
public class Main_backup {
    private static QueryExecutionService queryService;

    public static void main(String[] args) {
        try {
            // Initialize query service
            queryService = new QueryExecutionService();

            // Process command line arguments
            File dataDir = null;
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("--data")) {
                    dataDir = new File(args[++i]);
                }
            }

            if (dataDir == null || !dataDir.isDirectory()) {
                throw new IllegalArgumentException("Please provide a valid data directory with --data argument");
            }

            // Process each SQL file
            for (int i = 0; i < args.length; i++) {
                if (args[i].endsWith(".sql")) {
                    processSqlFile(new File(args[i]), dataDir);
                }
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void processSqlFile(File sqlFile, File dataDir) throws Exception {
        try (FileReader reader = new FileReader(sqlFile)) {
            CCJSqlParser parser = new CCJSqlParser(reader);
            Statement statement;

            while ((statement = parser.Statement()) != null) {
                if (statement instanceof CreateTable) {
                    processCreateTable((CreateTable) statement, dataDir);
                } else {
                    // Execute query and display results
                    Table result = queryService.executeQuery(statement.toString());
                    if (result != null) {
                        displayResults(result);
                    }
                }
            }
        }
    }

    private static void processCreateTable(CreateTable createTable, File dataDir) {
        String tableName = createTable.getTable().getName().toLowerCase();
        File tableFile = Paths.get(dataDir.getPath(), tableName + ".dat").toFile();
        if (!tableFile.exists()) {
            tableFile = Paths.get(dataDir.getPath(), tableName + ".tbl").toFile();
        }

        Table table = new Table(
            tableName,
            createTable.getColumnDefinitions().size(),
            tableFile,
            dataDir
        );
        
        table.setColumnDefinitions(new ArrayList<>(createTable.getColumnDefinitions()));
        table.populateColumnIndexMap();
        
        try {
            table.populateTable();
            queryService.registerTable(tableName, table);
        } catch (IOException e) {
            System.err.println("Error loading table " + tableName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void displayResults(Table result) {
        if (result != null && result.getTuples() != null) {
            for (String tuple : result.getTuples()) {
                System.out.println(tuple);
            }
        }
    }
}