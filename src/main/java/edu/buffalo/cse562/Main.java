package edu.buffalo.cse562;

import edu.buffalo.cse562.service.QueryExecutionService;
import edu.buffalo.cse562.service.SqlFileProcessor;
import edu.buffalo.cse562.util.CommandLineParser;

import java.io.File;

/**
 * Main entry point for the relational database query parser application.
 * Handles initialization and high-level application flow.
 */
public class Main {
    /**
     * Application entry point.
     * 
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        try {
            // Parse command line arguments
            CommandLineParser parser = new CommandLineParser();
            parser.parse(args);
            parser.validate();

            // Initialize services
            QueryExecutionService queryService = new QueryExecutionService();
            SqlFileProcessor sqlProcessor = new SqlFileProcessor(queryService);

            // Process each SQL file
            for (File sqlFile : parser.getSqlFileList()) {
                sqlProcessor.processSqlFile(sqlFile, parser.getDataDirectory());
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
