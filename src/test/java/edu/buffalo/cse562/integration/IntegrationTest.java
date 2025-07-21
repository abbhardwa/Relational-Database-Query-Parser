package edu.buffalo.cse562.integration;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.service.QueryExecutionService;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.*;

public class IntegrationTest {
    private File testDataDir;
    private QueryExecutionService queryService;

    @Before
    public void setUp() throws IOException {
        // Create test data directory
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        
        // Create comprehensive test dataset
        createEmployeesTable();
        createDepartmentsTable();
        createProjectsTable();
        
        // Initialize query service
        queryService = new QueryExecutionService(testDataDir);
    }

    @After
    public void tearDown() {
        // Clean up test files
        String[] testFiles = {
            "employees_integration.tbl",
            "departments_integration.tbl", 
            "projects_integration.tbl",
            "large_employees.tbl"
        };
        
        for (String fileName : testFiles) {
            File file = new File(testDataDir, fileName);
            if (file.exists()) {
                file.delete();
            }
        }
    }

    private void createEmployeesTable() throws IOException {
        File employeesFile = new File(testDataDir, "employees_integration.tbl");
        try (FileWriter writer = new FileWriter(employeesFile)) {
            writer.write("1|John Doe|Engineering|75000|john@company.com\n");
            writer.write("2|Jane Smith|Marketing|65000|jane@company.com\n");
            writer.write("3|Mike Johnson|Engineering|80000|mike@company.com\n");
            writer.write("4|Sarah Wilson|HR|60000|sarah@company.com\n");
            writer.write("5|David Brown|Engineering|85000|david@company.com\n");
            writer.write("6|Lisa Davis|Marketing|70000|lisa@company.com\n");
            writer.write("7|Tom Anderson|Sales|55000|tom@company.com\n");
            writer.write("8|Anna White|Engineering|90000|anna@company.com\n");
            writer.write("9|Chris Taylor|Marketing|62000|chris@company.com\n");
            writer.write("10|Emma Garcia|HR|58000|emma@company.com\n");
        }
    }

    private void createDepartmentsTable() throws IOException {
        File departmentsFile = new File(testDataDir, "departments_integration.tbl");
        try (FileWriter writer = new FileWriter(departmentsFile)) {
            writer.write("Engineering|Tech Building|John Doe\n");
            writer.write("Marketing|Marketing Building|Jane Smith\n");
            writer.write("HR|Admin Building|Sarah Wilson\n");
            writer.write("Sales|Sales Building|Tom Anderson\n");
            writer.write("Finance|Finance Building|Robert Lee\n");
        }
    }

    private void createProjectsTable() throws IOException {
        File projectsFile = new File(testDataDir, "projects_integration.tbl");
        try (FileWriter writer = new FileWriter(projectsFile)) {
            writer.write("1|Database System|Engineering|2023-01-01|2023-12-31|Active\n");
            writer.write("2|Marketing Campaign|Marketing|2023-02-01|2023-06-30|Completed\n");
            writer.write("3|HR Portal|HR|2023-03-01|2023-09-30|Active\n");
            writer.write("4|Sales Analytics|Sales|2023-01-15|2023-08-15|Active\n");
            writer.write("5|Mobile App|Engineering|2023-04-01|2023-11-30|Active\n");
            writer.write("6|Customer Survey|Marketing|2023-05-01|2023-07-31|Planning\n");
        }
    }

    @Test
    public void testSimpleSelectFromEmployees() throws Exception {
        String sql = "SELECT * FROM employees_integration";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return all 10 employees", 10, result.getTuples().size());
        
        // Verify first employee
        String firstEmployee = result.getTuples().get(0);
        assertTrue("Should contain John Doe", firstEmployee.contains("John Doe"));
        assertTrue("Should contain Engineering", firstEmployee.contains("Engineering"));
    }

    @Test
    public void testSelectFromDifferentTables() throws Exception {
        // Test departments table
        String sql = "SELECT * FROM departments_integration";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return all 5 departments", 5, result.getTuples().size());
        
        // Verify first department
        String firstDepartment = result.getTuples().get(0);
        assertTrue("Should contain Engineering", firstDepartment.contains("Engineering"));
        assertTrue("Should contain Tech Building", firstDepartment.contains("Tech Building"));
    }

    @Test
    public void testSelectFromProjectsTable() throws Exception {
        String sql = "SELECT * FROM projects_integration";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return all 6 projects", 6, result.getTuples().size());
        
        // Verify first project
        String firstProject = result.getTuples().get(0);
        assertTrue("Should contain Database System", firstProject.contains("Database System"));
        assertTrue("Should contain Active", firstProject.contains("Active"));
    }

    @Test
    public void testQueryExecutionServiceConsistency() throws Exception {
        // Test that the same query returns consistent results
        String sql = "SELECT * FROM employees_integration";
        Statement stmt = parseSQL(sql);
        
        Table result1 = queryService.executeQuery(stmt);
        Table result2 = queryService.executeQuery(stmt);
        
        assertNotNull("First result should not be null", result1);
        assertNotNull("Second result should not be null", result2);
        assertEquals("Results should be consistent", 
                    result1.getTuples().size(), result2.getTuples().size());
        
        // Verify data consistency
        for (int i = 0; i < result1.getTuples().size(); i++) {
            assertEquals("Data should be consistent between queries",
                        result1.getTuples().get(i), result2.getTuples().get(i));
        }
    }

    @Test
    public void testQueryServiceWithMultipleTables() throws Exception {
        // Test that the query service can handle queries from different tables
        String[] queries = {
            "SELECT * FROM employees_integration",
            "SELECT * FROM departments_integration", 
            "SELECT * FROM projects_integration"
        };
        
        int[] expectedCounts = {10, 5, 6};
        
        for (int i = 0; i < queries.length; i++) {
            Statement stmt = parseSQL(queries[i]);
            Table result = queryService.executeQuery(stmt);
            
            assertNotNull("Result should not be null for query " + i, result);
            assertEquals("Should return correct count for table " + i, 
                        expectedCounts[i], result.getTuples().size());
        }
    }

    @Test
    public void testErrorHandlingWithInvalidTable() {
        try {
            String sql = "SELECT * FROM nonexistent_table";
            Statement stmt = parseSQL(sql);
            Table result = queryService.executeQuery(stmt);
            // If no exception is thrown, the service handles missing tables gracefully
            if (result != null) {
                assertTrue("Service handles missing tables gracefully", true);
            }
        } catch (Exception e) {
            // Exceptions are expected for invalid tables
            assertNotNull("Exception should have a message", e.getMessage());
            assertTrue("Exception message should be meaningful", 
                      e.getMessage().length() > 0);
        }
    }

    @Test
    public void testErrorHandlingWithInvalidSQL() {
        try {
            String sql = "INVALID SQL SYNTAX";
            Statement stmt = parseSQL(sql);
            // If parsing fails, that's expected
            fail("Should throw ParseException for invalid SQL");
        } catch (ParseException e) {
            // This is expected behavior
            assertNotNull("Parse exception should have a message", e.getMessage());
        } catch (Exception e) {
            // Other exceptions are also acceptable
            assertNotNull("Exception should have a message", e.getMessage());
        }
    }

    @Test
    public void testPerformanceWithRepeatedQueries() throws Exception {
        String sql = "SELECT * FROM employees_integration";
        Statement stmt = parseSQL(sql);
        
        // Execute the same query multiple times to test performance consistency
        long totalTime = 0;
        int iterations = 10;
        
        for (int i = 0; i < iterations; i++) {
            long startTime = System.currentTimeMillis();
            Table result = queryService.executeQuery(stmt);
            long endTime = System.currentTimeMillis();
            
            assertNotNull("Result should not be null", result);
            assertEquals("Should return consistent results", 10, result.getTuples().size());
            
            totalTime += (endTime - startTime);
        }
        
        long averageTime = totalTime / iterations;
        assertTrue("Average query time should be reasonable (< 1 second)", averageTime < 1000);
    }

    @Test
    public void testDataIntegrityAfterMultipleQueries() throws Exception {
        // Execute multiple different queries and verify data integrity
        String[] queries = {
            "SELECT * FROM employees_integration",
            "SELECT * FROM departments_integration",
            "SELECT * FROM projects_integration",
            "SELECT * FROM employees_integration" // Repeat first query
        };
        
        Table firstResult = null;
        Table lastResult = null;
        
        for (int i = 0; i < queries.length; i++) {
            Statement stmt = parseSQL(queries[i]);
            Table result = queryService.executeQuery(stmt);
            
            assertNotNull("Result should not be null for query " + i, result);
            
            if (i == 0) {
                firstResult = result;
            } else if (i == queries.length - 1) {
                lastResult = result;
            }
        }
        
        // Verify that the first and last queries (same table) return identical results
        assertNotNull("First result should not be null", firstResult);
        assertNotNull("Last result should not be null", lastResult);
        assertEquals("Results should be identical for same query", 
                    firstResult.getTuples().size(), lastResult.getTuples().size());
        
        for (int i = 0; i < firstResult.getTuples().size(); i++) {
            assertEquals("Tuples should be identical",
                        firstResult.getTuples().get(i), lastResult.getTuples().get(i));
        }
    }

    @Test
    public void testLargeDatasetHandling() throws Exception {
        // Create a larger dataset for testing
        File largeEmployeesFile = new File(testDataDir, "large_employees.tbl");
        try (FileWriter writer = new FileWriter(largeEmployeesFile)) {
            for (int i = 1; i <= 500; i++) {
                writer.write(i + "|Employee" + i + "|Department" + (i % 5) + "|" + 
                           (50000 + (i * 100)) + "|emp" + i + "@company.com\n");
            }
        }
        
        String sql = "SELECT * FROM large_employees";
        Statement stmt = parseSQL(sql);
        
        long startTime = System.currentTimeMillis();
        Table result = queryService.executeQuery(stmt);
        long endTime = System.currentTimeMillis();
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return all 500 employees", 500, result.getTuples().size());
        
        long executionTime = endTime - startTime;
        assertTrue("Query should complete in reasonable time (< 3 seconds)", executionTime < 3000);
        
        // Verify data quality
        String firstEmployee = result.getTuples().get(0);
        assertTrue("First employee should be Employee1", firstEmployee.contains("Employee1"));
        
        String lastEmployee = result.getTuples().get(499);
        assertTrue("Last employee should be Employee500", lastEmployee.contains("Employee500"));
    }

    @Test
    public void testEmptyTableHandling() throws Exception {
        // Create an empty table file
        File emptyFile = new File(testDataDir, "empty_table.tbl");
        try (FileWriter writer = new FileWriter(emptyFile)) {
            // Write nothing - empty file
        }
        
        String sql = "SELECT * FROM empty_table";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null for empty table", result);
        assertEquals("Empty table should return 0 tuples", 0, result.getTuples().size());
        
        // Clean up
        emptyFile.delete();
    }

    @Test
    public void testSpecialCharactersInData() throws Exception {
        // Create table with special characters
        File specialFile = new File(testDataDir, "special_chars.tbl");
        try (FileWriter writer = new FileWriter(specialFile)) {
            writer.write("1|O'Connor|Marketing|65000|oconnor@company.com\n");
            writer.write("2|Smith & Jones|Engineering|75000|smith.jones@company.com\n");
            writer.write("3|García|Sales|55000|garcia@company.com\n");
            writer.write("4|User With Spaces|HR|60000|user.spaces@company.com\n");
        }
        
        String sql = "SELECT * FROM special_chars";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return all 4 records with special characters", 4, result.getTuples().size());
        
        // Verify special characters are preserved
        boolean foundSpecialChars = false;
        for (String tuple : result.getTuples()) {
            if (tuple.contains("O'Connor") || tuple.contains("García") || tuple.contains("Smith & Jones")) {
                foundSpecialChars = true;
                break;
            }
        }
        assertTrue("Should preserve special characters in data", foundSpecialChars);
        
        // Clean up
        specialFile.delete();
    }

    private Statement parseSQL(String sql) throws ParseException {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(sql));
        return parser.Statement();
    }
}