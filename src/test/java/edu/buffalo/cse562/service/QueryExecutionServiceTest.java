/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562.service;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class QueryExecutionServiceTest {
    private File testDataDir;
    private QueryExecutionService queryService;
    
    @Before
    public void setUp() throws IOException {
        // Create test data directory
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        
        // Create customers table
        File customersFile = new File(testDataDir, "customers.tbl");
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|Customer A|CA|30\n");
            writer.write("2|Customer B|NY|25\n");
            writer.write("3|Customer C|CA|35\n");
            writer.write("4|Customer D|TX|28\n");
        }
        
        // Create orders table
        File ordersFile = new File(testDataDir, "orders.tbl");
        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00|2023-01-01\n");
            writer.write("102|1|200.00|2023-01-02\n");
            writer.write("103|2|150.00|2023-01-03\n");
            writer.write("104|3|300.00|2023-01-04\n");
        }
        
        // Initialize query service
        queryService = new QueryExecutionService(testDataDir);
    }
    
    @Test
    public void testSimpleSelect() throws ParseException {
        String sql = "SELECT * FROM customers";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return all customers", 4, result.getTuples().size());
    }
    
    @Test
    public void testSelectWithWhere() throws ParseException {
        String sql = "SELECT * FROM customers WHERE state = 'CA'";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return CA customers", 2, result.getTuples().size());
        assertTrue("Should contain Customer A", containsCustomer(result, "Customer A"));
        assertTrue("Should contain Customer C", containsCustomer(result, "Customer C"));
    }
    
    @Test
    public void testJoinQuery() throws ParseException {
        String sql = "SELECT c.name, o.order_id, o.amount " +
                    "FROM customers c JOIN orders o ON c.id = o.customer_id";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return 4 joined records", 4, result.getTuples().size());
        assertTrue("Should contain Customer A's orders", 
            result.getTuples().stream().anyMatch(t -> t.contains("Customer A") && t.contains("101")));
    }
    
    @Test
    public void testAggregateQuery() throws ParseException {
        String sql = "SELECT state, COUNT(*) as count, AVG(age) as avg_age " +
                    "FROM customers GROUP BY state";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return 3 groups", 3, result.getTuples().size());
    }
    
    @Test
    public void testOrderByQuery() throws ParseException {
        String sql = "SELECT * FROM customers ORDER BY age DESC";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should return all customers", 4, result.getTuples().size());
        assertTrue("First customer should be oldest", 
            result.getTuples().get(0).contains("Customer C"));
    }
    
    @Test
    public void testComplexQuery() throws ParseException {
        String sql = "SELECT c.state, COUNT(*) as order_count, SUM(o.amount) as total_amount " +
                    "FROM customers c JOIN orders o ON c.id = o.customer_id " +
                    "GROUP BY c.state " +
                    "HAVING SUM(o.amount) > 200 " +
                    "ORDER BY total_amount DESC";
        Statement stmt = parseSQL(sql);
        Table result = queryService.executeQuery(stmt);
        
        assertNotNull("Result should not be null", result);
        assertTrue("Should have results", result.getTuples().size() > 0);
    }
    
    @Test(expected = ParseException.class)
    public void testInvalidQuery() throws ParseException {
        String sql = "SELECT * FORM customers"; // Intentional typo
        parseSQL(sql);
    }
    
    private Statement parseSQL(String sql) throws ParseException {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(sql));
        return parser.Statement();
    }
    
    private boolean containsCustomer(Table table, String customerName) {
        return table.getTuples().stream()
            .anyMatch(tuple -> tuple.contains(customerName));
    }
}