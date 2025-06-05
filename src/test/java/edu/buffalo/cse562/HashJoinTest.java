package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the HashJoin class.
 */
public class HashJoinTest {
    private File testDataDir;
    private File customersFile;
    private File ordersFile;
    private Table customersTable;
    private Table ordersTable;
    private HashJoin hashJoin;

    @Before
    public void setUp() throws IOException {
        // Create test data directory
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();

        // Create test data files
        customersFile = new File(testDataDir, "customers.tbl");
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|Customer A|30\n");
            writer.write("2|Customer B|25\n");
            writer.write("3|Customer C|35\n");
        }

        ordersFile = new File(testDataDir, "orders.tbl");
        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00\n");
            writer.write("102|1|200.00\n");
            writer.write("103|2|150.00\n");
            writer.write("104|4|300.00\n");
        }

        // Create tables
        customersTable = new Table("customers", 3, customersFile, testDataDir);
        ordersTable = new Table("orders", 3, ordersFile, testDataDir);

        // Configure hash join condition
        hashJoin = new HashJoin(customersTable, ordersTable);
        hashJoin.setLeftKeyIndex(0);  // customer_id in customers table
        hashJoin.setRightKeyIndex(1); // customer_id in orders table
    }

    @Test
    public void testHashJoin() throws IOException {
        // Populate tables
        customersTable.populateTable();
        ordersTable.populateTable();

        // Perform hash join
        Table resultTable = hashJoin.join();

        // Verify results
        assertNotNull("Result table should not be null", resultTable);
        assertNotNull("Result tuples should not be null", resultTable.getTuples());
        
        // Should have 3 matches (2 for customer 1, 1 for customer 2)
        assertEquals("Should have 3 joined tuples", 3, resultTable.getTuples().size());

        // Verify joined tuples
        assertTrue("Should contain joined tuple for Customer A's first order",
            resultTable.getTuples().contains("1|Customer A|30|101|1|100.00") ||
            resultTable.getTuples().contains("1|Customer A|30|102|1|200.00"));
        
        assertTrue("Should contain joined tuple for Customer B's order",
            resultTable.getTuples().contains("2|Customer B|25|103|2|150.00"));
    }

    @Test
    public void testHashJoinWithNoMatches() throws IOException {
        // Create test data with no matching records
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("5|Customer X|40\n");
            writer.write("6|Customer Y|45\n");
        }

        // Populate tables
        customersTable.populateTable();
        ordersTable.populateTable();

        // Perform hash join
        Table resultTable = hashJoin.join();

        // Verify results
        assertNotNull("Result table should not be null", resultTable);
        assertTrue("Result should be empty", resultTable.getTuples().isEmpty());
    }

    @Test
    public void testHashJoinWithEmptyTables() throws IOException {
        // Create empty files
        try (FileWriter writer = new FileWriter(customersFile)) {}
        try (FileWriter writer = new FileWriter(ordersFile)) {}

        // Populate tables
        customersTable.populateTable();
        ordersTable.populateTable();

        // Perform hash join
        Table resultTable = hashJoin.join();

        // Verify results
        assertNotNull("Result table should not be null", resultTable);
        assertTrue("Result should be empty", resultTable.getTuples().isEmpty());
    }

    /**
     * Tests column mapping and ordering in hash join results.
     * Verifies:
     * - Correct preservation of column names
     * - Proper ordering of columns in result
     * - Handling of duplicate column names
     */
    @Test
    public void testHashJoinColumnMapping() throws IOException {
        // Set up test tables with specific column names
        ArrayList<ColumnDefinition> customerColumns = new ArrayList<>();
        ArrayList<ColumnDefinition> orderColumns = new ArrayList<>();
        
        // Customer columns
        ColumnDefinition custId = new ColumnDefinition();
        custId.setColumnName("ID");
        ColumnDefinition custName = new ColumnDefinition();
        custName.setColumnName("NAME");
        ColumnDefinition custAge = new ColumnDefinition();
        custAge.setColumnName("AGE");
        customerColumns.add(custId);
        customerColumns.add(custName);
        customerColumns.add(custAge);
        
        // Order columns (note: ID appears in both tables)
        ColumnDefinition orderId = new ColumnDefinition();
        orderId.setColumnName("ORDER_ID");
        ColumnDefinition orderCustId = new ColumnDefinition();
        orderCustId.setColumnName("ID"); // Same name as customer ID
        ColumnDefinition amount = new ColumnDefinition();
        amount.setColumnName("AMOUNT");
        orderColumns.add(orderId);
        orderColumns.add(orderCustId);
        orderColumns.add(amount);

        // Create and populate test data
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|John Doe|30\n");
            writer.write("2|Jane Smith|25\n");
        }

        try (FileWriter writer = new FileWriter(ordersFile)) {
            // Create test orders with format: ORDER_ID|ID|AMOUNT
            // ID here matches with customer ID for join purposes
            // 101|1|100.00 = Order #101 for customer #1 with amount $100.00
            // 102|2|200.00 = Order #102 for customer #2 with amount $200.00
            writer.write("101|1|100.00\n");
            writer.write("102|2|200.00\n");
        }

        // Set up tables with column definitions
        customersTable = new Table("customers", 3, customersFile, testDataDir);
        customersTable.setColumnDescriptionList(customerColumns);
        ordersTable = new Table("orders", 3, ordersFile, testDataDir);
        ordersTable.setColumnDescriptionList(orderColumns);
        
        // Configure hash join condition
        hashJoin = new HashJoin(customersTable, ordersTable);
        hashJoin.setLeftKeyIndex(0);  // customer_id in customers table
        hashJoin.setRightKeyIndex(1); // customer_id in orders table

        // Populate tables
        customersTable.populateTable();
        ordersTable.populateTable();

        // Perform hash join
        Table resultTable = hashJoin.join();

        // Verify column definitions in result
        ArrayList<ColumnDefinition> resultColumns = resultTable.getColumnDescriptionList();
        assertEquals("Should have combined number of columns", 6, resultColumns.size());

        // Verify column order and names
        assertEquals("First column should be customer ID", "ID", resultColumns.get(0).getColumnName());
        assertEquals("Second column should be customer NAME", "NAME", resultColumns.get(1).getColumnName());
        assertEquals("Third column should be customer AGE", "AGE", resultColumns.get(2).getColumnName());
        assertEquals("Fourth column should be order ORDER_ID", "ORDER_ID", resultColumns.get(3).getColumnName());
        assertEquals("Fifth column should be order ID", "ID", resultColumns.get(4).getColumnName());
        assertEquals("Sixth column should be order AMOUNT", "AMOUNT", resultColumns.get(5).getColumnName());

        // Verify data matches column definitions
        for (String tuple : resultTable.getTuples()) {
            String[] values = tuple.split("\\|");
            assertEquals("Tuple should have correct number of values", 6, values.length);
            // First value should be numeric (customer ID)
            assertTrue("First value should be numeric", values[0].matches("\\d+"));
            // Last value should be amount in decimal format
            assertTrue("Last value should be decimal amount", values[5].matches("\\d+\\.\\d{2}"));
        }
    }

    /**
     * Tests handling of string formatting and pipe separators in hash join.
     * Verifies:
     * - Correct handling of trailing pipe characters
     * - Proper string concatenation
     * - Handling of pipe characters within column values
     */
    @Test
    public void testHashJoinStringFormatting() throws IOException {
        // Create test data with various pipe character scenarios
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|Customer|With|Pipes|30|\n");  // Extra pipes in value and at end
            writer.write("2|Regular Customer|35\n");      // Normal format
            writer.write("3|Customer|No Age|\n");         // Empty last value with pipe
        }

        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00|\n");    // With trailing pipe
            writer.write("102|2|200.00\n");     // Without trailing pipe
            writer.write("103|3|300.00|\n");    // With trailing pipe
        }

        // Populate tables
        customersTable.populateTable();
        ordersTable.populateTable();

        // Perform hash join
        Table resultTable = hashJoin.join();

        // Verify results maintain correct formatting
        assertEquals("Should have 3 joined tuples", 3, resultTable.getTuples().size());

        // Verify proper handling of pipes in values and at end
        assertTrue("Should handle pipes in customer data correctly",
            resultTable.getTuples().contains("1|Customer|With|Pipes|30|101|1|100.00"));
        
        // Verify normal case without extra pipes
        assertTrue("Should handle normal format correctly",
            resultTable.getTuples().contains("2|Regular Customer|35|102|2|200.00"));
        
        // Verify empty values with pipes
        assertTrue("Should handle empty values and ending pipes correctly",
            resultTable.getTuples().contains("3|Customer|No Age|103|3|300.00"));
    }

    /**
     * Tests hash join behavior when multiple records in both tables match on the join key.
     * Verifies that:
     * - All possible combinations are generated (cartesian product)
     * - Results are correctly formatted
     * - No records are lost or duplicated
     */
    @Test
    public void testHashJoinMultipleMatches() throws IOException {
        // Create test data with multiple matches
        // 2 customers with id 1, 3 orders for customer id 1
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|Customer A|30\n");
            writer.write("1|Customer B|35\n");
            writer.write("2|Customer C|40\n");
        }

        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00\n");
            writer.write("102|1|200.00\n");
            writer.write("103|1|300.00\n");
            writer.write("104|2|400.00\n");
        }

        // Populate tables
        customersTable.populateTable();
        ordersTable.populateTable();

        // Perform hash join
        Table resultTable = hashJoin.join();

        // Should have 6 matches for id 1 (2 customers * 3 orders) + 1 match for id 2
        assertEquals("Should have correct number of joined tuples", 7, resultTable.getTuples().size());

        // Verify all combinations for customer id 1
        assertTrue("Should contain Customer A's first order",
            resultTable.getTuples().contains("1|Customer A|30|101|1|100.00"));
        assertTrue("Should contain Customer A's second order",
            resultTable.getTuples().contains("1|Customer A|30|102|1|200.00"));
        assertTrue("Should contain Customer A's third order",
            resultTable.getTuples().contains("1|Customer A|30|103|1|300.00"));
        
        assertTrue("Should contain Customer B's first order",
            resultTable.getTuples().contains("1|Customer B|35|101|1|100.00"));
        assertTrue("Should contain Customer B's second order",
            resultTable.getTuples().contains("1|Customer B|35|102|1|200.00"));
        assertTrue("Should contain Customer B's third order",
            resultTable.getTuples().contains("1|Customer B|35|103|1|300.00"));

        // Verify single match for customer id 2
        assertTrue("Should contain Customer C's order",
            resultTable.getTuples().contains("2|Customer C|40|104|2|400.00"));
    }

    /**
     * Tests the table size optimization in hash join where the algorithm should:
     * - Use the smaller table to build the hash table
     * - Use the larger table to probe the hash table
     * This test verifies both scenarios (t1 > t2 and t1 <= t2)
     */
    @Test
    public void testHashJoinTableSizeOptimization() throws IOException {
        // Test case 1: t1 > t2
        // Create a larger customers table and smaller orders table
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|Customer A|30\n");
            writer.write("2|Customer B|25\n");
            writer.write("3|Customer C|35\n");
            writer.write("4|Customer D|40\n");
            writer.write("5|Customer E|45\n");
        }

        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00\n");
            writer.write("102|2|200.00\n");
        }

        customersTable.populateTable();
        ordersTable.populateTable();
        Table result1 = hashJoin.join();
        
        assertEquals("Should have 2 joined tuples when t1 > t2", 2, result1.getTuples().size());
        assertTrue("Should contain correct joined tuple for Customer A",
            result1.getTuples().contains("1|Customer A|30|101|1|100.00"));
        assertTrue("Should contain correct joined tuple for Customer B", 
            result1.getTuples().contains("2|Customer B|25|102|2|200.00"));

        // Test case 2: t1 <= t2
        // Create a smaller customers table and larger orders table
        try (FileWriter writer = new FileWriter(customersFile)) {
            // Create only two customers to ensure t1 <= t2 condition
            // Format: ID|NAME|AGE
            writer.write("1|Customer A|30\n");
            writer.write("2|Customer B|25\n");
        } catch (IOException e) {
            // Log error and fail test if file operations fail
            System.err.println("Error writing customer test data: " + e.getMessage());
            fail("Test setup failed: Could not create customer test data");
        }

        try (FileWriter writer = new FileWriter(ordersFile)) {
            // Create more orders than customers (4 > 2) to test t1 <= t2 condition
            // Format: ORDER_ID|CUSTOMER_ID|AMOUNT
            // Two orders per customer to test multiple matches
            writer.write("101|1|100.00\n"); // First order for customer 1
            writer.write("102|2|200.00\n"); // First order for customer 2
            writer.write("103|1|300.00\n"); // Second order for customer 1
            writer.write("104|2|400.00\n"); // Second order for customer 2
        } catch (IOException e) {
            // Log error and fail test if file operations fail
            System.err.println("Error writing order test data: " + e.getMessage());
            fail("Test setup failed: Could not create order test data");
        }

        customersTable.populateTable();
        ordersTable.populateTable();
        Table result2 = hashJoin.join();

        assertEquals("Should have 4 joined tuples when t1 <= t2", 4, result2.getTuples().size());
        assertTrue("Should contain all joined tuples for Customer A",
            result2.getTuples().contains("1|Customer A|30|101|1|100.00") &&
            result2.getTuples().contains("1|Customer A|30|103|1|300.00"));
        assertTrue("Should contain all joined tuples for Customer B",
            result2.getTuples().contains("2|Customer B|25|102|2|200.00") &&
            result2.getTuples().contains("2|Customer B|25|104|2|400.00"));
    }

    /**
     * Tests edge cases and error conditions in hash join operations.
     * Verifies:
     * - Handling of null/invalid column indexes
     * - Handling of invalid table data
     * - Handling of out-of-range join key indexes 
     */
    @Test
    public void testHashJoinEdgeCases() throws IOException {
        // Test 1: Invalid join key index
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|John Doe|30\n");
        }
        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00\n");
        }

        customersTable = new Table("customers", 3, customersFile, testDataDir);
        ordersTable = new Table("orders", 3, ordersFile, testDataDir);

        // Set up with invalid join key index
        hashJoin = new HashJoin(customersTable, ordersTable);
        hashJoin.setLeftKeyIndex(5);  // Invalid index
        hashJoin.setRightKeyIndex(1);

        customersTable.populateTable();
        ordersTable.populateTable();

        // Should still produce valid but empty results when key index is invalid
        Table result1 = hashJoin.join();
        assertNotNull("Result should not be null with invalid key index", result1);
        assertTrue("Result should be empty with invalid key index", result1.getTuples().isEmpty());

        // Test 2: Malformed data (wrong number of columns)
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|John Doe\n");  // Missing age column
            writer.write("2|Jane|Smith|25\n");  // Extra column
        }
        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00\n");
        }

        customersTable = new Table("customers", 3, customersFile, testDataDir);
        ordersTable = new Table("orders", 3, ordersFile, testDataDir);
        hashJoin = new HashJoin(customersTable, ordersTable);
        hashJoin.setLeftKeyIndex(0);
        hashJoin.setRightKeyIndex(1);

        customersTable.populateTable();
        ordersTable.populateTable();

        // Should handle malformed data gracefully
        Table result2 = hashJoin.join();
        assertNotNull("Result should not be null with malformed data", result2);
        // Any valid tuples should still be processed
        assertEquals("Should process valid tuples from malformed data", 1, result2.getTuples().size());
        assertTrue("Should contain valid joined tuple",
            result2.getTuples().stream().anyMatch(t -> t.startsWith("2|Jane|Smith|25|101|1|100.00")));

        // Test 3: Non-matching data types
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("AAA|John Doe|30\n");  // Non-numeric ID
        }
        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00\n");
        }

        customersTable = new Table("customers", 3, customersFile, testDataDir);
        ordersTable = new Table("orders", 3, ordersFile, testDataDir);
        hashJoin = new HashJoin(customersTable, ordersTable);
        hashJoin.setLeftKeyIndex(0);
        hashJoin.setRightKeyIndex(1);

        customersTable.populateTable();
        ordersTable.populateTable();

        // Should handle non-matching data types gracefully
        Table result3 = hashJoin.join();
        assertNotNull("Result should not be null with non-matching data types", result3);
        assertTrue("Result should be empty with non-matching data types", result3.getTuples().isEmpty());
    }

    @After
    public void tearDown() {
        // Clean up test files
        customersFile.delete();
        ordersFile.delete();
    }
}