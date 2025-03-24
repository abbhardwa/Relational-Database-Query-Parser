package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
            writer.write("1|Customer A|30\n");
            writer.write("2|Customer B|25\n");
        }

        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|100.00\n");
            writer.write("102|2|200.00\n");
            writer.write("103|1|300.00\n");
            writer.write("104|2|400.00\n");
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

    @After
    public void tearDown() {
        // Clean up test files
        customersFile.delete();
        ordersFile.delete();
    }
}