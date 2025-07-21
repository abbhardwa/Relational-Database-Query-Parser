package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class HybridHashTest {
    private File testDataDir;
    private File customersFile;
    private File ordersFile;
    private Table customersTable;
    private Table ordersTable;

    @Before
    public void setUp() throws IOException {
        // Create test data directory
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();

        // Create customers test data
        customersFile = new File(testDataDir, "customers_hybrid.tbl");
        try (FileWriter writer = new FileWriter(customersFile)) {
            writer.write("1|Alice|30|Marketing\n");
            writer.write("2|Bob|25|Engineering\n");
            writer.write("3|Charlie|35|Sales\n");
            writer.write("4|Diana|28|Marketing\n");
            writer.write("5|Eve|32|Engineering\n");
        }

        // Create orders test data  
        ordersFile = new File(testDataDir, "orders_hybrid.tbl");
        try (FileWriter writer = new FileWriter(ordersFile)) {
            writer.write("101|1|150.00|2023-01-01\n");
            writer.write("102|1|200.00|2023-01-02\n");
            writer.write("103|2|75.00|2023-01-03\n");
            writer.write("104|3|300.00|2023-01-04\n");
            writer.write("105|6|50.00|2023-01-05\n"); // Customer 6 doesn't exist in customers table
        }

        // Set up customers table
        customersTable = new Table("customers", 4, customersFile, testDataDir);
        ArrayList<ColumnDefinition> customersColumns = new ArrayList<>();
        
        ColumnDefinition custCol1 = new ColumnDefinition();
        custCol1.setColumnName("id");
        customersColumns.add(custCol1);
        
        ColumnDefinition custCol2 = new ColumnDefinition();
        custCol2.setColumnName("name");
        customersColumns.add(custCol2);
        
        ColumnDefinition custCol3 = new ColumnDefinition();
        custCol3.setColumnName("age");
        customersColumns.add(custCol3);
        
        ColumnDefinition custCol4 = new ColumnDefinition();
        custCol4.setColumnName("department");
        customersColumns.add(custCol4);
        
        customersTable.setColumnDefinitions(customersColumns);
        customersTable.populateColumnIndexMap();
        customersTable.populateTable();

        // Set up orders table
        ordersTable = new Table("orders", 4, ordersFile, testDataDir);
        ArrayList<ColumnDefinition> ordersColumns = new ArrayList<>();
        
        ColumnDefinition orderCol1 = new ColumnDefinition();
        orderCol1.setColumnName("order_id");
        ordersColumns.add(orderCol1);
        
        ColumnDefinition orderCol2 = new ColumnDefinition();
        orderCol2.setColumnName("customer_id");
        ordersColumns.add(orderCol2);
        
        ColumnDefinition orderCol3 = new ColumnDefinition();
        orderCol3.setColumnName("amount");
        ordersColumns.add(orderCol3);
        
        ColumnDefinition orderCol4 = new ColumnDefinition();
        orderCol4.setColumnName("order_date");
        ordersColumns.add(orderCol4);
        
        ordersTable.setColumnDefinitions(ordersColumns);
        ordersTable.populateColumnIndexMap();
        ordersTable.populateTable();
    }

    @After
    public void tearDown() {
        // Clean up test files
        if (customersFile != null && customersFile.exists()) {
            customersFile.delete();
        }
        if (ordersFile != null && ordersFile.exists()) {
            ordersFile.delete();
        }
        
        // Clean up any temporary hash files
        File[] tempFiles = testDataDir.listFiles((dir, name) -> 
            name.startsWith("hash_temp") || name.startsWith("bucket_"));
        if (tempFiles != null) {
            for (File tempFile : tempFiles) {
                tempFile.delete();
            }
        }
    }

    @Test
    public void testHybridHashJoin() throws IOException {
        // Perform hybrid hash join between customers and orders on customer_id
        Table resultTable = HybridHash.hybridHashJoin(customersTable, ordersTable, 0, 1);
        
        assertNotNull("Result table should not be null", resultTable);
        assertNotNull("Result tuples should not be null", resultTable.getTuples());
        
        // Should have 4 matches (Alice: 2 orders, Bob: 1 order, Charlie: 1 order)
        // Order 105 for customer 6 should not match since customer 6 doesn't exist
        assertEquals("Should have 4 joined tuples", 4, resultTable.getTuples().size());
        
        // Verify that all result tuples contain data from both tables
        for (String tuple : resultTable.getTuples()) {
            String[] parts = tuple.split("\\|");
            assertTrue("Each tuple should have 8 columns (4 from each table)", parts.length == 8);
        }
    }

    @Test
    public void testHybridHashJoinWithNoMatches() throws IOException {
        // Create orders table with no matching customer IDs
        File noMatchOrdersFile = new File(testDataDir, "no_match_orders.tbl");
        try (FileWriter writer = new FileWriter(noMatchOrdersFile)) {
            writer.write("201|10|100.00|2023-01-01\n");
            writer.write("202|11|200.00|2023-01-02\n");
            writer.write("203|12|300.00|2023-01-03\n");
        }
        
        Table noMatchOrdersTable = new Table("no_match_orders", 4, noMatchOrdersFile, testDataDir);
        noMatchOrdersTable.setColumnDefinitions(ordersTable.getColumnDefinitions());
        noMatchOrdersTable.populateColumnIndexMap();
        noMatchOrdersTable.populateTable();
        
        Table resultTable = HybridHash.hybridHashJoin(customersTable, noMatchOrdersTable, 0, 1);
        
        assertNotNull("Result table should not be null", resultTable);
        assertNotNull("Result tuples should not be null", resultTable.getTuples());
        assertEquals("Should have no matches", 0, resultTable.getTuples().size());
        
        // Clean up
        noMatchOrdersFile.delete();
    }

    @Test
    public void testHybridHashJoinWithEmptyTables() throws IOException {
        // Create empty tables
        File emptyCustomersFile = new File(testDataDir, "empty_customers.tbl");
        File emptyOrdersFile = new File(testDataDir, "empty_orders.tbl");
        
        try (FileWriter writer = new FileWriter(emptyCustomersFile)) {
            // Empty file
        }
        try (FileWriter writer = new FileWriter(emptyOrdersFile)) {
            // Empty file
        }
        
        Table emptyCustomersTable = new Table("empty_customers", 4, emptyCustomersFile, testDataDir);
        emptyCustomersTable.setColumnDefinitions(customersTable.getColumnDefinitions());
        emptyCustomersTable.populateColumnIndexMap();
        emptyCustomersTable.populateTable();
        
        Table emptyOrdersTable = new Table("empty_orders", 4, emptyOrdersFile, testDataDir);
        emptyOrdersTable.setColumnDefinitions(ordersTable.getColumnDefinitions());
        emptyOrdersTable.populateColumnIndexMap();
        emptyOrdersTable.populateTable();
        
        Table resultTable = HybridHash.hybridHashJoin(emptyCustomersTable, emptyOrdersTable, 0, 1);
        
        assertNotNull("Result table should not be null", resultTable);
        assertNotNull("Result tuples should not be null", resultTable.getTuples());
        assertEquals("Should have no matches for empty tables", 0, resultTable.getTuples().size());
        
        // Clean up
        emptyCustomersFile.delete();
        emptyOrdersFile.delete();
    }

    @Test
    public void testHybridHashJoinWithSingleMatch() throws IOException {
        // Create a customer table with single record
        File singleCustomerFile = new File(testDataDir, "single_customer.tbl");
        try (FileWriter writer = new FileWriter(singleCustomerFile)) {
            writer.write("1|SingleCustomer|25|IT\n");
        }
        
        // Create orders table with single matching order
        File singleOrderFile = new File(testDataDir, "single_order.tbl");
        try (FileWriter writer = new FileWriter(singleOrderFile)) {
            writer.write("100|1|500.00|2023-01-01\n");
        }
        
        Table singleCustomerTable = new Table("single_customer", 4, singleCustomerFile, testDataDir);
        singleCustomerTable.setColumnDefinitions(customersTable.getColumnDefinitions());
        singleCustomerTable.populateColumnIndexMap();
        singleCustomerTable.populateTable();
        
        Table singleOrderTable = new Table("single_order", 4, singleOrderFile, testDataDir);
        singleOrderTable.setColumnDefinitions(ordersTable.getColumnDefinitions());
        singleOrderTable.populateColumnIndexMap();
        singleOrderTable.populateTable();
        
        Table resultTable = HybridHash.hybridHashJoin(singleCustomerTable, singleOrderTable, 0, 1);
        
        assertNotNull("Result table should not be null", resultTable);
        assertEquals("Should have exactly one match", 1, resultTable.getTuples().size());
        
        String resultTuple = resultTable.getTuples().get(0);
        assertTrue("Result should contain customer data", resultTuple.contains("SingleCustomer"));
        assertTrue("Result should contain order data", resultTuple.contains("500.00"));
        
        // Clean up
        singleCustomerFile.delete();
        singleOrderFile.delete();
    }

    @Test
    public void testHybridHashJoinWithDuplicateKeys() throws IOException {
        // Create customer table with duplicate IDs (edge case)
        File dupCustomersFile = new File(testDataDir, "dup_customers.tbl");
        try (FileWriter writer = new FileWriter(dupCustomersFile)) {
            writer.write("1|Alice1|30|Marketing\n");
            writer.write("1|Alice2|25|Sales\n");
            writer.write("2|Bob|30|Engineering\n");
        }
        
        // Create orders table with orders for customer 1
        File dupOrdersFile = new File(testDataDir, "dup_orders.tbl");
        try (FileWriter writer = new FileWriter(dupOrdersFile)) {
            writer.write("101|1|100.00|2023-01-01\n");
            writer.write("102|1|200.00|2023-01-02\n");
        }
        
        Table dupCustomersTable = new Table("dup_customers", 4, dupCustomersFile, testDataDir);
        dupCustomersTable.setColumnDefinitions(customersTable.getColumnDefinitions());
        dupCustomersTable.populateColumnIndexMap();
        dupCustomersTable.populateTable();
        
        Table dupOrdersTable = new Table("dup_orders", 4, dupOrdersFile, testDataDir);
        dupOrdersTable.setColumnDefinitions(ordersTable.getColumnDefinitions());
        dupOrdersTable.populateColumnIndexMap();
        dupOrdersTable.populateTable();
        
        Table resultTable = HybridHash.hybridHashJoin(dupCustomersTable, dupOrdersTable, 0, 1);
        
        assertNotNull("Result table should not be null", resultTable);
        // Should have 4 matches: 2 customers × 2 orders = 4 combinations
        assertEquals("Should have 4 matches for duplicate keys", 4, resultTable.getTuples().size());
        
        // Clean up
        dupCustomersFile.delete();
        dupOrdersFile.delete();
    }

    @Test(expected = IOException.class) 
    public void testHybridHashJoinWithInvalidJoinColumns() throws IOException {
        // Test with invalid join column indices
        HybridHash.hybridHashJoin(customersTable, ordersTable, 10, 1);
    }

    @Test(expected = IOException.class)
    public void testHybridHashJoinWithNegativeJoinColumns() throws IOException {
        // Test with negative join column indices
        HybridHash.hybridHashJoin(customersTable, ordersTable, -1, 1);
    }

    @Test
    public void testHybridHashJoinPreservesDataIntegrity() throws IOException {
        Table resultTable = HybridHash.hybridHashJoin(customersTable, ordersTable, 0, 1);
        
        assertNotNull("Result table should not be null", resultTable);
        
        // Verify that original tables are not modified
        assertEquals("Original customers table should be unchanged", 5, customersTable.getTuples().size());
        assertEquals("Original orders table should be unchanged", 5, ordersTable.getTuples().size());
        
        // Verify result table structure
        assertNotNull("Result table name should not be null", resultTable.getTableName());
        assertEquals("Result should have combined columns", 
                    customersTable.getNoOfColumns() + ordersTable.getNoOfColumns(), 
                    resultTable.getNoOfColumns());
    }

    @Test
    public void testHybridHashJoinWithLargeDataset() throws IOException {
        // Create larger dataset to test performance characteristics
        File largeCustomersFile = new File(testDataDir, "large_customers.tbl");
        File largeOrdersFile = new File(testDataDir, "large_orders.tbl");
        
        // Create 100 customers
        try (FileWriter writer = new FileWriter(largeCustomersFile)) {
            for (int i = 1; i <= 100; i++) {
                writer.write(i + "|Customer" + i + "|" + (20 + i % 40) + "|Dept" + (i % 5) + "\n");
            }
        }
        
        // Create 200 orders (some customers have multiple orders)
        try (FileWriter writer = new FileWriter(largeOrdersFile)) {
            for (int i = 1; i <= 200; i++) {
                int customerId = (i % 100) + 1; // Ensures all customers have at least one order
                writer.write((1000 + i) + "|" + customerId + "|" + (100.0 + i) + "|2023-01-" + 
                           String.format("%02d", (i % 28) + 1) + "\n");
            }
        }
        
        Table largeCustomersTable = new Table("large_customers", 4, largeCustomersFile, testDataDir);
        largeCustomersTable.setColumnDefinitions(customersTable.getColumnDefinitions());
        largeCustomersTable.populateColumnIndexMap();
        largeCustomersTable.populateTable();
        
        Table largeOrdersTable = new Table("large_orders", 4, largeOrdersFile, testDataDir);
        largeOrdersTable.setColumnDefinitions(ordersTable.getColumnDefinitions());
        largeOrdersTable.populateColumnIndexMap();
        largeOrdersTable.populateTable();
        
        Table resultTable = HybridHash.hybridHashJoin(largeCustomersTable, largeOrdersTable, 0, 1);
        
        assertNotNull("Result table should not be null", resultTable);
        assertEquals("Should have 200 matches (all orders should match)", 200, resultTable.getTuples().size());
        
        // Clean up
        largeCustomersFile.delete();
        largeOrdersFile.delete();
    }
}