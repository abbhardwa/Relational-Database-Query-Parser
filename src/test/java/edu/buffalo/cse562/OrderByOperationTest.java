/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class OrderByOperationTest {
    private File testDataDir;
    private File dataFile;
    private Table table;
    private OrderByOperation orderByOp;

    @Before
    public void setUp() throws IOException {
        // Create test data directory and file
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        dataFile = new File(testDataDir, "orders_sort.tbl");
        
        // Create test data
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("3|30|300\n");
            writer.write("1|20|100\n");
            writer.write("2|10|200\n");
            writer.write("4|40|150\n");
        }

        // Set up table
        table = new Table("orders", 3, dataFile, testDataDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("id");
        columns.add(col1);
        
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("quantity");
        columns.add(col2);
        
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("total");
        columns.add(col3);
        
        table.setColumnDefinitions(columns);
        table.populateColumnIndexMap();
        table.populateTable();
    }

    @Test
    public void testOrderByAscending() {
        orderByOp = new OrderByOperation(table, 0, true); // Sort by id ascending
        Table result = orderByOp.sort();

        assertNotNull("Result table should not be null", result);
        ArrayList<String> tuples = result.getTuples();
        assertEquals("Should contain all records", 4, tuples.size());
        assertEquals("First record should be id=1", "1|20|100", tuples.get(0));
        assertEquals("Last record should be id=4", "4|40|150", tuples.get(3));
    }

    @Test
    public void testOrderByDescending() {
        orderByOp = new OrderByOperation(table, 0, false); // Sort by id descending
        Table result = orderByOp.sort();

        assertNotNull("Result table should not be null", result);
        ArrayList<String> tuples = result.getTuples();
        assertEquals("Should contain all records", 4, tuples.size());
        assertEquals("First record should be id=4", "4|40|150", tuples.get(0));
        assertEquals("Last record should be id=1", "1|20|100", tuples.get(3));
    }

    @Test
    public void testOrderByDifferentColumn() {
        orderByOp = new OrderByOperation(table, 1, true); // Sort by quantity ascending
        Table result = orderByOp.sort();

        assertNotNull("Result table should not be null", result);
        ArrayList<String> tuples = result.getTuples();
        assertEquals("Should contain all records", 4, tuples.size());
        assertEquals("First record should have quantity=10", "2|10|200", tuples.get(0));
        assertEquals("Last record should have quantity=40", "4|40|150", tuples.get(3));
    }

    @Test
    public void testOrderByWithEmptyTable() throws IOException {
        // Create empty table
        try (FileWriter writer = new FileWriter(dataFile)) {}
        table.populateTable();

        orderByOp = new OrderByOperation(table, 0, true);
        Table result = orderByOp.sort();

        assertNotNull("Result table should not be null", result);
        assertTrue("Result should be empty", result.getTuples().isEmpty());
    }

    @Test
    public void testOrderBySingleRecord() throws IOException {
        // Create table with single record
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|10|100\n");
        }
        table.populateTable();

        orderByOp = new OrderByOperation(table, 0, true);
        Table result = orderByOp.sort();

        assertNotNull("Result table should not be null", result);
        assertEquals("Should contain one record", 1, result.getTuples().size());
        assertEquals("Record should be unchanged", "1|10|100", result.getTuples().get(0));
    }
}