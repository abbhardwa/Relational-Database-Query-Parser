/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.operations.SelectionOperation;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class SelectionOperationTest {
    private File testDataDir;
    private File dataFile;
    private Table table;

    @Before
    public void setUp() throws IOException {
        // Create test data directory and file
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        dataFile = new File(testDataDir, "products.tbl");
        
        // Create test data
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|Product A|10.00|100\n");
            writer.write("2|Product B|20.00|200\n");
            writer.write("3|Product C|30.00|300\n");
            writer.write("4|Product D|40.00|400\n");
        }

        // Set up table
        table = new Table("products", 4, dataFile, testDataDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("id");
        columns.add(col1);
        
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("name");
        columns.add(col2);
        
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("price");
        columns.add(col3);
        
        ColumnDefinition col4 = new ColumnDefinition();
        col4.setColumnName("stock");
        columns.add(col4);
        
        table.setColumnDefinitions(columns);
        table.populateColumnIndexMap();
        table.populateTable();
    }

    @Test
    public void testSelectSingleColumn() {
        int[] columnIndices = {0}; // Select only id
        SelectionOperation selectOp = new SelectionOperation(table, columnIndices);
        Table result = selectOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have same number of rows", 4, result.getTuples().size());
        assertEquals("Should only contain id column", "1", result.getTuples().get(0));
        assertEquals("Should only contain id column", "2", result.getTuples().get(1));
    }

    @Test
    public void testSelectMultipleColumns() {
        int[] columnIndices = {0, 1}; // Select id and name
        SelectionOperation selectOp = new SelectionOperation(table, columnIndices);
        Table result = selectOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have same number of rows", 4, result.getTuples().size());
        assertEquals("Should contain id and name columns", "1|Product A", result.getTuples().get(0));
        assertEquals("Should contain id and name columns", "2|Product B", result.getTuples().get(1));
    }

    @Test
    public void testSelectAllColumns() {
        int[] columnIndices = {0, 1, 2, 3}; // Select all columns
        SelectionOperation selectOp = new SelectionOperation(table, columnIndices);
        Table result = selectOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have same number of rows", 4, result.getTuples().size());
        assertEquals("Should contain all columns", table.getTuples().get(0), result.getTuples().get(0));
        assertEquals("Should contain all columns", table.getTuples().get(1), result.getTuples().get(1));
    }

    @Test
    public void testSelectNonSequentialColumns() {
        int[] columnIndices = {0, 2}; // Select id and price
        SelectionOperation selectOp = new SelectionOperation(table, columnIndices);
        Table result = selectOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have same number of rows", 4, result.getTuples().size());
        assertEquals("Should contain id and price columns", "1|10.00", result.getTuples().get(0));
        assertEquals("Should contain id and price columns", "2|20.00", result.getTuples().get(1));
    }

    @Test
    public void testEmptyTable() throws IOException {
        // Create empty table
        try (FileWriter writer = new FileWriter(dataFile)) {}
        table.populateTable();

        int[] columnIndices = {0, 1}; // Select id and name
        SelectionOperation selectOp = new SelectionOperation(table, columnIndices);
        Table result = selectOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be empty", result.getTuples().isEmpty());
    }

    @Test
    public void testSingleRecord() throws IOException {
        // Create table with single record
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|Product A|10.00|100\n");
        }
        table.populateTable();

        int[] columnIndices = {0, 1}; // Select id and name
        SelectionOperation selectOp = new SelectionOperation(table, columnIndices);
        Table result = selectOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have one row", 1, result.getTuples().size());
        assertEquals("Should contain id and name columns", "1|Product A", result.getTuples().get(0));
    }
}