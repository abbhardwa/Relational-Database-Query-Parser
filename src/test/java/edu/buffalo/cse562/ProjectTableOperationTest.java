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

public class ProjectTableOperationTest {
    private File testDataDir;
    private File dataFile;
    private Table table;

    @Before
    public void setUp() throws IOException {
        // Create test data directory and file
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        dataFile = new File(testDataDir, "employees_proj.tbl");
        
        // Create test data
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|John|HR|50000|30\n");
            writer.write("2|Jane|IT|60000|28\n");
            writer.write("3|Bob|HR|55000|35\n");
            writer.write("4|Alice|IT|65000|32\n");
        }

        // Set up table
        table = new Table("employees", 5, dataFile, testDataDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("id");
        columns.add(col1);
        
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("name");
        columns.add(col2);
        
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("dept");
        columns.add(col3);
        
        ColumnDefinition col4 = new ColumnDefinition();
        col4.setColumnName("salary");
        columns.add(col4);
        
        ColumnDefinition col5 = new ColumnDefinition();
        col5.setColumnName("age");
        columns.add(col5);
        
        table.setColumnDefinitions(columns);
        table.populateColumnIndexMap();
        table.populateTable();
    }

    @Test
    public void testProjectSingleColumn() {
        ArrayList<String> projectionColumns = new ArrayList<>();
        projectionColumns.add("name");
        
        ProjectTableOperation projectOp = new ProjectTableOperation(table, projectionColumns);
        Table result = projectOp.project();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have same number of rows", 4, result.getTuples().size());
        assertEquals("Should only contain name", "John", result.getTuples().get(0));
        assertEquals("Should only contain name", "Jane", result.getTuples().get(1));
    }

    @Test
    public void testProjectMultipleColumns() {
        ArrayList<String> projectionColumns = new ArrayList<>();
        projectionColumns.add("name");
        projectionColumns.add("dept");
        projectionColumns.add("salary");
        
        ProjectTableOperation projectOp = new ProjectTableOperation(table, projectionColumns);
        Table result = projectOp.project();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have same number of rows", 4, result.getTuples().size());
        assertEquals("Should contain name, dept, salary", "John|HR|50000", result.getTuples().get(0));
        assertEquals("Should contain name, dept, salary", "Jane|IT|60000", result.getTuples().get(1));
    }

    @Test
    public void testProjectAllColumns() {
        ArrayList<String> projectionColumns = new ArrayList<>();
        projectionColumns.add("id");
        projectionColumns.add("name");
        projectionColumns.add("dept");
        projectionColumns.add("salary");
        projectionColumns.add("age");
        
        ProjectTableOperation projectOp = new ProjectTableOperation(table, projectionColumns);
        Table result = projectOp.project();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have same number of rows", 4, result.getTuples().size());
        assertEquals("Should contain all columns", table.getTuples().get(0), result.getTuples().get(0));
        assertEquals("Should contain all columns", table.getTuples().get(1), result.getTuples().get(1));
    }

    @Test
    public void testProjectReorderedColumns() {
        ArrayList<String> projectionColumns = new ArrayList<>();
        projectionColumns.add("salary");
        projectionColumns.add("name");
        projectionColumns.add("dept");
        
        ProjectTableOperation projectOp = new ProjectTableOperation(table, projectionColumns);
        Table result = projectOp.project();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have same number of rows", 4, result.getTuples().size());
        assertEquals("Should contain reordered columns", "50000|John|HR", result.getTuples().get(0));
        assertEquals("Should contain reordered columns", "60000|Jane|IT", result.getTuples().get(1));
    }

    @Test
    public void testEmptyTable() throws IOException {
        // Create empty table
        try (FileWriter writer = new FileWriter(dataFile)) {}
        table.populateTable();

        ArrayList<String> projectionColumns = new ArrayList<>();
        projectionColumns.add("name");
        projectionColumns.add("dept");
        
        ProjectTableOperation projectOp = new ProjectTableOperation(table, projectionColumns);
        Table result = projectOp.project();

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be empty", result.getTuples().isEmpty());
    }

    @Test
    public void testSingleRecord() throws IOException {
        // Create table with single record
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|John|HR|50000|30\n");
        }
        table.populateTable();

        ArrayList<String> projectionColumns = new ArrayList<>();
        projectionColumns.add("name");
        projectionColumns.add("dept");
        
        ProjectTableOperation projectOp = new ProjectTableOperation(table, projectionColumns);
        Table result = projectOp.project();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have one row", 1, result.getTuples().size());
        assertEquals("Should contain projected columns", "John|HR", result.getTuples().get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidColumn() {
        ArrayList<String> projectionColumns = new ArrayList<>();
        projectionColumns.add("nonexistent_column");
        
        ProjectTableOperation projectOp = new ProjectTableOperation(table, projectionColumns);
        projectOp.project();
    }
}