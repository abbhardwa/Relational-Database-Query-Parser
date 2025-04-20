package edu.buffalo.cse562;

import static org.junit.Assert.*;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExternalSortTest {
    private Table testTable;
    private File tempDir;
    
    @Before
    public void setUp() throws IOException {
        // Create temporary directory for test data
        tempDir = new File("src/test/resources/testdata");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        
        // Create a test table file
        File tableFile = new File(tempDir, "sort_test.tbl");
        if (!tableFile.exists()) {
            tableFile.createNewFile();
        }
        
        // Write test data
        try (FileWriter writer = new FileWriter(tableFile)) {
            writer.write("3|30|300\n");
            writer.write("1|10|100\n");
            writer.write("2|20|200\n");
            writer.write("5|50|500\n");
            writer.write("4|40|400\n");
        }
        
        // Create Table object
        testTable = new Table("sort_test", 3, tableFile, tempDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("col1");
        ColDataType col1Type = new ColDataType();
        col1Type.setDataType("int");
        col1.setColDataType(col1Type);
        
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("col2");
        ColDataType col2Type = new ColDataType();
        col2Type.setDataType("int");
        col2.setColDataType(col2Type);
        
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("col3");
        ColDataType col3Type = new ColDataType();
        col3Type.setDataType("int");
        col3.setColDataType(col3Type);
        
        columns.add(col1);
        columns.add(col2);
        columns.add(col3);
        
        testTable.setColumnDefinitions(columns);
        
        // Set up column index map
        testTable.populateColumnIndexMap();
        
        // Load table data
        testTable.populateTable();
    }
    
    @Test
    public void testPerformExternalMergeSort() throws IOException {
        // Create order by list for sorting by first column
        List orderByList = Arrays.asList("col1");
        
        // Perform external sort
        Table sortedTable = ExternalSort.performExternalMergeSort(testTable, orderByList);
        
        // Verify the result
        assertNotNull(sortedTable);
        ArrayList<String> tuples = sortedTable.getTuples();
        assertNotNull(tuples);
        assertEquals(5, tuples.size());
        
        // Check if rows are sorted by col1
        assertEquals("1|10|100", tuples.get(0));
        assertEquals("2|20|200", tuples.get(1));
        assertEquals("3|30|300", tuples.get(2));
        assertEquals("4|40|400", tuples.get(3));
        assertEquals("5|50|500", tuples.get(4));
    }
    
    @Test
    public void testSortingWithMultipleColumns() throws IOException {
        // Create test data with duplicate first column values
        File tableFile = new File(tempDir, "multi_sort_test.tbl");
        if (!tableFile.exists()) {
            tableFile.createNewFile();
        }
        
        // Write test data with duplicate col1 values
        try (FileWriter writer = new FileWriter(tableFile)) {
            writer.write("1|30|300\n");
            writer.write("1|10|100\n");
            writer.write("2|20|200\n");
            writer.write("2|50|500\n");
            writer.write("1|40|400\n");
        }
        
        // Create Table object
        Table multiSortTable = new Table("multi_sort_test", 3, tableFile, tempDir);
        
        // Set up column definitions - copy from testTable
        multiSortTable.setColumnDefinitions(testTable.getColumnDefinitions());
        
        // Set up column index map
        multiSortTable.populateColumnIndexMap();
        
        // Load table data
        multiSortTable.populateTable();
        
        // Create order by list for sorting by multiple columns (col1 ASC, col2 ASC)
        List orderByList = Arrays.asList("col1", "col2");
        
        // Perform external sort
        Table sortedTable = ExternalSort.performExternalMergeSort(multiSortTable, orderByList);
        
        // Verify the result
        assertNotNull(sortedTable);
        ArrayList<String> tuples = sortedTable.getTuples();
        assertNotNull(tuples);
        assertEquals(5, tuples.size());
        
        // Check if rows are sorted by col1, then col2
        assertEquals("1|10|100", tuples.get(0));
        assertEquals("1|30|300", tuples.get(1));
        assertEquals("1|40|400", tuples.get(2));
        assertEquals("2|20|200", tuples.get(3));
        assertEquals("2|50|500", tuples.get(4));
    }
    
    @Test
    public void testSortingWithDescendingOrder() throws IOException {
        // Create order by list for sorting by first column in descending order
        List orderByList = Arrays.asList("col1 DESC");
        
        // Perform external sort
        Table sortedTable = ExternalSort.performExternalMergeSort(testTable, orderByList);
        
        // Verify the result
        assertNotNull(sortedTable);
        ArrayList<String> tuples = sortedTable.getTuples();
        assertNotNull(tuples);
        assertEquals(5, tuples.size());
        
        // Check if rows are sorted by col1 in descending order
        assertEquals("5|50|500", tuples.get(0));
        assertEquals("4|40|400", tuples.get(1));
        assertEquals("3|30|300", tuples.get(2));
        assertEquals("2|20|200", tuples.get(3));
        assertEquals("1|10|100", tuples.get(4));
    }
    
    @Test
    public void testSortingWithEmptyTable() throws IOException {
        // Create an empty table
        File emptyFile = new File(tempDir, "empty_sort_test.tbl");
        if (!emptyFile.exists()) {
            emptyFile.createNewFile();
        }
        
        // Create Table object
        Table emptyTable = new Table("empty_sort_test", 3, emptyFile, tempDir);
        
        // Set up column definitions - copy from testTable
        emptyTable.setColumnDefinitions(testTable.getColumnDefinitions());
        
        // Set up column index map
        emptyTable.populateColumnIndexMap();
        
        // Load table data (will be empty)
        emptyTable.populateTable();
        
        // Create order by list
        List orderByList = Arrays.asList("col1");
        
        // Perform external sort
        Table sortedTable = ExternalSort.performExternalMergeSort(emptyTable, orderByList);
        
        // Verify the result
        assertNotNull(sortedTable);
        ArrayList<String> tuples = sortedTable.getTuples();
        assertNotNull(tuples);
        assertEquals(0, tuples.size());
    }
    
    @After
    public void tearDown() {
        // Clean up temporary files
        File sortTestFile = new File(tempDir, "sort_test.tbl");
        if (sortTestFile.exists()) {
            sortTestFile.delete();
        }
        
        File multiSortTestFile = new File(tempDir, "multi_sort_test.tbl");
        if (multiSortTestFile.exists()) {
            multiSortTestFile.delete();
        }
        
        File emptySortTestFile = new File(tempDir, "empty_sort_test.tbl");
        if (emptySortTestFile.exists()) {
            emptySortTestFile.delete();
        }
        
        // Delete any temporary files created during external sort
        for (File file : tempDir.listFiles()) {
            if (file.getName().startsWith("temp_outside") || 
                file.getName().startsWith("final_table")) {
                file.delete();
            }
        }
    }
}