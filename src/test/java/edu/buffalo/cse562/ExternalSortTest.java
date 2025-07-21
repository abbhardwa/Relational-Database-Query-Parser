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

public class ExternalSortTest {
    private File testDataDir;
    private File dataFile;
    private Table table;

    @Before
    public void setUp() throws IOException {
        // Create test data directory and file
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        dataFile = new File(testDataDir, "sort_test.tbl");
        
        // Create test data with unsorted values
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("3|Charlie|35|6000\n");
            writer.write("1|Alice|25|4000\n");
            writer.write("4|David|40|7000\n");
            writer.write("2|Bob|30|5000\n");
            writer.write("5|Eve|28|4500\n");
        }

        // Set up table
        table = new Table("sort_test", 4, dataFile, testDataDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("id");
        columns.add(col1);
        
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("name");
        columns.add(col2);
        
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("age");
        columns.add(col3);
        
        ColumnDefinition col4 = new ColumnDefinition();
        col4.setColumnName("salary");
        columns.add(col4);
        
        table.setColumnDefinitions(columns);
        table.populateColumnIndexMap();
        table.populateTable();
    }

    @After
    public void tearDown() {
        // Clean up test files
        if (dataFile != null && dataFile.exists()) {
            dataFile.delete();
        }
        
        // Clean up any temporary sort files
        File[] tempFiles = testDataDir.listFiles((dir, name) -> 
            name.startsWith("sort_test_temp") || name.startsWith("sorted_"));
        if (tempFiles != null) {
            for (File tempFile : tempFiles) {
                tempFile.delete();
            }
        }
    }

    @Test
    public void testExternalSortById() throws IOException {
        // Test external sort by id column (index 0)
        Table sortedTable = ExternalSort.externalSort(table, 0, true);
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Should have same number of rows", 5, sortedTable.getTuples().size());
        
        // Verify sorting order (ascending by id)
        assertEquals("First tuple should be Alice", "1|Alice|25|4000", sortedTable.getTuples().get(0));
        assertEquals("Second tuple should be Bob", "2|Bob|30|5000", sortedTable.getTuples().get(1));
        assertEquals("Third tuple should be Charlie", "3|Charlie|35|6000", sortedTable.getTuples().get(2));
        assertEquals("Fourth tuple should be David", "4|David|40|7000", sortedTable.getTuples().get(3));
        assertEquals("Fifth tuple should be Eve", "5|Eve|28|4500", sortedTable.getTuples().get(4));
    }

    @Test
    public void testExternalSortByIdDescending() throws IOException {
        // Test external sort by id column in descending order
        Table sortedTable = ExternalSort.externalSort(table, 0, false);
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Should have same number of rows", 5, sortedTable.getTuples().size());
        
        // Verify sorting order (descending by id)
        assertEquals("First tuple should be Eve", "5|Eve|28|4500", sortedTable.getTuples().get(0));
        assertEquals("Second tuple should be David", "4|David|40|7000", sortedTable.getTuples().get(1));
        assertEquals("Third tuple should be Charlie", "3|Charlie|35|6000", sortedTable.getTuples().get(2));
        assertEquals("Fourth tuple should be Bob", "2|Bob|30|5000", sortedTable.getTuples().get(3));
        assertEquals("Fifth tuple should be Alice", "1|Alice|25|4000", sortedTable.getTuples().get(4));
    }

    @Test
    public void testExternalSortByName() throws IOException {
        // Test external sort by name column (index 1)
        Table sortedTable = ExternalSort.externalSort(table, 1, true);
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Should have same number of rows", 5, sortedTable.getTuples().size());
        
        // Verify sorting order (ascending by name)
        assertEquals("First tuple should be Alice", "1|Alice|25|4000", sortedTable.getTuples().get(0));
        assertEquals("Second tuple should be Bob", "2|Bob|30|5000", sortedTable.getTuples().get(1));
        assertEquals("Third tuple should be Charlie", "3|Charlie|35|6000", sortedTable.getTuples().get(2));
        assertEquals("Fourth tuple should be David", "4|David|40|7000", sortedTable.getTuples().get(3));
        assertEquals("Fifth tuple should be Eve", "5|Eve|28|4500", sortedTable.getTuples().get(4));
    }

    @Test
    public void testExternalSortByAge() throws IOException {
        // Test external sort by age column (index 2)
        Table sortedTable = ExternalSort.externalSort(table, 2, true);
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Should have same number of rows", 5, sortedTable.getTuples().size());
        
        // Verify sorting order (ascending by age)
        assertEquals("First tuple should be Alice (25)", "1|Alice|25|4000", sortedTable.getTuples().get(0));
        assertEquals("Second tuple should be Eve (28)", "5|Eve|28|4500", sortedTable.getTuples().get(1));
        assertEquals("Third tuple should be Bob (30)", "2|Bob|30|5000", sortedTable.getTuples().get(2));
        assertEquals("Fourth tuple should be Charlie (35)", "3|Charlie|35|6000", sortedTable.getTuples().get(3));
        assertEquals("Fifth tuple should be David (40)", "4|David|40|7000", sortedTable.getTuples().get(4));
    }

    @Test
    public void testExternalSortBySalary() throws IOException {
        // Test external sort by salary column (index 3)
        Table sortedTable = ExternalSort.externalSort(table, 3, true);
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Should have same number of rows", 5, sortedTable.getTuples().size());
        
        // Verify sorting order (ascending by salary)
        assertEquals("First tuple should be Alice (4000)", "1|Alice|25|4000", sortedTable.getTuples().get(0));
        assertEquals("Second tuple should be Eve (4500)", "5|Eve|28|4500", sortedTable.getTuples().get(1));
        assertEquals("Third tuple should be Bob (5000)", "2|Bob|30|5000", sortedTable.getTuples().get(2));
        assertEquals("Fourth tuple should be Charlie (6000)", "3|Charlie|35|6000", sortedTable.getTuples().get(3));
        assertEquals("Fifth tuple should be David (7000)", "4|David|40|7000", sortedTable.getTuples().get(4));
    }

    @Test
    public void testExternalSortEmptyTable() throws IOException {
        // Create empty table
        try (FileWriter writer = new FileWriter(dataFile)) {
            // Write nothing - empty file
        }
        table.populateTable();

        Table sortedTable = ExternalSort.externalSort(table, 0, true);
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertTrue("Sorted table should be empty", sortedTable.getTuples().isEmpty());
    }

    @Test
    public void testExternalSortSingleRecord() throws IOException {
        // Create single record table
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|OnlyOne|30|5000\n");
        }
        table.populateTable();

        Table sortedTable = ExternalSort.externalSort(table, 0, true);
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Should have one record", 1, sortedTable.getTuples().size());
        assertEquals("Record should be unchanged", "1|OnlyOne|30|5000", sortedTable.getTuples().get(0));
    }

    @Test
    public void testExternalSortDuplicateValues() throws IOException {
        // Create table with duplicate values
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("2|Bob|30|5000\n");
            writer.write("1|Alice|30|5000\n");
            writer.write("3|Charlie|30|5000\n");
            writer.write("1|Alice2|30|5000\n");
        }
        table.populateTable();

        Table sortedTable = ExternalSort.externalSort(table, 2, true); // Sort by age (all 30)
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Should have 4 records", 4, sortedTable.getTuples().size());
        
        // All records should have age 30, order for equal values may vary
        for (String tuple : sortedTable.getTuples()) {
            assertTrue("All tuples should have age 30", tuple.contains("|30|"));
        }
    }

    @Test
    public void testExternalSortLargeNumbers() throws IOException {
        // Create table with large numeric values
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1000000|User1|25|1000000\n");
            writer.write("500000|User2|30|500000\n");
            writer.write("2000000|User3|35|2000000\n");
            writer.write("1|User4|40|1\n");
        }
        table.populateTable();

        Table sortedTable = ExternalSort.externalSort(table, 3, true); // Sort by salary
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Should have 4 records", 4, sortedTable.getTuples().size());
        
        // Verify sorting with large numbers
        assertEquals("First should be smallest salary", "1|User4|40|1", sortedTable.getTuples().get(0));
        assertEquals("Last should be largest salary", "2000000|User3|35|2000000", sortedTable.getTuples().get(3));
    }

    @Test(expected = IOException.class)
    public void testExternalSortInvalidColumnIndex() throws IOException {
        // Test with invalid column index
        ExternalSort.externalSort(table, 10, true);
    }

    @Test(expected = IOException.class)
    public void testExternalSortNegativeColumnIndex() throws IOException {
        // Test with negative column index
        ExternalSort.externalSort(table, -1, true);
    }

    @Test
    public void testExternalSortPreservesTableStructure() throws IOException {
        Table sortedTable = ExternalSort.externalSort(table, 0, true);
        
        assertNotNull("Sorted table should not be null", sortedTable);
        assertEquals("Table name should be preserved", table.getTableName(), sortedTable.getTableName());
        assertEquals("Number of columns should be preserved", table.getNoOfColumns(), sortedTable.getNoOfColumns());
        assertEquals("Column definitions should be preserved", 
                    table.getColumnDefinitions().size(), 
                    sortedTable.getColumnDefinitions().size());
    }
}