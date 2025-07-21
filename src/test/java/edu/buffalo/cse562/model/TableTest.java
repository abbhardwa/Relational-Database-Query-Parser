package edu.buffalo.cse562.model;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class TableTest {
    private Table table;
    private File testDataDir;
    private File testFile;
    
    @Before
    public void setUp() {
        testDataDir = new File("src/test/resources/testdata");
        testFile = new File(testDataDir, "test.tbl");
        table = new Table("test", 3, testFile, testDataDir);
        
        // Create column definitions
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("col1");
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("col2");
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("col3");
        
        columns.add(col1);
        columns.add(col2);
        columns.add(col3);
        
        table.setColumnDefinitions(columns);
    }
    
    @Test
    public void testTableConstructor() {
        assertEquals("test", table.getTableName());
        assertEquals(3, table.getColumnCount());
        assertEquals(testFile, table.getDataFile());
        assertEquals(testDataDir, table.getDataDirectory());
        assertTrue(table.getTuples().isEmpty());
    }
    
    @Test
    public void testTableCopyConstructor() {
        Table clonedTable = new Table(table);
        
        assertEquals(table.getTableName(), clonedTable.getTableName());
        assertEquals(table.getColumnCount(), clonedTable.getColumnCount());
        assertEquals(table.getDataFile(), clonedTable.getDataFile());
        assertEquals(table.getDataDirectory(), clonedTable.getDataDirectory());
        assertEquals(table.getColumnDefinitions(), clonedTable.getColumnDefinitions());
    }
    
    @Test
    public void testPopulateTable() throws IOException {
        table.populateTable();
        ArrayList<String> tuples = table.getTuples();
        
        assertEquals(2, tuples.size());
        assertEquals("line1|value1|123", tuples.get(0));
        assertEquals("line2|value2|456", tuples.get(1));
    }
    
    @Test
    public void testPopulateColumnIndexMap() {
        table.populateColumnIndexMap();
        
        assertEquals(Integer.valueOf(0), table.getColumnIndexMap().get("col1"));
        assertEquals(Integer.valueOf(1), table.getColumnIndexMap().get("col2"));
        assertEquals(Integer.valueOf(2), table.getColumnIndexMap().get("col3"));
    }
    
    @Test
    public void testBufferedReaderOperations() throws IOException {
        table.initializeBufferedReader();
        
        String firstTuple = table.getNextTuple();
        assertEquals("line1|value1|123", firstTuple);
        
        String secondTuple = table.getNextTuple();
        assertEquals("line2|value2|456", secondTuple);
        
        String thirdTuple = table.getNextTuple();
        assertNull(thirdTuple);
        
        table.closeReaders();
    }
    
    @Test(expected = IOException.class)
    public void testPopulateTableWithNonexistentFile() throws IOException {
        Table invalidTable = new Table("invalid", 3, 
            new File(testDataDir, "nonexistent.tbl"), testDataDir);
        invalidTable.populateTable();
    }
    
    @Test
    public void testSetGetTuples() {
        ArrayList<String> newTuples = new ArrayList<>();
        newTuples.add("test1|a|1");
        newTuples.add("test2|b|2");
        
        table.setTuples(newTuples);
        assertEquals(newTuples, table.getTuples());
    }

    @Test
    public void testTableWithDifferentColumnCounts() {
        Table table2 = new Table("test2", 2, testFile, testDataDir);
        assertEquals("test2", table2.getTableName());
        assertEquals(2, table2.getNoOfColumns());
    }

    @Test
    public void testTableCopyConstructorWithNullData() {
        Table originalTable = new Table("original", 3, testFile, testDataDir);
        Table copiedTable = new Table(originalTable);
        
        assertEquals(originalTable.getTableName(), copiedTable.getTableName());
        assertEquals(originalTable.getNoOfColumns(), copiedTable.getNoOfColumns());
        assertEquals(originalTable.getTableDataDirectoryPath(), copiedTable.getTableDataDirectoryPath());
    }

    @Test
    public void testColumnIndexMapWithEmptyColumns() {
        Table emptyTable = new Table("empty", 0, testFile, testDataDir);
        emptyTable.populateColumnIndexMap();
        
        assertNotNull(emptyTable.getColumnIndexMap());
        assertTrue(emptyTable.getColumnIndexMap().isEmpty());
    }

    @Test
    public void testColumnIndexMapWithDuplicateColumnNames() {
        // Test behavior with duplicate column names
        ArrayList<ColumnDefinition> duplicateColumns = new ArrayList<>();
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("duplicate");
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("duplicate");
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("unique");
        
        duplicateColumns.add(col1);
        duplicateColumns.add(col2);
        duplicateColumns.add(col3);
        
        table.setColumnDefinitions(duplicateColumns);
        table.populateColumnIndexMap();
        
        // Should have entries for all columns (behavior may vary for duplicates)
        assertNotNull(table.getColumnIndexMap());
        assertTrue(table.getColumnIndexMap().containsKey("duplicate"));
        assertTrue(table.getColumnIndexMap().containsKey("unique"));
    }

    @Test
    public void testGetNextTupleWithoutInitialization() throws IOException {
        // Test getting tuple without initializing buffered reader first
        String tuple = table.getNextTuple();
        // Should handle gracefully (either return null or auto-initialize)
        // The exact behavior depends on implementation
    }

    @Test
    public void testCloseReadersMultipleTimes() throws IOException {
        table.initializeBufferedReader();
        table.closeReaders();
        
        // Should not throw exception when called multiple times
        table.closeReaders();
        table.closeReaders();
    }

    @Test
    public void testTableNameCaseHandling() {
        Table upperCaseTable = new Table("UPPERCASE", 3, testFile, testDataDir);
        Table lowerCaseTable = new Table("lowercase", 3, testFile, testDataDir);
        Table mixedCaseTable = new Table("MixedCase", 3, testFile, testDataDir);
        
        assertEquals("UPPERCASE", upperCaseTable.getTableName());
        assertEquals("lowercase", lowerCaseTable.getTableName());
        assertEquals("MixedCase", mixedCaseTable.getTableName());
    }

    @Test
    public void testTableWithZeroColumns() {
        Table zeroColumnTable = new Table("zero", 0, testFile, testDataDir);
        assertEquals(0, zeroColumnTable.getNoOfColumns());
        
        zeroColumnTable.setColumnDefinitions(new ArrayList<>());
        zeroColumnTable.populateColumnIndexMap();
        
        assertNotNull(zeroColumnTable.getColumnDefinitions());
        assertEquals(0, zeroColumnTable.getColumnDefinitions().size());
    }

    @Test
    public void testPopulateTableWithLargeFile() throws IOException {
        // Create a large test file
        File largeFile = new File(testDataDir, "large_test.tbl");
        try (FileWriter writer = new FileWriter(largeFile)) {
            for (int i = 0; i < 1000; i++) {
                writer.write("line" + i + "|value" + i + "|" + i + "\n");
            }
        }
        
        Table largeTable = new Table("large", 3, largeFile, testDataDir);
        largeTable.populateTable();
        
        assertEquals(1000, largeTable.getTuples().size());
        assertEquals("line0|value0|0", largeTable.getTuples().get(0));
        assertEquals("line999|value999|999", largeTable.getTuples().get(999));
        
        // Clean up
        largeFile.delete();
    }

    @Test
    public void testPopulateTableWithEmptyFile() throws IOException {
        // Create an empty test file
        File emptyFile = new File(testDataDir, "empty_test.tbl");
        try (FileWriter writer = new FileWriter(emptyFile)) {
            // Write nothing
        }
        
        Table emptyTable = new Table("empty", 3, emptyFile, testDataDir);
        emptyTable.populateTable();
        
        assertNotNull(emptyTable.getTuples());
        assertEquals(0, emptyTable.getTuples().size());
        
        // Clean up
        emptyFile.delete();
    }

    @Test
    public void testPopulateTableWithSpecialCharacters() throws IOException {
        // Create test file with special characters
        File specialFile = new File(testDataDir, "special_test.tbl");
        try (FileWriter writer = new FileWriter(specialFile)) {
            writer.write("data with spaces|symbols!@#$%|123\n");
            writer.write("unicode|éññüñ|456\n");
            writer.write("tabs\tand\ttabs|newlines|789\n");
        }
        
        Table specialTable = new Table("special", 3, specialFile, testDataDir);
        specialTable.populateTable();
        
        assertEquals(3, specialTable.getTuples().size());
        assertEquals("data with spaces|symbols!@#$%|123", specialTable.getTuples().get(0));
        assertEquals("unicode|éññüñ|456", specialTable.getTuples().get(1));
        
        // Clean up
        specialFile.delete();
    }

    @Test
    public void testBufferedReaderWithEmptyFile() throws IOException {
        File emptyFile = new File(testDataDir, "empty_reader_test.tbl");
        try (FileWriter writer = new FileWriter(emptyFile)) {
            // Create empty file
        }
        
        Table emptyTable = new Table("empty_reader", 3, emptyFile, testDataDir);
        emptyTable.initializeBufferedReader();
        
        String tuple = emptyTable.getNextTuple();
        assertNull("Should return null for empty file", tuple);
        
        emptyTable.closeReaders();
        
        // Clean up
        emptyFile.delete();
    }
    
    @After
    public void tearDown() {
        try {
            table.closeReaders();
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }
}