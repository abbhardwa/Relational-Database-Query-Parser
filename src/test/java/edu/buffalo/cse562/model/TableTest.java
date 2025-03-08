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
    
    @After
    public void tearDown() {
        try {
            table.closeReaders();
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }
}