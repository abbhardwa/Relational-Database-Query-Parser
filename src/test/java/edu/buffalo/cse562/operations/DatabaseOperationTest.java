package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.operations.DatabaseOperation;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class DatabaseOperationTest {
    private File testDataDir;
    private Table testTable;

    @Before
    public void setUp() throws IOException {
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        
        // Create test data file
        File dataFile = new File(testDataDir, "operation_test.tbl");
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|Alice|25|Engineering\n");
            writer.write("2|Bob|30|Marketing\n");
            writer.write("3|Charlie|35|Sales\n");
        }

        // Set up table
        testTable = new Table("operation_test", 4, dataFile, testDataDir);
        
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
        col4.setColumnName("department");
        columns.add(col4);
        
        testTable.setColumnDefinitions(columns);
        testTable.populateColumnIndexMap();
        testTable.populateTable();
    }

    @After
    public void tearDown() {
        // Clean up test files
        File[] files = testDataDir.listFiles((dir, name) -> name.startsWith("operation_test"));
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
    }

    // Test implementation of DatabaseOperation interface
    private static class TestDatabaseOperation implements DatabaseOperation {
        @Override
        public Table execute(Table... input) throws IOException {
            if (input == null || input.length == 0) {
                throw new IOException("No input tables provided");
            }
            
            Table inputTable = input[0];
            // Simple operation: return a copy of the first table
            Table result = new Table(inputTable.getTableName() + "_copy", 
                                   inputTable.getNoOfColumns(),
                                   null,
                                   inputTable.getTableDataDirectoryPath());
            result.setColumnDefinitions(new ArrayList<>(inputTable.getColumnDefinitions()));
            result.populateColumnIndexMap();
            result.setTuples(new ArrayList<>(inputTable.getTuples()));
            
            return result;
        }
    }

    // Test implementation that filters data
    private static class FilterOperation implements DatabaseOperation {
        private final String filterColumn;
        private final String filterValue;
        
        public FilterOperation(String filterColumn, String filterValue) {
            this.filterColumn = filterColumn;
            this.filterValue = filterValue;
        }
        
        @Override
        public Table execute(Table... input) throws IOException {
            if (input == null || input.length == 0) {
                throw new IOException("No input tables provided");
            }
            
            Table inputTable = input[0];
            Integer columnIndex = inputTable.getColumnIndexMap().get(filterColumn);
            if (columnIndex == null) {
                throw new IOException("Column not found: " + filterColumn);
            }
            
            Table result = new Table(inputTable.getTableName() + "_filtered", 
                                   inputTable.getNoOfColumns(),
                                   null,
                                   inputTable.getTableDataDirectoryPath());
            result.setColumnDefinitions(new ArrayList<>(inputTable.getColumnDefinitions()));
            result.populateColumnIndexMap();
            
            ArrayList<String> filteredTuples = new ArrayList<>();
            for (String tuple : inputTable.getTuples()) {
                String[] parts = tuple.split("\\|");
                if (parts.length > columnIndex && parts[columnIndex].equals(filterValue)) {
                    filteredTuples.add(tuple);
                }
            }
            result.setTuples(filteredTuples);
            
            return result;
        }
    }

    @Test
    public void testDatabaseOperationInterface() throws IOException {
        DatabaseOperation operation = new TestDatabaseOperation();
        Table result = operation.execute(testTable);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Result should have same number of tuples", 
                    testTable.getTuples().size(), result.getTuples().size());
        assertEquals("Result should have copy suffix", 
                    testTable.getTableName() + "_copy", result.getTableName());
    }

    @Test
    public void testFilterOperation() throws IOException {
        DatabaseOperation filterOp = new FilterOperation("department", "Engineering");
        Table result = filterOp.execute(testTable);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should have 1 engineering employee", 1, result.getTuples().size());
        assertTrue("Result should contain Alice", result.getTuples().get(0).contains("Alice"));
        assertTrue("Result should contain Engineering", result.getTuples().get(0).contains("Engineering"));
    }

    @Test
    public void testFilterOperationNoMatches() throws IOException {
        DatabaseOperation filterOp = new FilterOperation("department", "NonExistent");
        Table result = filterOp.execute(testTable);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should have no matches", 0, result.getTuples().size());
    }

    @Test(expected = IOException.class)
    public void testDatabaseOperationWithNullInput() throws IOException {
        DatabaseOperation operation = new TestDatabaseOperation();
        operation.execute((Table[]) null);
    }

    @Test(expected = IOException.class)
    public void testDatabaseOperationWithEmptyInput() throws IOException {
        DatabaseOperation operation = new TestDatabaseOperation();
        operation.execute();
    }

    @Test(expected = IOException.class)
    public void testFilterOperationWithInvalidColumn() throws IOException {
        DatabaseOperation filterOp = new FilterOperation("nonexistent_column", "value");
        filterOp.execute(testTable);
    }

    @Test
    public void testOperationChaining() throws IOException {
        // Test chaining operations
        DatabaseOperation filterOp = new FilterOperation("age", "25");
        Table filtered = filterOp.execute(testTable);
        
        DatabaseOperation copyOp = new TestDatabaseOperation();
        Table result = copyOp.execute(filtered);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should have 1 tuple (Alice, age 25)", 1, result.getTuples().size());
        assertTrue("Result should contain Alice", result.getTuples().get(0).contains("Alice"));
    }

    @Test
    public void testOperationWithEmptyTable() throws IOException {
        // Create empty table
        File emptyFile = new File(testDataDir, "empty_operation_test.tbl");
        try (FileWriter writer = new FileWriter(emptyFile)) {
            // Empty file
        }

        Table emptyTable = new Table("empty", 4, emptyFile, testDataDir);
        emptyTable.setColumnDefinitions(testTable.getColumnDefinitions());
        emptyTable.populateColumnIndexMap();
        emptyTable.populateTable();
        
        DatabaseOperation operation = new TestDatabaseOperation();
        Table result = operation.execute(emptyTable);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Result should be empty", 0, result.getTuples().size());
        
        // Clean up
        emptyFile.delete();
    }

    @Test
    public void testOperationPreservesTableStructure() throws IOException {
        DatabaseOperation operation = new TestDatabaseOperation();
        Table result = operation.execute(testTable);
        
        assertNotNull("Result should not be null", result);
        assertEquals("Should preserve number of columns", 
                    testTable.getNoOfColumns(), result.getNoOfColumns());
        assertEquals("Should preserve column definitions count", 
                    testTable.getColumnDefinitions().size(), 
                    result.getColumnDefinitions().size());
        
        // Verify column names are preserved
        for (int i = 0; i < testTable.getColumnDefinitions().size(); i++) {
            assertEquals("Column name should be preserved",
                        testTable.getColumnDefinitions().get(i).getColumnName(),
                        result.getColumnDefinitions().get(i).getColumnName());
        }
    }
}