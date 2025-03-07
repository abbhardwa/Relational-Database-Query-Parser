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

public class AggregateOperationsTest {
    private File testDataDir;
    private File dataFile;
    private Table table;
    private AggregateOperations aggregateOp;

    @Before
    public void setUp() throws IOException {
        // Create test data directory and file
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        dataFile = new File(testDataDir, "sales.tbl");
        
        // Create test data
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|A|100\n");
            writer.write("2|A|200\n");
            writer.write("3|B|150\n");
            writer.write("4|B|250\n");
            writer.write("5|C|300\n");
        }

        // Set up table
        table = new Table("sales", 3, dataFile, testDataDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("id");
        columns.add(col1);
        
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("category");
        columns.add(col2);
        
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("amount");
        columns.add(col3);
        
        table.setColumnDefinitions(columns);
        table.populateColumnIndexMap();
        table.populateTable();
        
        aggregateOp = new AggregateOperations(table);
    }

    @Test
    public void testSum() {
        double sum = aggregateOp.sum(2); // sum amount column
        assertEquals(1000.0, sum, 0.001);
    }

    @Test
    public void testAverage() {
        double avg = aggregateOp.average(2); // average amount column
        assertEquals(200.0, avg, 0.001);
    }

    @Test
    public void testCount() {
        int count = aggregateOp.count(2); // count amount column
        assertEquals(5, count);
    }

    @Test
    public void testMin() {
        double min = aggregateOp.min(2); // min amount column
        assertEquals(100.0, min, 0.001);
    }

    @Test
    public void testMax() {
        double max = aggregateOp.max(2); // max amount column
        assertEquals(300.0, max, 0.001);
    }

    @Test
    public void testGroupBySum() {
        Table result = aggregateOp.groupBySum(1, 2); // group by category, sum amount
        assertNotNull("Result table should not be null", result);
        assertEquals("Should have three groups", 3, result.getTuples().size());
        
        ArrayList<String> tuples = result.getTuples();
        boolean foundGroupA = false;
        boolean foundGroupB = false;
        boolean foundGroupC = false;
        
        for (String tuple : tuples) {
            String[] parts = tuple.split("\\|");
            if (parts[0].equals("A")) {
                assertEquals("300.0", parts[1]);
                foundGroupA = true;
            } else if (parts[0].equals("B")) {
                assertEquals("400.0", parts[1]);
                foundGroupB = true;
            } else if (parts[0].equals("C")) {
                assertEquals("300.0", parts[1]);
                foundGroupC = true;
            }
        }
        
        assertTrue("Should find group A", foundGroupA);
        assertTrue("Should find group B", foundGroupB);
        assertTrue("Should find group C", foundGroupC);
    }

    @Test
    public void testEmptyTableAggregates() throws IOException {
        // Create empty table
        try (FileWriter writer = new FileWriter(dataFile)) {}
        table.populateTable();
        
        aggregateOp = new AggregateOperations(table);
        
        assertEquals("Sum should be 0 for empty table", 0.0, aggregateOp.sum(2), 0.001);
        assertEquals("Average should be 0 for empty table", 0.0, aggregateOp.average(2), 0.001);
        assertEquals("Count should be 0 for empty table", 0, aggregateOp.count(2));
        assertEquals("Min should be 0 for empty table", 0.0, aggregateOp.min(2), 0.001);
        assertEquals("Max should be 0 for empty table", 0.0, aggregateOp.max(2), 0.001);
        
        Table groupResult = aggregateOp.groupBySum(1, 2);
        assertTrue("Group by result should be empty", groupResult.getTuples().isEmpty());
    }

    @Test
    public void testSingleRecordAggregates() throws IOException {
        // Create table with single record
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|A|100\n");
        }
        table.populateTable();
        
        aggregateOp = new AggregateOperations(table);
        
        assertEquals("Sum should be 100", 100.0, aggregateOp.sum(2), 0.001);
        assertEquals("Average should be 100", 100.0, aggregateOp.average(2), 0.001);
        assertEquals("Count should be 1", 1, aggregateOp.count(2));
        assertEquals("Min should be 100", 100.0, aggregateOp.min(2), 0.001);
        assertEquals("Max should be 100", 100.0, aggregateOp.max(2), 0.001);
        
        Table groupResult = aggregateOp.groupBySum(1, 2);
        assertEquals("Should have one group", 1, groupResult.getTuples().size());
        assertEquals("Group should sum to 100", "A|100.0", groupResult.getTuples().get(0));
    }
}