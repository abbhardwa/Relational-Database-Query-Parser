package edu.buffalo.cse562.aggregation;

import edu.buffalo.cse562.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

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
        
        ColumnDefinition col1 = createColumn("id", "INTEGER");
        ColumnDefinition col2 = createColumn("category", "VARCHAR");
        ColumnDefinition col3 = createColumn("amount", "DECIMAL");
        
        columns.add(col1);
        columns.add(col2);
        columns.add(col3);
        
        table.setColumnDefinitions(columns);
        table.populateColumnIndexMap();
        table.populateTable();
        
        aggregateOp = new AggregateOperations(table);
    }
    
    private ColumnDefinition createColumn(String name, String type) {
        ColumnDefinition col = new ColumnDefinition();
        col.setColumnName(name);
        ColDataType dataType = new ColDataType();
        dataType.setDataType(type);
        col.setColDataType(dataType);
        return col;
    }

    @Test
    public void testSimpleAggregations() {
        // Test SUM
        AggregateResult sum = aggregateOp.aggregate("amount", AggregateFunction.SUM);
        assertEquals(new BigDecimal("1000.00"), sum.getNumericResult().get().setScale(2));
        
        // Test AVG
        AggregateResult avg = aggregateOp.aggregate("amount", AggregateFunction.AVG);
        assertEquals(new BigDecimal("200.00"), avg.getNumericResult().get().setScale(2));
        
        // Test COUNT
        AggregateResult count = aggregateOp.aggregate("amount", AggregateFunction.COUNT);
        assertEquals(5L, count.getCount().get().longValue());
        
        // Test MIN
        AggregateResult min = aggregateOp.aggregate("amount", AggregateFunction.MIN);
        assertEquals(new BigDecimal("100.00"), min.getNumericResult().get().setScale(2));
        
        // Test MAX
        AggregateResult max = aggregateOp.aggregate("amount", AggregateFunction.MAX);
        assertEquals(new BigDecimal("300.00"), max.getNumericResult().get().setScale(2));
    }
    
    @Test
    public void testCountDistinct() {
        // Test COUNT DISTINCT on category column
        AggregateResult distinctCount = aggregateOp.aggregate("category", AggregateFunction.COUNT_DISTINCT);
        assertEquals(3L, distinctCount.getCount().get().longValue());
    }

    @Test
    public void testGroupBy() {
        // Test grouping by category with SUM of amount
        Map<String, AggregateFunction> aggregations = new HashMap<>();
        aggregations.put("amount", AggregateFunction.SUM);
        
        GroupByResult result = aggregateOp.groupBy(Collections.singletonList("category"), aggregations);
        
        assertEquals(3, result.size()); // Should have 3 groups: A, B, C
        
        // Check group A
        GroupByKey keyA = GroupByKey.of("A");
        List<AggregateResult> groupAResults = result.getResultsForGroup(keyA);
        assertEquals(new BigDecimal("300.00"), groupAResults.get(0).getNumericResult().get().setScale(2));
        
        // Check group B
        GroupByKey keyB = GroupByKey.of("B");
        List<AggregateResult> groupBResults = result.getResultsForGroup(keyB);
        assertEquals(new BigDecimal("400.00"), groupBResults.get(0).getNumericResult().get().setScale(2));
        
        // Check group C
        GroupByKey keyC = GroupByKey.of("C");
        List<AggregateResult> groupCResults = result.getResultsForGroup(keyC);
        assertEquals(new BigDecimal("300.00"), groupCResults.get(0).getNumericResult().get().setScale(2));
    }

    @Test
    public void testMultipleGroupBy() {
        // Create test data with multiple columns to group by
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|A|East|100\n");
            writer.write("2|A|West|200\n");
            writer.write("3|B|East|150\n");
            writer.write("4|B|West|250\n");
            writer.write("5|C|East|300\n");
            writer.write("6|C|West|350\n");
        } catch (IOException e) {
            fail("Failed to create test data: " + e.getMessage());
        }
        
        // Update table with new column
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        columns.add(createColumn("id", "INTEGER"));
        columns.add(createColumn("category", "VARCHAR"));
        columns.add(createColumn("region", "VARCHAR"));
        columns.add(createColumn("amount", "DECIMAL"));
        
        table = new Table("sales", 4, dataFile, testDataDir);
        table.setColumnDefinitions(columns);
        table.populateColumnIndexMap();
        table.populateTable();
        
        aggregateOp = new AggregateOperations(table);
        
        // Test grouping by category and region with SUM of amount
        Map<String, AggregateFunction> aggregations = new HashMap<>();
        aggregations.put("amount", AggregateFunction.SUM);
        
        List<String> groupByColumns = Arrays.asList("category", "region");
        GroupByResult result = aggregateOp.groupBy(groupByColumns, aggregations);
        
        assertEquals(6, result.size()); // Should have 6 groups (A/East, A/West, B/East, etc.)
        
        // Check A/East group
        GroupByKey keyAEast = GroupByKey.of("A", "East");
        List<AggregateResult> groupAEastResults = result.getResultsForGroup(keyAEast);
        assertEquals(new BigDecimal("100.00"), groupAEastResults.get(0).getNumericResult().get().setScale(2));
    }
    
    @Test
    public void testEmptyTable() throws IOException {
        // Create empty table
        try (FileWriter writer = new FileWriter(dataFile)) {
            // Empty file
        }
        
        table.populateTable();
        aggregateOp = new AggregateOperations(table);
        
        // Test SUM on empty table
        AggregateResult sum = aggregateOp.aggregate("amount", AggregateFunction.SUM);
        assertEquals(BigDecimal.ZERO, sum.getNumericResult().get());
        
        // Test AVG on empty table
        AggregateResult avg = aggregateOp.aggregate("amount", AggregateFunction.AVG);
        assertEquals(BigDecimal.ZERO, avg.getNumericResult().get());
        
        // Test COUNT on empty table
        AggregateResult count = aggregateOp.aggregate("amount", AggregateFunction.COUNT);
        assertEquals(0L, count.getCount().get().longValue());
    }
    
    @Test
    public void testMultipleAggregationsInGroupBy() {
        Map<String, AggregateFunction> aggregations = new HashMap<>();
        aggregations.put("amount", AggregateFunction.SUM);
        aggregations.put("amount", AggregateFunction.AVG);
        aggregations.put("id", AggregateFunction.COUNT);
        
        GroupByResult result = aggregateOp.groupBy(Collections.singletonList("category"), aggregations);
        
        // Check that we got all three aggregations for group A
        GroupByKey keyA = GroupByKey.of("A");
        List<AggregateResult> groupAResults = result.getResultsForGroup(keyA);
        assertEquals(3, groupAResults.size());
        
        // Verify we have all aggregation types
        Set<AggregateFunction> functions = new HashSet<>();
        for (AggregateResult aggResult : groupAResults) {
            functions.add(aggResult.getFunction());
        }
        
        assertTrue(functions.contains(AggregateFunction.SUM));
        assertTrue(functions.contains(AggregateFunction.AVG));
        assertTrue(functions.contains(AggregateFunction.COUNT));
    }
    
    @Test(expected = IllegalArgumentException.class) 
    public void testInvalidColumnName() {
        aggregateOp.aggregate("invalid_column", AggregateFunction.SUM);
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullTable() {
        new AggregateOperations(null);
    }
}