package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class AggregateOperationsTest {
    
    private List<String[]> sampleData;
    private List<String[]> nullData;
    private List<String[]> boundaryData;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        // Basic data without nulls
        sampleData = TestUtils.createSampleData(10, false, false);
        // Data with nulls
        nullData = TestUtils.createSampleData(10, true, false);
        // Data with boundary values
        boundaryData = TestUtils.createSampleData(10, false, true);
    }
    
    @After
    public void tearDown() {
        TestUtils.restoreStreams();
        TestUtils.clearOutput();
    }
    
    @Test
    public void testSum() {
        // Test sum aggregation on the Value column
        AggregateOperations aggregator = new AggregateOperations();
        double sum = 0;
        for (String[] row : sampleData) {
            sum += Double.parseDouble(row[2]); // Value column
        }
        assertEquals(sum, aggregator.calculateSum(sampleData, 2), 0.001);
    }
    
    @Test
    public void testSumWithNulls() {
        AggregateOperations aggregator = new AggregateOperations();
        double sum = 0;
        int nullCount = 0;
        for (String[] row : nullData) {
            if (row[2] != null) {
                sum += Double.parseDouble(row[2]);
            } else {
                nullCount++;
            }
        }
        assertEquals(sum, aggregator.calculateSum(nullData, 2), 0.001);
        assertTrue("Data should contain some null values", nullCount > 0);
    }
    
    @Test
    public void testSumWithBoundaryValues() {
        AggregateOperations aggregator = new AggregateOperations();
        double sum = 0;
        for (String[] row : boundaryData) {
            double val = Double.parseDouble(row[2]);
            if (Double.isFinite(val)) { // Skip infinity/NaN
                sum += val;
            }
        }
        assertEquals(sum, aggregator.calculateSum(boundaryData, 2), 0.001);
    }
    
    @Test
    public void testAverage() {
        AggregateOperations aggregator = new AggregateOperations();
        double sum = 0;
        for (String[] row : sampleData) {
            sum += Double.parseDouble(row[2]); // Value column
        }
        double expected = sum / sampleData.size();
        assertEquals(expected, aggregator.calculateAverage(sampleData, 2), 0.001);
    }
    
    @Test
    public void testAverageWithNulls() {
        AggregateOperations aggregator = new AggregateOperations();
        double sum = 0;
        int count = 0;
        for (String[] row : nullData) {
            if (row[2] != null) {
                sum += Double.parseDouble(row[2]);
                count++;
            }
        }
        double expected = sum / count;
        assertEquals(expected, aggregator.calculateAverage(nullData, 2), 0.001);
    }
    
    @Test
    public void testCount() {
        AggregateOperations aggregator = new AggregateOperations();
        assertEquals(sampleData.size(), aggregator.calculateCount(sampleData));
    }
    
    @Test
    public void testCountDistinct() {
        AggregateOperations aggregator = new AggregateOperations();
        Set<String> distinct = new HashSet<>();
        for (String[] row : sampleData) {
            distinct.add(row[1]); // Name column
        }
        assertEquals(distinct.size(), aggregator.calculateCountDistinct(sampleData, 1));
    }
    
    @Test
    public void testCountDistinctWithNulls() {
        AggregateOperations aggregator = new AggregateOperations();
        Set<String> distinct = new HashSet<>();
        for (String[] row : nullData) {
            if (row[1] != null) {
                distinct.add(row[1]);
            }
        }
        assertEquals(distinct.size(), aggregator.calculateCountDistinct(nullData, 1));
    }
    
    @Test
    public void testMin() {
        AggregateOperations aggregator = new AggregateOperations();
        double min = Double.MAX_VALUE;
        for (String[] row : sampleData) {
            min = Math.min(min, Double.parseDouble(row[2]));
        }
        assertEquals(min, aggregator.calculateMin(sampleData, 2), 0.001);
    }
    
    @Test
    public void testMinWithNulls() {
        AggregateOperations aggregator = new AggregateOperations();
        double min = Double.MAX_VALUE;
        for (String[] row : nullData) {
            if (row[2] != null) {
                min = Math.min(min, Double.parseDouble(row[2]));
            }
        }
        assertEquals(min, aggregator.calculateMin(nullData, 2), 0.001);
    }
    
    @Test
    public void testMax() {
        AggregateOperations aggregator = new AggregateOperations();
        double max = Double.MIN_VALUE;
        for (String[] row : sampleData) {
            max = Math.max(max, Double.parseDouble(row[2]));
        }
        assertEquals(max, aggregator.calculateMax(sampleData, 2), 0.001);
    }
    
    @Test
    public void testMaxWithNulls() {
        AggregateOperations aggregator = new AggregateOperations();
        double max = Double.MIN_VALUE;
        for (String[] row : nullData) {
            if (row[2] != null) {
                max = Math.max(max, Double.parseDouble(row[2]));
            }
        }
        assertEquals(max, aggregator.calculateMax(nullData, 2), 0.001);
    }
    
    @Test
    public void testGroupBySum() {
        AggregateOperations aggregator = new AggregateOperations();
        // Group by boolean flag (column 3), sum values (column 2)
        Map<String, Double> expectedGroups = new HashMap<>();
        for (String[] row : sampleData) {
            String groupKey = row[3];
            double value = Double.parseDouble(row[2]);
            expectedGroups.merge(groupKey, value, Double::sum);
        }
        
        Map<String, Double> result = aggregator.groupBySum(sampleData, 3, 2);
        assertEquals(expectedGroups, result);
    }
    
    @Test
    public void testGroupBySumWithNulls() {
        AggregateOperations aggregator = new AggregateOperations();
        // Group by boolean flag (column 3), sum values (column 2)
        Map<String, Double> expectedGroups = new HashMap<>();
        for (String[] row : nullData) {
            String groupKey = row[3] != null ? row[3] : "NULL";
            if (row[2] != null) {
                double value = Double.parseDouble(row[2]);
                expectedGroups.merge(groupKey, value, Double::sum);
            }
        }
        
        Map<String, Double> result = aggregator.groupBySum(nullData, 3, 2);
        assertEquals(expectedGroups, result);
    }
    
    @Test
    public void testEmptyInput() {
        AggregateOperations aggregator = new AggregateOperations();
        List<String[]> emptyData = TestUtils.createSampleData(0);
        
        assertEquals(0, aggregator.calculateCount(emptyData));
        assertEquals(0, aggregator.calculateSum(emptyData, 2), 0.001);
        assertEquals(0, aggregator.calculateAverage(emptyData, 2), 0.001);
        assertEquals(0, aggregator.calculateMin(emptyData, 2), 0.001);
        assertEquals(0, aggregator.calculateMax(emptyData, 2), 0.001);
        assertTrue(aggregator.groupBySum(emptyData, 3, 2).isEmpty());
    }
    
    @Test
    public void testMultiColumnGroupBy() {
        AggregateOperations aggregator = new AggregateOperations();
        // Group by name and flag (columns 1,3), sum values (column 2)
        Map<String, Double> expectedGroups = new HashMap<>();
        for (String[] row : sampleData) {
            String groupKey = row[1] + "|" + row[3]; // Composite key
            double value = Double.parseDouble(row[2]);
            expectedGroups.merge(groupKey, value, Double::sum);
        }
        
        Map<String, Double> result = aggregator.groupByMultiColumnSum(sampleData, new int[]{1, 3}, 2);
        assertEquals(expectedGroups, result);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidColumnAccess() {
        AggregateOperations aggregator = new AggregateOperations();
        aggregator.calculateSum(sampleData, 99); // Invalid column index
    }
    
    @Test(expected = NumberFormatException.class)
    public void testInvalidNumericValue() {
        AggregateOperations aggregator = new AggregateOperations();
        List<String[]> invalidData = new ArrayList<>(sampleData);
        invalidData.get(0)[2] = "not a number";
        aggregator.calculateSum(invalidData, 2);
    }
}