package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class AggregateOperationsTest {
    
    private List<String[]> sampleData;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        sampleData = TestUtils.createSampleData(10);
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
    public void testCount() {
        AggregateOperations aggregator = new AggregateOperations();
        assertEquals(sampleData.size(), aggregator.calculateCount(sampleData));
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
    public void testMax() {
        AggregateOperations aggregator = new AggregateOperations();
        double max = Double.MIN_VALUE;
        for (String[] row : sampleData) {
            max = Math.max(max, Double.parseDouble(row[2]));
        }
        assertEquals(max, aggregator.calculateMax(sampleData, 2), 0.001);
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
    }
}