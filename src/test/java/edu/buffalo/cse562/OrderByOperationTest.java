package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import static org.junit.Assert.*;

public class OrderByOperationTest {
    
    private List<String[]> sampleData;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        sampleData = TestUtils.createSampleData(10);
        // Shuffle the data to ensure it's not pre-sorted
        java.util.Collections.shuffle(sampleData);
    }
    
    @After
    public void tearDown() {
        TestUtils.restoreStreams();
        TestUtils.clearOutput();
    }
    
    @Test
    public void testOrderByNumericAscending() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 2 }; // Order by Value column
        boolean[] ascending = new boolean[] { true };
        
        List<String[]> result = orderer.orderBy(sampleData, columns, ascending);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 1; i < result.size(); i++) {
            double prev = Double.parseDouble(result.get(i-1)[2]);
            double curr = Double.parseDouble(result.get(i)[2]);
            assertTrue(prev <= curr);
        }
    }
    
    @Test
    public void testOrderByNumericDescending() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 2 }; // Order by Value column
        boolean[] ascending = new boolean[] { false };
        
        List<String[]> result = orderer.orderBy(sampleData, columns, ascending);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 1; i < result.size(); i++) {
            double prev = Double.parseDouble(result.get(i-1)[2]);
            double curr = Double.parseDouble(result.get(i)[2]);
            assertTrue(prev >= curr);
        }
    }
    
    @Test
    public void testOrderByString() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 1 }; // Order by Name column
        boolean[] ascending = new boolean[] { true };
        
        List<String[]> result = orderer.orderBy(sampleData, columns, ascending);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 1; i < result.size(); i++) {
            String prev = result.get(i-1)[1];
            String curr = result.get(i)[1];
            assertTrue(prev.compareTo(curr) <= 0);
        }
    }
    
    @Test
    public void testOrderByMultipleColumns() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 3, 2 }; // Order by Flag then Value
        boolean[] ascending = new boolean[] { true, false };
        
        List<String[]> result = orderer.orderBy(sampleData, columns, ascending);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 1; i < result.size(); i++) {
            boolean prevFlag = Boolean.parseBoolean(result.get(i-1)[3]);
            boolean currFlag = Boolean.parseBoolean(result.get(i)[3]);
            
            if (prevFlag == currFlag) {
                double prevVal = Double.parseDouble(result.get(i-1)[2]);
                double currVal = Double.parseDouble(result.get(i)[2]);
                assertTrue(prevVal >= currVal); // Descending order for Value
            } else {
                assertTrue(!prevFlag || currFlag); // Ascending order for Flag
            }
        }
    }
    
    @Test
    public void testOrderByEmptyInput() {
        OrderByOperation orderer = new OrderByOperation();
        List<String[]> emptyData = TestUtils.createSampleData(0);
        int[] columns = new int[] { 0 };
        boolean[] ascending = new boolean[] { true };
        
        List<String[]> result = orderer.orderBy(emptyData, columns, ascending);
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testStableSort() {
        // Create data with duplicate values in sort column
        List<String[]> duplicateData = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            duplicateData.add(new String[] { String.valueOf(i), "Name" + i, "100", "true" });
        }
        
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 2 }; // Order by Value column (all same value)
        boolean[] ascending = new boolean[] { true };
        
        List<String[]> result = orderer.orderBy(duplicateData, columns, ascending);
        
        // Check that relative order of equal elements is preserved
        for (int i = 0; i < result.size(); i++) {
            assertEquals(String.valueOf(i), result.get(i)[0]);
        }
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidColumnIndices() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 5 }; // Invalid column index
        boolean[] ascending = new boolean[] { true };
        
        orderer.orderBy(sampleData, columns, ascending);
    }
}