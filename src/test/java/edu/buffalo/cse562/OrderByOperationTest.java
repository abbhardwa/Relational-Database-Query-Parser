package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import java.text.SimpleDateFormat;
import static org.junit.Assert.*;

public class OrderByOperationTest {
    
    private List<String[]> sampleData;
    private List<String[]> nullData;
    private List<String[]> largeData;
    private List<String[]> specialData;
    private SimpleDateFormat dateFormat;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        sampleData = TestUtils.createSampleData(10, false, false);
        nullData = TestUtils.createSampleData(10, true, false);
        largeData = TestUtils.createSampleData(1000, false, false);
        specialData = TestUtils.createSampleData(10, false, true);
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        
        // Shuffle all data to ensure it's not pre-sorted
        Collections.shuffle(sampleData);
        Collections.shuffle(nullData);
        Collections.shuffle(largeData);
        Collections.shuffle(specialData);
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
        // Order by Flag(3), Value(2), Name(1)
        int[] columns = new int[] { 3, 2, 1 };
        boolean[] ascending = new boolean[] { true, false, true };
        
        List<String[]> result = orderer.orderBy(sampleData, columns, ascending);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 1; i < result.size(); i++) {
            boolean prevFlag = Boolean.parseBoolean(result.get(i-1)[3]);
            boolean currFlag = Boolean.parseBoolean(result.get(i)[3]);
            
            if (prevFlag == currFlag) {
                double prevVal = Double.parseDouble(result.get(i-1)[2]);
                double currVal = Double.parseDouble(result.get(i)[2]);
                
                if (prevVal == currVal) {
                    String prevName = result.get(i-1)[1];
                    String currName = result.get(i)[1];
                    assertTrue(prevName.compareTo(currName) <= 0);
                } else {
                    assertTrue(prevVal >= currVal); // Descending order for Value
                }
            } else {
                assertTrue(!prevFlag || currFlag); // Ascending order for Flag
            }
        }
    }
    
    @Test
    public void testOrderByWithNulls() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 1, 2 }; // Name and Value columns
        boolean[] ascending = new boolean[] { true, true };
        
        List<String[]> result = orderer.orderBy(nullData, columns, ascending);
        
        assertEquals(nullData.size(), result.size());
        
        // Verify nulls are handled consistently (e.g., nulls first)
        int firstNonNullIndex = -1;
        for (int i = 0; i < result.size(); i++) {
            if (result.get(i)[1] != null) {
                firstNonNullIndex = i;
                break;
            }
        }
        
        if (firstNonNullIndex > -1) {
            // Check sorting of non-null values
            for (int i = firstNonNullIndex + 1; i < result.size(); i++) {
                if (result.get(i)[1] != null) {
                    assertTrue(result.get(i-1)[1].compareTo(result.get(i)[1]) <= 0);
                }
            }
        }
    }
    
    @Test
    public void testOrderByDate() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 4 }; // Date column
        boolean[] ascending = new boolean[] { true };
        
        List<String[]> result = orderer.orderBy(sampleData, columns, ascending);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 1; i < result.size(); i++) {
            try {
                Date prevDate = dateFormat.parse(result.get(i-1)[4]);
                Date currDate = dateFormat.parse(result.get(i)[4]);
                assertTrue(prevDate.compareTo(currDate) <= 0);
            } catch (Exception e) {
                fail("Invalid date format");
            }
        }
    }
    
    @Test
    public void testOrderByLargeDataset() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 2 }; // Value column
        boolean[] ascending = new boolean[] { true };
        
        long startTime = System.currentTimeMillis();
        List<String[]> result = orderer.orderBy(largeData, columns, ascending);
        long endTime = System.currentTimeMillis();
        
        // Verify correctness
        assertEquals(largeData.size(), result.size());
        for (int i = 1; i < result.size(); i++) {
            double prev = Double.parseDouble(result.get(i-1)[2]);
            double curr = Double.parseDouble(result.get(i)[2]);
            assertTrue(prev <= curr);
        }
        
        // Verify performance
        long duration = endTime - startTime;
        assertTrue("Sort should complete in reasonable time", duration < 1000); // 1 second
        
        // Verify memory usage
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        assertTrue("Memory usage should be reasonable",
            usedMemory < 100 * 1024 * 1024); // 100MB
    }
    
    @Test
    public void testOrderByMixedTypes() {
        OrderByOperation orderer = new OrderByOperation();
        // Create data with mixed types in same column
        List<String[]> mixedData = new ArrayList<>();
        mixedData.add(new String[] { "1", "abc", "123.45", "true" });
        mixedData.add(new String[] { "2", "123", "456.78", "false" });
        mixedData.add(new String[] { "3", "ABC", "789.01", "true" });
        mixedData.add(new String[] { "4", "123abc", "234.56", "false" });
        
        // Sort by second column (mixed strings/numbers)
        int[] columns = new int[] { 1 };
        boolean[] ascending = new boolean[] { true };
        
        List<String[]> result = orderer.orderBy(mixedData, columns, ascending);
        
        assertEquals(mixedData.size(), result.size());
        for (int i = 1; i < result.size(); i++) {
            String prev = result.get(i-1)[1];
            String curr = result.get(i)[1];
            assertTrue(prev.compareTo(curr) <= 0);
        }
    }
    
    @Test
    public void testOrderByStability() {
        OrderByOperation orderer = new OrderByOperation();
        
        // Create data with duplicate values in sort column
        List<String[]> duplicateData = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            duplicateData.add(new String[] { 
                String.valueOf(i),           // ID
                "Name" + (i % 3),           // Name (3 distinct values)
                String.valueOf(i % 2 * 100), // Value (2 distinct values)
                String.valueOf(i % 2 == 0)   // Flag
            });
        }
        
        // Sort by multiple columns with duplicates
        int[] columns = new int[] { 2, 1 };  // Value then Name
        boolean[] ascending = new boolean[] { true, true };
        
        List<String[]> result = orderer.orderBy(duplicateData, columns, ascending);
        
        // Verify stability by checking IDs maintain relative order within groups
        Map<String, List<Integer>> groups = new HashMap<>();
        for (String[] row : result) {
            String groupKey = row[2] + "|" + row[1]; // Value + Name
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>())
                  .add(Integer.parseInt(row[0]));
        }
        
        // Check that IDs within each group are in ascending order
        for (List<Integer> ids : groups.values()) {
            for (int i = 1; i < ids.size(); i++) {
                assertTrue(ids.get(i-1) < ids.get(i));
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
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidColumnIndices() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 5 }; // Invalid column index
        boolean[] ascending = new boolean[] { true };
        
        orderer.orderBy(sampleData, columns, ascending);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testMismatchedColumnArrays() {
        OrderByOperation orderer = new OrderByOperation();
        int[] columns = new int[] { 1, 2 };
        boolean[] ascending = new boolean[] { true }; // Missing second direction
        
        orderer.orderBy(sampleData, columns, ascending);
    }
}
