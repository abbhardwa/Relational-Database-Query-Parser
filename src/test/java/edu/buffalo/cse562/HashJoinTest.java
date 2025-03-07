package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.*;
import static org.junit.Assert.*;

public class HashJoinTest {
    
    private List<String[]> leftTable;
    private List<String[]> rightTable;
    private List<String[]> duplicateKeyTable;
    private List<String[]> nullKeyTable;
    private List<String[]> largeTable;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        
        // Create sample data for left table (Orders)
        leftTable = new ArrayList<>();
        leftTable.add(new String[] { "1", "100.0", "2023-01-01" });
        leftTable.add(new String[] { "2", "200.0", "2023-01-02" });
        leftTable.add(new String[] { "3", "300.0", "2023-01-03" });
        leftTable.add(new String[] { "4", "400.0", "2023-01-04" });
        
        // Create sample data for right table (Customers)
        rightTable = new ArrayList<>();
        rightTable.add(new String[] { "1", "John", "NY" });
        rightTable.add(new String[] { "2", "Jane", "CA" });
        rightTable.add(new String[] { "3", "Bob", "TX" });
        rightTable.add(new String[] { "5", "Alice", "FL" }); // No matching order
        
        // Create table with duplicate keys
        duplicateKeyTable = new ArrayList<>();
        duplicateKeyTable.add(new String[] { "1", "John", "NY" });
        duplicateKeyTable.add(new String[] { "1", "Johnny", "NJ" }); // Duplicate key
        duplicateKeyTable.add(new String[] { "2", "Jane", "CA" });
        duplicateKeyTable.add(new String[] { "2", "Janet", "CT" }); // Duplicate key
        
        // Create table with null keys
        nullKeyTable = new ArrayList<>();
        nullKeyTable.add(new String[] { null, "Unknown", "Unknown" });
        nullKeyTable.add(new String[] { "1", "John", "NY" });
        nullKeyTable.add(new String[] { null, "Anon", "Hidden" });
        nullKeyTable.add(new String[] { "2", "Jane", "CA" });
        
        // Create large table for performance testing
        largeTable = TestUtils.createSampleData(1000, false, false);
    }
    
    @After
    public void tearDown() {
        TestUtils.restoreStreams();
        TestUtils.clearOutput();
    }
    
    @Test
    public void testInnerJoin() {
        HashJoin joiner = new HashJoin();
        int leftJoinCol = 0;  // Order ID
        int rightJoinCol = 0; // Customer ID
        
        List<String[]> result = joiner.join(leftTable, rightTable, leftJoinCol, rightJoinCol);
        
        assertEquals(3, result.size()); // Only matching rows should be included
        
        // Verify join results
        for (String[] row : result) {
            // Result should have columns from both tables
            assertEquals(6, row.length);
            // Check that IDs match
            assertEquals(row[0], row[3]);
            // Verify data is from correct rows
            assertTrue(containsId(leftTable, row[0]));
            assertTrue(containsId(rightTable, row[3]));
        }
    }
    
    @Test
    public void testLeftJoin() {
        HashJoin joiner = new HashJoin();
        int leftJoinCol = 0;  // Order ID
        int rightJoinCol = 0; // Customer ID
        
        List<String[]> result = joiner.leftJoin(leftTable, rightTable, leftJoinCol, rightJoinCol);
        
        assertEquals(4, result.size()); // All left rows should be included
        
        // Verify join results
        for (String[] row : result) {
            assertEquals(6, row.length);
            assertTrue(containsId(leftTable, row[0]));
            
            if (containsId(rightTable, row[0])) {
                // Matching rows should have data from both tables
                assertNotNull(row[4]); // Customer name
                assertNotNull(row[5]); // Customer location
            } else {
                // Non-matching rows should have nulls for right table columns
                assertNull(row[4]);
                assertNull(row[5]);
            }
        }
    }
    
    @Test
    public void testRightJoin() {
        HashJoin joiner = new HashJoin();
        int leftJoinCol = 0;  // Order ID
        int rightJoinCol = 0; // Customer ID
        
        List<String[]> result = joiner.rightJoin(leftTable, rightTable, leftJoinCol, rightJoinCol);
        
        assertEquals(4, result.size()); // All right rows should be included
        
        // Verify join results
        for (String[] row : result) {
            assertEquals(6, row.length);
            assertTrue(containsId(rightTable, row[3]));
            
            if (containsId(leftTable, row[3])) {
                // Matching rows should have data from both tables
                assertNotNull(row[1]); // Order amount
                assertNotNull(row[2]); // Order date
            } else {
                // Non-matching rows should have nulls for left table columns
                assertNull(row[1]);
                assertNull(row[2]);
            }
        }
    }
    
    @Test
    public void testEmptyTables() {
        HashJoin joiner = new HashJoin();
        List<String[]> emptyTable = new ArrayList<>();
        
        // Test joining empty left table
        List<String[]> result1 = joiner.join(emptyTable, rightTable, 0, 0);
        assertTrue(result1.isEmpty());
        
        // Test joining empty right table
        List<String[]> result2 = joiner.join(leftTable, emptyTable, 0, 0);
        assertTrue(result2.isEmpty());
        
        // Test joining two empty tables
        List<String[]> result3 = joiner.join(emptyTable, emptyTable, 0, 0);
        assertTrue(result3.isEmpty());
        
        // Test left join with empty right table
        List<String[]> result4 = joiner.leftJoin(leftTable, emptyTable, 0, 0);
        assertEquals(leftTable.size(), result4.size());
        for (String[] row : result4) {
            // Right side columns should be null
            assertNull(row[3]);
            assertNull(row[4]);
            assertNull(row[5]);
        }
        
        // Test right join with empty left table
        List<String[]> result5 = joiner.rightJoin(emptyTable, rightTable, 0, 0);
        assertEquals(rightTable.size(), result5.size());
        for (String[] row : result5) {
            // Left side columns should be null
            assertNull(row[0]);
            assertNull(row[1]);
            assertNull(row[2]);
        }
    }
    
    @Test
    public void testJoinWithDuplicateKeys() {
        HashJoin joiner = new HashJoin();
        
        // Join orders with customers having duplicate entries
        List<String[]> result = joiner.join(leftTable, duplicateKeyTable, 0, 0);
        
        // Each matching key should produce multiple rows
        Set<String> uniqueKeys = new HashSet<>();
        int totalRows = 0;
        for (String[] row : result) {
            uniqueKeys.add(row[0]);
            totalRows++;
        }
        
        assertTrue("Should have multiple rows per key", totalRows > uniqueKeys.size());
        // Verify cartesian product for matching keys
        for (String key : uniqueKeys) {
            int keyCount = 0;
            int duplicateCount = countId(duplicateKeyTable, key);
            for (String[] row : result) {
                if (row[0].equals(key)) {
                    keyCount++;
                }
            }
            assertEquals("Each row should join with all matching keys", 
                duplicateCount, keyCount);
        }
    }
    
    @Test
    public void testJoinWithNullKeys() {
        HashJoin joiner = new HashJoin();
        
        // Test inner join with null keys
        List<String[]> result1 = joiner.join(leftTable, nullKeyTable, 0, 0);
        // Null keys should be excluded from inner join
        for (String[] row : result1) {
            assertNotNull("Join key should not be null", row[0]);
            assertNotNull("Join key should not be null", row[3]);
        }
        
        // Test left join with null keys
        List<String[]> result2 = joiner.leftJoin(leftTable, nullKeyTable, 0, 0);
        assertEquals("All left rows should be included", leftTable.size(), result2.size());
        
        // Test right join with null keys
        List<String[]> result3 = joiner.rightJoin(leftTable, nullKeyTable, 0, 0);
        assertEquals("All right rows should be included", nullKeyTable.size(), result3.size());
        
        int nullCount = 0;
        for (String[] row : result3) {
            if (row[3] == null) {
                nullCount++;
                // Left side should be null for right table's null keys
                assertNull(row[0]);
                assertNull(row[1]);
                assertNull(row[2]);
            }
        }
        assertEquals("Should have same number of nulls as input", 
            countNullIds(nullKeyTable), nullCount);
    }
    
    @Test
    public void testLargeDatasetJoin() {
        HashJoin joiner = new HashJoin();
        
        // Create two large tables with some overlapping keys
        List<String[]> largeLeft = new ArrayList<>(largeTable);
        List<String[]> largeRight = new ArrayList<>();
        for (int i = 500; i < 1500; i++) { // Overlap from 500-999
            largeRight.add(new String[] { 
                String.valueOf(i), 
                "Name" + i,
                String.valueOf(i * 100)
            });
        }
        
        long startTime = System.currentTimeMillis();
        List<String[]> result = joiner.join(largeLeft, largeRight, 0, 0);
        long endTime = System.currentTimeMillis();
        
        // Verify join correctness
        assertEquals(500, result.size()); // Should have 500 matching rows
        
        // Verify performance is reasonable (less than 1 second for this size)
        assertTrue("Join should complete in reasonable time",
            endTime - startTime < 1000);
        
        // Verify memory usage stays reasonable
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        assertTrue("Memory usage should be reasonable",
            usedMemory < 100 * 1024 * 1024); // Less than 100MB
    }
    
    @Test(expected = IndexOutOfBoundsException.class)
    public void testInvalidJoinColumn() {
        HashJoin joiner = new HashJoin();
        joiner.join(leftTable, rightTable, 10, 0); // Invalid left join column
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNullTables() {
        HashJoin joiner = new HashJoin();
        joiner.join(null, rightTable, 0, 0);
    }
    
    private boolean containsId(List<String[]> table, String id) {
        if (id == null) return false;
        for (String[] row : table) {
            if (id.equals(row[0])) {
                return true;
            }
        }
        return false;
    }
    
    private int countId(List<String[]> table, String id) {
        int count = 0;
        for (String[] row : table) {
            if (id.equals(row[0])) {
                count++;
            }
        }
        return count;
    }
    
    private int countNullIds(List<String[]> table) {
        int count = 0;
        for (String[] row : table) {
            if (row[0] == null) {
                count++;
            }
        }
        return count;
    }
}