package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;
import static org.junit.Assert.*;

public class HashJoinTest {
    
    private List<String[]> leftTable;
    private List<String[]> rightTable;
    
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
    }
    
    @Test(expected = IndexOutOfBoundsException.class)
    public void testInvalidJoinColumn() {
        HashJoin joiner = new HashJoin();
        joiner.join(leftTable, rightTable, 10, 0); // Invalid left join column
    }
    
    private boolean containsId(List<String[]> table, String id) {
        for (String[] row : table) {
            if (row[0].equals(id)) {
                return true;
            }
        }
        return false;
    }
}