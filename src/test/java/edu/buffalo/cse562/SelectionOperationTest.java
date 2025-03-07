package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.function.Predicate;
import static org.junit.Assert.*;

public class SelectionOperationTest {
    
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
    public void testSelectByNumericCondition() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where Value > 300
        List<String[]> result = selector.select(sampleData, row -> 
            Double.parseDouble(row[2]) > 300);
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertTrue(Double.parseDouble(row[2]) > 300);
        }
    }
    
    @Test
    public void testSelectByStringCondition() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where Name contains "5"
        List<String[]> result = selector.select(sampleData, row -> 
            row[1].contains("5"));
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertTrue(row[1].contains("5"));
        }
    }
    
    @Test
    public void testSelectByBooleanCondition() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where Flag is true
        List<String[]> result = selector.select(sampleData, row -> 
            Boolean.parseBoolean(row[3]));
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertTrue(Boolean.parseBoolean(row[3]));
        }
    }
    
    @Test
    public void testSelectNoMatches() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where Value > 1000 (should be empty)
        List<String[]> result = selector.select(sampleData, row -> 
            Double.parseDouble(row[2]) > 1000);
        
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testSelectAllMatches() {
        SelectionOperation selector = new SelectionOperation();
        // Select all rows where Value >= 0 (should match all)
        List<String[]> result = selector.select(sampleData, row -> 
            Double.parseDouble(row[2]) >= 0);
        
        assertEquals(sampleData.size(), result.size());
    }
    
    @Test
    public void testSelectEmptyInput() {
        SelectionOperation selector = new SelectionOperation();
        List<String[]> emptyData = TestUtils.createSampleData(0);
        List<String[]> result = selector.select(emptyData, row -> true);
        
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testSelectComplexCondition() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where (Value > 300 AND Name contains "5") OR Flag is true
        Predicate<String[]> complexPredicate = row -> 
            (Double.parseDouble(row[2]) > 300 && row[1].contains("5")) 
            || Boolean.parseBoolean(row[3]);
            
        List<String[]> result = selector.select(sampleData, complexPredicate);
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertTrue(complexPredicate.test(row));
        }
    }
}