package edu.buffalo.cse562.util;

import edu.buffalo.cse562.model.Table;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.*;

public class TableComparatorTest {
    private Table testTable;
    private TableComparator comparator;
    
    @Before
    public void setUp() {
        testTable = new Table("test", 3, new File("test.tbl"), "/tmp");
        testTable.columnDescriptionList = Arrays.asList("id", "name", "price");
        testTable.columnIndexMap = new HashMap<>();
        testTable.columnIndexMap.put("id", 0);
        testTable.columnIndexMap.put("name", 1);
        testTable.columnIndexMap.put("price", 2);
    }
    
    @Test
    public void testCompareIntegers() {
        comparator = new TableComparator(testTable, Arrays.asList("id"), 1);
        
        String row1 = "1|Test Name|100.00";
        String row2 = "2|Test Name|100.00";
        String row3 = "1|Test Name|100.00";
        
        assertTrue(comparator.compare(row1, row2) < 0);
        assertTrue(comparator.compare(row2, row1) > 0);
        assertEquals(0, comparator.compare(row1, row3));
    }
    
    @Test
    public void testCompareStrings() {
        comparator = new TableComparator(testTable, Arrays.asList("name"), 1);
        
        String row1 = "1|Apple|100.00";
        String row2 = "2|Banana|100.00";
        String row3 = "3|Apple|100.00";
        
        assertTrue(comparator.compare(row1, row2) < 0);
        assertTrue(comparator.compare(row2, row1) > 0);
        assertEquals(0, comparator.compare(row1, row3));
    }
    
    @Test
    public void testCompareDecimals() {
        comparator = new TableComparator(testTable, Arrays.asList("price"), 1);
        
        String row1 = "1|Test|50.25";
        String row2 = "2|Test|100.50";
        String row3 = "3|Test|50.25";
        
        assertTrue(comparator.compare(row1, row2) < 0);
        assertTrue(comparator.compare(row2, row1) > 0);
        assertEquals(0, comparator.compare(row1, row3));
    }
    
    @Test
    public void testCompareDescending() {
        comparator = new TableComparator(testTable, Arrays.asList("id DESC"), 1);
        
        String row1 = "1|Test|100.00";
        String row2 = "2|Test|100.00";
        
        assertTrue(comparator.compare(row1, row2) > 0);
        assertTrue(comparator.compare(row2, row1) < 0);
    }
    
    @Test
    public void testMultipleOrderByColumns() {
        comparator = new TableComparator(testTable, Arrays.asList("name", "id DESC"), 2);
        
        String row1 = "1|Apple|100.00";
        String row2 = "2|Apple|100.00";
        String row3 = "1|Banana|100.00";
        
        assertTrue(comparator.compare(row1, row2) > 0);  // Same name, different id (DESC)
        assertTrue(comparator.compare(row1, row3) < 0);  // Different name
    }
}