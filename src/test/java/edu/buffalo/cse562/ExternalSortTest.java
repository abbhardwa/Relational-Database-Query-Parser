package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import static org.junit.Assert.*;
import java.io.File;

public class ExternalSortTest {
    
    private List<String[]> largeData;
    private ExternalSort sorter;
    private static final String TEMP_DIR = "/tmp/external_sort_test";
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        // Create larger dataset for external sort testing
        largeData = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeData.add(new String[] {
                String.valueOf(i),
                "Name" + (1000 - i),
                String.valueOf(Math.random() * 1000),
                String.valueOf(i % 2 == 0)
            });
        }
        
        // Create temp directory for sort files
        new File(TEMP_DIR).mkdirs();
        sorter = new ExternalSort(TEMP_DIR);
    }
    
    @After
    public void tearDown() {
        TestUtils.restoreStreams();
        TestUtils.clearOutput();
        
        // Clean up temp files
        deleteDirectory(new File(TEMP_DIR));
    }
    
    @Test
    public void testSortByOneColumn() {
        int[] sortColumns = {0}; // Sort by ID
        boolean[] ascending = {true};
        
        List<String[]> result = sorter.sort(largeData, sortColumns, ascending);
        
        assertEquals(largeData.size(), result.size());
        
        // Verify sorting
        for (int i = 1; i < result.size(); i++) {
            int prev = Integer.parseInt(result.get(i-1)[0]);
            int curr = Integer.parseInt(result.get(i)[0]);
            assertTrue(prev <= curr);
        }
    }
    
    @Test
    public void testSortByMultipleColumns() {
        int[] sortColumns = {3, 2}; // Sort by flag then value
        boolean[] ascending = {true, false};
        
        List<String[]> result = sorter.sort(largeData, sortColumns, ascending);
        
        assertEquals(largeData.size(), result.size());
        
        // Verify sorting
        for (int i = 1; i < result.size(); i++) {
            boolean prevFlag = Boolean.parseBoolean(result.get(i-1)[3]);
            boolean currFlag = Boolean.parseBoolean(result.get(i)[3]);
            
            if (prevFlag == currFlag) {
                // For same flags, check value is in descending order
                double prevVal = Double.parseDouble(result.get(i-1)[2]);
                double currVal = Double.parseDouble(result.get(i)[2]);
                assertTrue(prevVal >= currVal);
            } else {
                // Check flags are in ascending order
                assertTrue(!prevFlag || currFlag);
            }
        }
    }
    
    @Test
    public void testSortStringColumn() {
        int[] sortColumns = {1}; // Sort by Name
        boolean[] ascending = {true};
        
        List<String[]> result = sorter.sort(largeData, sortColumns, ascending);
        
        assertEquals(largeData.size(), result.size());
        
        // Verify sorting
        for (int i = 1; i < result.size(); i++) {
            String prev = result.get(i-1)[1];
            String curr = result.get(i)[1];
            assertTrue(prev.compareTo(curr) <= 0);
        }
    }
    
    @Test
    public void testSortWithDuplicates() {
        // Create data with duplicate values
        List<String[]> duplicateData = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            duplicateData.add(new String[] {
                String.valueOf(i),
                "Name" + (i % 10), // Only 10 different names
                String.valueOf(100),
                "true"
            });
        }
        
        int[] sortColumns = {1}; // Sort by Name
        boolean[] ascending = {true};
        
        List<String[]> result = sorter.sort(duplicateData, sortColumns, ascending);
        
        assertEquals(duplicateData.size(), result.size());
        
        // Verify sorting is stable (maintains relative order of equal elements)
        for (int i = 1; i < result.size(); i++) {
            String prev = result.get(i-1)[1];
            String curr = result.get(i)[1];
            assertTrue(prev.compareTo(curr) <= 0);
        }
    }
    
    @Test
    public void testSortEmptyInput() {
        List<String[]> emptyData = new ArrayList<>();
        int[] sortColumns = {0};
        boolean[] ascending = {true};
        
        List<String[]> result = sorter.sort(emptyData, sortColumns, ascending);
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testSortSingleElement() {
        List<String[]> singleElement = new ArrayList<>();
        singleElement.add(new String[] {"1", "Name1", "100", "true"});
        
        int[] sortColumns = {0};
        boolean[] ascending = {true};
        
        List<String[]> result = sorter.sort(singleElement, sortColumns, ascending);
        assertEquals(1, result.size());
        assertArrayEquals(singleElement.get(0), result.get(0));
    }
    
    @Test
    public void testSortLargeDataset() {
        // Create very large dataset to force external sorting
        List<String[]> veryLargeData = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            veryLargeData.add(new String[] {
                String.valueOf(i),
                "Name" + (100000 - i),
                String.valueOf(Math.random() * 1000),
                String.valueOf(i % 2 == 0)
            });
        }
        
        int[] sortColumns = {0};
        boolean[] ascending = {true};
        
        List<String[]> result = sorter.sort(veryLargeData, sortColumns, ascending);
        
        assertEquals(veryLargeData.size(), result.size());
        
        // Verify sorting
        for (int i = 1; i < result.size(); i++) {
            int prev = Integer.parseInt(result.get(i-1)[0]);
            int curr = Integer.parseInt(result.get(i)[0]);
            assertTrue(prev <= curr);
        }
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidColumnIndex() {
        int[] sortColumns = {5}; // Invalid column index
        boolean[] ascending = {true};
        
        sorter.sort(largeData, sortColumns, ascending);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testMismatchedSortParameters() {
        int[] sortColumns = {0, 1};
        boolean[] ascending = {true}; // Missing second direction
        
        sorter.sort(largeData, sortColumns, ascending);
    }
    
    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}