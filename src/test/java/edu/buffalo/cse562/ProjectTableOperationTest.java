package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class ProjectTableOperationTest {
    
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
    public void testProjectSingleColumn() {
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] columns = new int[] { 1 }; // Only project Name column
        List<String[]> result = projector.project(sampleData, columns);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(1, result.get(i).length);
            assertEquals(sampleData.get(i)[1], result.get(i)[0]);
        }
    }
    
    @Test
    public void testProjectMultipleColumns() {
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] columns = new int[] { 0, 2 }; // Project ID and Value columns
        List<String[]> result = projector.project(sampleData, columns);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(2, result.get(i).length);
            assertEquals(sampleData.get(i)[0], result.get(i)[0]);
            assertEquals(sampleData.get(i)[2], result.get(i)[1]);
        }
    }
    
    @Test
    public void testProjectAllColumns() {
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] columns = new int[] { 0, 1, 2, 3 }; // Project all columns
        List<String[]> result = projector.project(sampleData, columns);
        
        assertEquals(sampleData.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            assertEquals(4, result.get(i).length);
            assertArrayEquals(sampleData.get(i), result.get(i));
        }
    }
    
    @Test
    public void testProjectEmptyInput() {
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] columns = new int[] { 0, 1 };
        List<String[]> emptyData = TestUtils.createSampleData(0);
        List<String[]> result = projector.project(emptyData, columns);
        
        assertTrue(result.isEmpty());
    }
    
    @Test(expected = IndexOutOfBoundsException.class)
    public void testProjectInvalidColumn() {
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] columns = new int[] { 5 }; // Invalid column index
        projector.project(sampleData, columns);
    }
}