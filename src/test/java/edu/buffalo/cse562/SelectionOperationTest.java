package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.function.Predicate;
import java.text.SimpleDateFormat;
import java.util.Date;
import static org.junit.Assert.*;

public class SelectionOperationTest {
    
    private List<String[]> sampleData;
    private List<String[]> nullData;
    private List<String[]> specialData;
    private SimpleDateFormat dateFormat;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        sampleData = TestUtils.createSampleData(10, false, false);
        nullData = TestUtils.createSampleData(10, true, false);
        specialData = TestUtils.createSampleData(10, false, true);
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
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
    public void testSelectByNumericRangeCondition() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where 100 <= Value <= 500
        List<String[]> result = selector.select(sampleData, row -> {
            double value = Double.parseDouble(row[2]);
            return value >= 100 && value <= 500;
        });
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            double value = Double.parseDouble(row[2]);
            assertTrue(value >= 100 && value <= 500);
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
    public void testSelectByStringPattern() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where Name matches pattern Name[0-9]+
        List<String[]> result = selector.select(sampleData, row -> 
            row[1].matches("Name[0-9]+"));
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertTrue(row[1].matches("Name[0-9]+"));
        }
    }
    
    @Test
    public void testSelectByStringCaseInsensitive() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where Name contains "NAME" (case insensitive)
        List<String[]> result = selector.select(sampleData, row -> 
            row[1].toLowerCase().contains("name"));
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertTrue(row[1].toLowerCase().contains("name"));
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
    public void testSelectByDateCondition() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where Date > 2023-01-01
        List<String[]> result = selector.select(sampleData, row -> {
            try {
                Date rowDate = dateFormat.parse(row[4]);
                Date compareDate = dateFormat.parse("2023-01-01");
                return rowDate.after(compareDate);
            } catch (Exception e) {
                return false;
            }
        });
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            try {
                Date rowDate = dateFormat.parse(row[4]);
                Date compareDate = dateFormat.parse("2023-01-01");
                assertTrue(rowDate.after(compareDate));
            } catch (Exception e) {
                fail("Invalid date format");
            }
        }
    }
    
    @Test
    public void testSelectWithNullValues() {
        SelectionOperation selector = new SelectionOperation();
        // Select non-null values in a column that may contain nulls
        List<String[]> result = selector.select(nullData, row -> row[2] != null);
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertNotNull(row[2]);
        }
    }
    
    @Test
    public void testSelectWithNullComparison() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where either column is null
        List<String[]> result = selector.select(nullData, row -> 
            row[1] == null || row[2] == null);
        
        // Verify that at least one column is null in each result row
        for (String[] row : result) {
            assertTrue(row[1] == null || row[2] == null);
        }
    }
    
    @Test
    public void testSelectWithSpecialCharacters() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows where Name contains special characters
        List<String[]> result = selector.select(specialData, row -> 
            row[1] != null && row[1].matches(".*[^a-zA-Z0-9 ].*"));
        
        for (String[] row : result) {
            assertTrue(row[1].matches(".*[^a-zA-Z0-9 ].*"));
        }
    }
    
    @Test
    public void testSelectWithBoundaryValues() {
        SelectionOperation selector = new SelectionOperation();
        // Select rows with extreme numeric values
        List<String[]> result = selector.select(specialData, row -> {
            if (row[2] == null) return false;
            double value = Double.parseDouble(row[2]);
            return Double.isInfinite(value) || value == Double.MAX_VALUE || value == Double.MIN_VALUE;
        });
        
        for (String[] row : result) {
            double value = Double.parseDouble(row[2]);
            assertTrue(Double.isInfinite(value) || value == Double.MAX_VALUE || value == Double.MIN_VALUE);
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
    public void testSelectWithCompoundCondition() {
        SelectionOperation selector = new SelectionOperation();
        // Test AND condition
        List<String[]> result1 = selector.select(sampleData, row -> 
            Double.parseDouble(row[2]) > 300 && Boolean.parseBoolean(row[3]));
        
        for (String[] row : result1) {
            assertTrue(Double.parseDouble(row[2]) > 300);
            assertTrue(Boolean.parseBoolean(row[3]));
        }
        
        // Test OR condition
        List<String[]> result2 = selector.select(sampleData, row -> 
            Double.parseDouble(row[2]) > 800 || Boolean.parseBoolean(row[3]));
        
        for (String[] row : result2) {
            assertTrue(Double.parseDouble(row[2]) > 800 || Boolean.parseBoolean(row[3]));
        }
        
        // Test NOT condition
        List<String[]> result3 = selector.select(sampleData, row -> 
            !(Double.parseDouble(row[2]) < 300));
        
        for (String[] row : result3) {
            assertTrue(Double.parseDouble(row[2]) >= 300);
        }
    }
    
    @Test
    public void testSelectWithTypeConversion() {
        SelectionOperation selector = new SelectionOperation();
        
        // String to Number conversion
        List<String[]> result1 = selector.select(sampleData, row -> {
            try {
                int value = Integer.parseInt(row[0]);
                return value % 2 == 0;
            } catch (NumberFormatException e) {
                return false;
            }
        });
        
        for (String[] row : result1) {
            assertTrue(Integer.parseInt(row[0]) % 2 == 0);
        }
        
        // String to Boolean conversion
        List<String[]> result2 = selector.select(sampleData, row -> {
            String value = row[3].toLowerCase();
            return "true".equals(value) || "1".equals(value) || "yes".equals(value);
        });
        
        for (String[] row : result2) {
            String value = row[3].toLowerCase();
            assertTrue("true".equals(value) || "1".equals(value) || "yes".equals(value));
        }
    }
    
    @Test(expected = NumberFormatException.class)
    public void testSelectWithInvalidNumericConversion() {
        SelectionOperation selector = new SelectionOperation();
        // Try to parse non-numeric string as number
        selector.select(specialData, row -> 
            Double.parseDouble(row[1]) > 0); // Name column contains non-numeric data
    }
    
    @Test
    public void testSelectWithComplexLogic() {
        SelectionOperation selector = new SelectionOperation();
        // Complex condition combining multiple types and operators
        Predicate<String[]> complexPredicate = row -> {
            // Parse numeric value safely
            double value;
            try {
                value = Double.parseDouble(row[2]);
            } catch (NumberFormatException e) {
                return false;
            }
            
            // Check date range safely
            boolean inDateRange = false;
            try {
                Date rowDate = dateFormat.parse(row[4]);
                Date startDate = dateFormat.parse("2023-01-01");
                Date endDate = dateFormat.parse("2023-12-31");
                inDateRange = !rowDate.before(startDate) && !rowDate.after(endDate);
            } catch (Exception e) {
                inDateRange = false;
            }
            
            // Combine multiple conditions
            boolean hasValidName = row[1] != null && row[1].length() > 3;
            boolean isActiveFlag = Boolean.parseBoolean(row[3]);
            
            return (value > 300 || isActiveFlag) && // Value or flag condition
                   (hasValidName && inDateRange) && // Name and date condition
                   (value % 2 == 0 || row[1].contains("5")); // Additional filter
        };
        
        List<String[]> result = selector.select(sampleData, complexPredicate);
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertTrue(complexPredicate.test(row));
        }
    }
}
