package edu.buffalo.cse562;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestUtils {
    
    private static final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private static final PrintStream originalOut = System.out;
    private static final Random random = new Random(42); // Fixed seed for reproducibility
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Redirect System.out to capture output
     */
    public static void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    /**
     * Restore System.out to original state
     */
    public static void restoreStreams() {
        System.setOut(originalOut);
    }

    /**
     * Get captured output as string
     */
    public static String getOutput() {
        return outContent.toString();
    }

    /**
     * Clear captured output
     */
    public static void clearOutput() {
        outContent.reset();
    }

    /**
     * Create sample data for testing with basic types
     * @param size Number of rows to generate
     * @return List of sample rows
     */
    public static List<String[]> createSampleData(int size) {
        return createSampleData(size, false, false);
    }

    /**
     * Create sample data with optional null values and special cases
     * @param size Number of rows to generate
     * @param includeNulls Whether to include null values
     * @param includeSpecialCases Whether to include special cases (empty strings, special chars, etc)
     * @return List of sample rows
     */
    public static List<String[]> createSampleData(int size, boolean includeNulls, boolean includeSpecialCases) {
        List<String[]> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String[] row = new String[7]; // Extended columns for different data types
            
            // ID (numeric)
            row[0] = String.valueOf(i);
            
            // Name (string)
            if (includeSpecialCases && i % 5 == 0) {
                row[1] = "Name'With\"Special,Chars" + i;
            } else if (includeNulls && i % 7 == 0) {
                row[1] = null;
            } else {
                row[1] = "Name" + i;
            }
            
            // Value (decimal)
            if (includeNulls && i % 11 == 0) {
                row[2] = null;
            } else if (includeSpecialCases && i % 13 == 0) {
                row[2] = String.valueOf(Double.MAX_VALUE);
            } else {
                row[2] = String.format("%.2f", i * 100.0);
            }
            
            // Flag (boolean)
            if (includeNulls && i % 5 == 0) {
                row[3] = null;
            } else {
                row[3] = String.valueOf(i % 2 == 0);
            }
            
            // Date
            if (includeNulls && i % 3 == 0) {
                row[4] = null;
            } else {
                Calendar cal = Calendar.getInstance();
                cal.set(2023, 0, 1);
                cal.add(Calendar.DAY_OF_YEAR, i);
                row[4] = dateFormat.format(cal.getTime());
            }
            
            // Integer
            if (includeNulls && i % 4 == 0) {
                row[5] = null;
            } else if (includeSpecialCases && i % 19 == 0) {
                row[5] = String.valueOf(Integer.MAX_VALUE);
            } else {
                row[5] = String.valueOf(i * 1000);
            }
            
            // Text with special chars
            if (includeSpecialCases && i % 7 == 0) {
                row[6] = "Line 1\nLine 2\tTabbed\u0000NullChar";
            } else if (includeNulls && i % 6 == 0) {
                row[6] = null;
            } else {
                row[6] = "Regular text " + i;
            }
            
            data.add(row);
        }
        return data;
    }

    /**
     * Create sorted data for testing ORDER BY operations
     * @param size Number of rows
     * @param sortColumns Column indices to sort by
     * @param ascending Array of booleans indicating sort direction for each column
     * @return Sorted list of rows
     */
    public static List<String[]> createSortedData(int size, int[] sortColumns, boolean[] ascending) {
        List<String[]> data = createSampleData(size, false, false);
        data.sort((a, b) -> {
            for (int i = 0; i < sortColumns.length; i++) {
                int col = sortColumns[i];
                int cmp = a[col].compareTo(b[col]);
                if (cmp != 0) {
                    return ascending[i] ? cmp : -cmp;
                }
            }
            return 0;
        });
        return data;
    }

    /**
     * Check if data is sorted correctly
     * @param data List of rows to check
     * @param sortColumns Column indices that should be sorted
     * @param ascending Array of booleans indicating sort direction for each column
     * @return true if data is sorted correctly
     */
    public static boolean isSorted(List<String[]> data, int[] sortColumns, boolean[] ascending) {
        if (data.size() <= 1) return true;
        
        for (int i = 1; i < data.size(); i++) {
            String[] prev = data.get(i - 1);
            String[] curr = data.get(i);
            
            for (int j = 0; j < sortColumns.length; j++) {
                int col = sortColumns[j];
                int cmp = prev[col].compareTo(curr[col]);
                
                if (cmp != 0) {
                    if (ascending[j] && cmp > 0) return false;
                    if (!ascending[j] && cmp < 0) return false;
                    break;
                }
            }
        }
        return true;
    }
}