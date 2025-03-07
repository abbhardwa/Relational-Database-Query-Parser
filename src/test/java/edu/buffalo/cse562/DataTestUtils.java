package edu.buffalo.cse562;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for generating test data and validating results
 */
public class DataTestUtils {
    private static final Random random = new Random(42); // Fixed seed for reproducibility
    
    /**
     * Generate a test orders table with random but consistent data
     */
    public static List<String[]> createOrdersTable(int numOrders, int numCustomers) {
        List<String[]> orders = new ArrayList<>();
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        
        for (int i = 0; i < numOrders; i++) {
            orders.add(new String[] {
                String.valueOf(i),                    // order_id
                String.valueOf(random.nextInt(numCustomers)), // customer_id
                String.format("%.2f", random.nextDouble() * 1000), // amount
                startDate.plusDays(random.nextInt(365)).format(DateTimeFormatter.ISO_DATE), // date
                String.valueOf(random.nextInt(5)),    // status
                String.valueOf(random.nextInt(10))    // priority
            });
        }
        return orders;
    }
    
    /**
     * Generate a test customers table with random but consistent data
     */
    public static List<String[]> createCustomersTable(int numCustomers) {
        List<String[]> customers = new ArrayList<>();
        String[] regions = {"North", "South", "East", "West", "Central"};
        
        for (int i = 0; i < numCustomers; i++) {
            customers.add(new String[] {
                String.valueOf(i),                    // customer_id
                "Customer" + i,                       // name
                regions[random.nextInt(regions.length)], // region
                String.valueOf(1000 + random.nextInt(9000)), // credit_limit
                String.valueOf(random.nextBoolean())  // active
            });
        }
        return customers;
    }
    
    /**
     * Verify that a result set contains only unique rows
     */
    public static boolean hasNoDuplicates(List<String[]> data) {
        Map<String, Boolean> seen = new HashMap<>();
        for (String[] row : data) {
            String key = String.join("|", row);
            if (seen.containsKey(key)) {
                return false;
            }
            seen.put(key, true);
        }
        return true;
    }
    
    /**
     * Check if a result set is sorted by specified columns
     */
    public static boolean isSorted(List<String[]> data, int[] columns, boolean[] ascending) {
        if (data.size() <= 1) return true;
        
        for (int i = 1; i < data.size(); i++) {
            String[] prev = data.get(i-1);
            String[] curr = data.get(i);
            
            for (int j = 0; j < columns.length; j++) {
                int col = columns[j];
                int comp = compare(prev[col], curr[col]);
                
                if (comp != 0) {
                    if (ascending[j] && comp > 0) return false;
                    if (!ascending[j] && comp < 0) return false;
                    break;
                }
            }
        }
        return true;
    }
    
    /**
     * Compare two strings as numbers if possible, otherwise lexicographically
     */
    private static int compare(String a, String b) {
        try {
            double numA = Double.parseDouble(a);
            double numB = Double.parseDouble(b);
            return Double.compare(numA, numB);
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }
    
    /**
     * Calculate aggregate values for a column
     */
    public static Map<String, Double> calculateAggregates(List<String[]> data, int column) {
        Map<String, Double> results = new HashMap<>();
        
        if (data.isEmpty()) {
            results.put("MIN", 0.0);
            results.put("MAX", 0.0);
            results.put("SUM", 0.0);
            results.put("AVG", 0.0);
            results.put("COUNT", 0.0);
            return results;
        }
        
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double sum = 0;
        int count = 0;
        
        for (String[] row : data) {
            try {
                double value = Double.parseDouble(row[column]);
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                count++;
            } catch (NumberFormatException e) {
                // Skip non-numeric values
            }
        }
        
        results.put("MIN", min);
        results.put("MAX", max);
        results.put("SUM", sum);
        results.put("AVG", count > 0 ? sum/count : 0);
        results.put("COUNT", (double)count);
        
        return results;
    }
    
    /**
     * Create a deep copy of a data set
     */
    public static List<String[]> deepCopy(List<String[]> data) {
        List<String[]> copy = new ArrayList<>();
        for (String[] row : data) {
            copy.add(row.clone());
        }
        return copy;
    }
}