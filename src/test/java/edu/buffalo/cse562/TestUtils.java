package edu.buffalo.cse562;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    
    private static final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private static final PrintStream originalOut = System.out;

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
     * Create sample data for testing
     * @param size Number of rows to generate
     * @return List of sample rows
     */
    public static List<String[]> createSampleData(int size) {
        List<String[]> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String[] row = new String[] {
                String.valueOf(i),              // ID
                "Name" + i,                     // Name
                String.valueOf(i * 100),        // Value
                String.valueOf(i % 2 == 0)      // Flag
            };
            data.add(row);
        }
        return data;
    }
}