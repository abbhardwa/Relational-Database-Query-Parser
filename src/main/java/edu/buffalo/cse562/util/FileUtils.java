/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for file operations.
 */
public class FileUtils {
    private FileUtils() {
        // Prevent instantiation
    }

    /**
     * Reads all lines from a file.
     *
     * @param file The file to read
     * @return List of lines from the file
     * @throws IOException If an I/O error occurs
     */
    public static List<String> readLines(File file) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    /**
     * Finds a table file in the given directory.
     * Checks for both .dat and .tbl extensions.
     *
     * @param directory Directory to search in
     * @param tableName Name of the table without extension
     * @return The table file or null if not found
     */
    public static File findTableFile(File directory, String tableName) {
        File datFile = new File(directory, tableName + ".dat");
        if (datFile.exists()) {
            return datFile;
        }

        File tblFile = new File(directory, tableName + ".tbl");
        if (tblFile.exists()) {
            return tblFile;
        }

        return null;
    }
}