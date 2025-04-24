package edu.buffalo.cse562.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    
    /**
     * Writes lines to a file.
     * 
     * @param file The file to write to
     * @param lines The lines to write
     * @param append Whether to append to the file
     * @throws IOException If an I/O error occurs
     */
    public static void writeLines(File file, List<String> lines, boolean append) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, append))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
    }
    
    /**
     * Creates a temporary file.
     * 
     * @param prefix File name prefix
     * @param suffix File name suffix
     * @return The created temporary file
     * @throws IOException If an I/O error occurs
     */
    public static File createTempFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(prefix, suffix).toFile();
    }
    
    /**
     * Creates a temporary directory.
     * 
     * @param prefix Directory name prefix
     * @return The created temporary directory
     * @throws IOException If an I/O error occurs
     */
    public static File createTempDirectory(String prefix) throws IOException {
        return Files.createTempDirectory(prefix).toFile();
    }
    
    /**
     * Finds files with a specific extension in a directory.
     * 
     * @param directory The directory to search
     * @param extension The file extension to match
     * @return List of matching files
     */
    public static List<File> findFilesWithExtension(File directory, String extension) {
        List<File> result = new ArrayList<>();
        if (!directory.exists() || !directory.isDirectory()) {
            return result;
        }
        
        File[] files = directory.listFiles();
        if (files == null) {
            return result;
        }
        
        for (File file : files) {
            if (file.isFile() && file.getName().endsWith(extension)) {
                result.add(file);
            }
        }
        
        return result;
    }
    
    /**
     * Creates a directory if it doesn't exist.
     * 
     * @param path The path to create
     * @return The directory File object
     */
    public static File createDirectoryIfNotExists(String path) {
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
}