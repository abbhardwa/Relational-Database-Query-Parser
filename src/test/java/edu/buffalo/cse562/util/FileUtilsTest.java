package edu.buffalo.cse562.util;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import static org.junit.Assert.*;

public class FileUtilsTest {
    private File testDataDir;
    private File tempTestFile;

    @Before
    public void setUp() throws IOException {
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        
        // Create a temporary test file for our tests
        tempTestFile = new File(testDataDir, "temp_test.tbl");
        try (FileWriter writer = new FileWriter(tempTestFile)) {
            writer.write("line1|value1|123\n");
            writer.write("line2|value2|456\n");
        }
    }

    @After
    public void tearDown() {
        // Clean up temporary test file
        if (tempTestFile != null && tempTestFile.exists()) {
            tempTestFile.delete();
        }
    }

    @Test
    public void testReadLines() throws IOException {
        List<String> lines = FileUtils.readLines(tempTestFile);
        
        assertNotNull("Lines should not be null", lines);
        assertEquals("Should read 2 lines", 2, lines.size());
        assertEquals("First line should match", "line1|value1|123", lines.get(0));
        assertEquals("Second line should match", "line2|value2|456", lines.get(1));
    }

    @Test
    public void testReadLinesEmptyFile() throws IOException {
        File emptyFile = new File(testDataDir, "empty_test.tbl");
        try (FileWriter writer = new FileWriter(emptyFile)) {
            // Create empty file
        }
        
        List<String> lines = FileUtils.readLines(emptyFile);
        
        assertNotNull("Lines should not be null", lines);
        assertEquals("Should read 0 lines", 0, lines.size());
        
        // Clean up
        emptyFile.delete();
    }

    @Test
    public void testReadLinesSingleLine() throws IOException {
        File singleLineFile = new File(testDataDir, "single_line_test.tbl");
        try (FileWriter writer = new FileWriter(singleLineFile)) {
            writer.write("single|line|data");
        }
        
        List<String> lines = FileUtils.readLines(singleLineFile);
        
        assertNotNull("Lines should not be null", lines);
        assertEquals("Should read 1 line", 1, lines.size());
        assertEquals("Single line should match", "single|line|data", lines.get(0));
        
        // Clean up
        singleLineFile.delete();
    }

    @Test
    public void testReadLinesWithSpecialCharacters() throws IOException {
        File specialFile = new File(testDataDir, "special_chars_test.tbl");
        try (FileWriter writer = new FileWriter(specialFile)) {
            writer.write("data|with spaces|123\n");
            writer.write("data,with,commas|456|789\n");
            writer.write("data|with|unicode|éñâ\n");
        }
        
        List<String> lines = FileUtils.readLines(specialFile);
        
        assertNotNull("Lines should not be null", lines);
        assertEquals("Should read 3 lines", 3, lines.size());
        assertEquals("First line should match", "data|with spaces|123", lines.get(0));
        assertEquals("Second line should match", "data,with,commas|456|789", lines.get(1));
        assertEquals("Third line should match", "data|with|unicode|éñâ", lines.get(2));
        
        // Clean up
        specialFile.delete();
    }

    @Test(expected = IOException.class)
    public void testReadLinesNonExistentFile() throws IOException {
        File nonExistentFile = new File(testDataDir, "nonexistent.txt");
        FileUtils.readLines(nonExistentFile);
    }

    @Test(expected = IOException.class)
    public void testReadLinesNullFile() throws IOException {
        FileUtils.readLines(null);
    }

    @Test
    public void testFindTableFileDat() throws IOException {
        File datFile = new File(testDataDir, "sample.dat");
        try (FileWriter writer = new FileWriter(datFile)) {
            writer.write("test data");
        }
        
        File found = FileUtils.findTableFile(testDataDir, "sample");
        assertNotNull("Should find .dat file", found);
        assertEquals("Found file should be sample.dat", 
                    datFile.getAbsolutePath(), 
                    found.getAbsolutePath());
        
        // Clean up
        datFile.delete();
    }

    @Test
    public void testFindTableFileTbl() throws IOException {
        File tblFile = new File(testDataDir, "test_table.tbl");
        try (FileWriter writer = new FileWriter(tblFile)) {
            writer.write("test data");
        }
        
        File found = FileUtils.findTableFile(testDataDir, "test_table");
        assertNotNull("Should find .tbl file", found);
        assertEquals("Found file should be test_table.tbl", 
                    tblFile.getAbsolutePath(), 
                    found.getAbsolutePath());
        
        // Clean up
        tblFile.delete();
    }

    @Test
    public void testFindTableFilePrefersDat() throws IOException {
        // Create both .dat and .tbl files with same name
        File datFile = new File(testDataDir, "prefer_test.dat");
        File tblFile = new File(testDataDir, "prefer_test.tbl");
        
        try (FileWriter writer = new FileWriter(datFile)) {
            writer.write("dat content");
        }
        try (FileWriter writer = new FileWriter(tblFile)) {
            writer.write("tbl content");
        }
        
        File found = FileUtils.findTableFile(testDataDir, "prefer_test");
        assertNotNull("Should find a file", found);
        assertEquals("Should prefer .dat file over .tbl", 
                    datFile.getAbsolutePath(), 
                    found.getAbsolutePath());
        
        // Clean up
        datFile.delete();
        tblFile.delete();
    }

    @Test
    public void testFindTableFileNotFound() {
        File found = FileUtils.findTableFile(testDataDir, "nonexistent");
        assertNull("Should return null for non-existent table", found);
    }

    @Test
    public void testFindTableFileNullDirectory() {
        File found = FileUtils.findTableFile(null, "sample");
        assertNull("Should return null for null directory", found);
    }

    @Test
    public void testFindTableFileNullTableName() {
        File found = FileUtils.findTableFile(testDataDir, null);
        assertNull("Should return null for null table name", found);
    }

    @Test
    public void testFindTableFileEmptyTableName() {
        File found = FileUtils.findTableFile(testDataDir, "");
        assertNull("Should return null for empty table name", found);
    }

    @Test
    public void testFindTableFileNonExistentDirectory() {
        File nonExistentDir = new File("non/existent/directory");
        File found = FileUtils.findTableFile(nonExistentDir, "sample");
        assertNull("Should return null for non-existent directory", found);
    }
}