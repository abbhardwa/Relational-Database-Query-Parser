/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562.util;

import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;
import static org.junit.Assert.*;

public class FileUtilsTest {
    private File testDataDir;

    @Before
    public void setUp() {
        testDataDir = new File("src/test/resources/testdata");
        assertTrue("Test data directory should exist", testDataDir.exists());
    }

    @Test
    public void testReadLines() throws IOException {
        File testFile = new File(testDataDir, "test.tbl");
        List<String> lines = FileUtils.readLines(testFile);
        
        assertNotNull("Lines should not be null", lines);
        assertEquals("Should read 2 lines", 2, lines.size());
        assertEquals("First line should match", "line1|value1|123", lines.get(0));
        assertEquals("Second line should match", "line2|value2|456", lines.get(1));
    }

    @Test(expected = IOException.class)
    public void testReadLinesNonExistentFile() throws IOException {
        File nonExistentFile = new File(testDataDir, "nonexistent.txt");
        FileUtils.readLines(nonExistentFile);
    }

    @Test
    public void testFindTableFileDat() {
        File found = FileUtils.findTableFile(testDataDir, "sample");
        assertNotNull("Should find .dat file", found);
        assertEquals("Found file should be sample.dat", 
                    new File(testDataDir, "sample.dat").getAbsolutePath(), 
                    found.getAbsolutePath());
    }

    @Test
    public void testFindTableFileTbl() {
        File found = FileUtils.findTableFile(testDataDir, "test");
        assertNotNull("Should find .tbl file", found);
        assertEquals("Found file should be test.tbl", 
                    new File(testDataDir, "test.tbl").getAbsolutePath(), 
                    found.getAbsolutePath());
    }

    @Test
    public void testFindTableFileNotFound() {
        File found = FileUtils.findTableFile(testDataDir, "nonexistent");
        assertNull("Should return null for non-existent table", found);
    }
}