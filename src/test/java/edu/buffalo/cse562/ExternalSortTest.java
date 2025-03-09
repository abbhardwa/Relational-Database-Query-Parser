package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.util.TableComparator;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ExternalSortTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private ExternalSort sorter;
    private Table testTable;
    private File dataDir;
    private File dataFile;
    
    @Before
    public void setUp() throws IOException {
        sorter = new ExternalSort(new ExternalSort.Config()
            .setChunkSize(2)  // Small chunk size for testing
            .setTempFilePrefix("test_chunk"));
            
        dataDir = tempFolder.newFolder("test_data");
        dataFile = new File(dataDir, "test.tbl");
        
        // Create test data file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(dataFile))) {
            writer.write("5|Test Name 5|100.00\n");
            writer.write("3|Test Name 3|30.50\n");
            writer.write("1|Test Name 1|75.25\n");
            writer.write("4|Test Name 4|45.75\n");
            writer.write("2|Test Name 2|60.00\n");
        }
        
        testTable = new Table("test", 3, dataFile, dataDir.getAbsolutePath());
        testTable.columnDescriptionList = Arrays.asList("id", "name", "price");
        testTable.columnIndexMap = new HashMap<>();
        testTable.columnIndexMap.put("id", 0);
        testTable.columnIndexMap.put("name", 1);
        testTable.columnIndexMap.put("price", 2);
    }
    
    @Test
    public void testSortByIdAscending() throws IOException {
        List orderByList = Arrays.asList("id");
        Table sortedTable = sorter.sortTable(testTable, orderByList);
        
        // Verify results
        String[] expected = {
            "1|Test Name 1|75.25",
            "2|Test Name 2|60.00",
            "3|Test Name 3|30.50",
            "4|Test Name 4|45.75",
            "5|Test Name 5|100.00"
        };
        
        verifyTableContents(sortedTable, expected);
    }
    
    @Test
    public void testSortByPriceDescending() throws IOException {
        List orderByList = Arrays.asList("price DESC");
        Table sortedTable = sorter.sortTable(testTable, orderByList);
        
        // Verify results
        String[] expected = {
            "5|Test Name 5|100.00",
            "1|Test Name 1|75.25",
            "2|Test Name 2|60.00",
            "4|Test Name 4|45.75",
            "3|Test Name 3|30.50"
        };
        
        verifyTableContents(sortedTable, expected);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSortWithNullTable() throws IOException {
        sorter.sortTable(null, Arrays.asList("id"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testSortWithEmptyOrderBy() throws IOException {
        sorter.sortTable(testTable, new ArrayList<>());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidChunkSize() {
        new ExternalSort(new ExternalSort.Config().setChunkSize(-1));
    }
    
    private void verifyTableContents(Table table, String[] expected) throws IOException {
        int i = 0;
        String tuple;
        while ((tuple = table.returnTuple()) != null) {
            assertEquals("Row " + i + " mismatch", expected[i], tuple);
            i++;
        }
        assertEquals("Number of rows mismatch", expected.length, i);
    }
    
    @After
    public void cleanup() {
        // Clean up temp files
        File[] tempFiles = dataDir.listFiles((dir, name) -> name.startsWith("test_chunk"));
        if (tempFiles != null) {
            for (File file : tempFiles) {
                file.delete();
            }
        }
    }
}