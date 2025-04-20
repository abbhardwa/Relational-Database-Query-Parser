package edu.buffalo.cse562;

import static org.junit.Assert.*;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;

public class HybridHashTest {
    private Table table1;
    private Table table2;
    private File testDir;
    private File swapDir;
    
    @Before
    public void setUp() throws IOException {
        // Create test directories
        testDir = new File("src/test/resources/testdata");
        if (!testDir.exists()) {
            testDir.mkdirs();
        }
        
        swapDir = new File("src/test/resources/testswap");
        if (!swapDir.exists()) {
            swapDir.mkdirs();
        }
        
        // Create test table files
        createTestTable1();
        createTestTable2();
    }
    
    private void createTestTable1() throws IOException {
        // Create table1 file
        File table1File = new File(testDir, "table1.tbl");
        try (FileWriter writer = new FileWriter(table1File)) {
            writer.write("1|John|25\n");
            writer.write("2|Jane|30\n");
            writer.write("3|Bob|35\n");
            writer.write("4|Alice|40\n");
            writer.write("5|David|45\n");
        }
        
        // Create Table object
        table1 = new Table("table1", 3, table1File, testDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns1 = new ArrayList<>();
        
        ColumnDefinition idColumn = new ColumnDefinition();
        idColumn.setColumnName("id");
        ColDataType idType = new ColDataType();
        idType.setDataType("int");
        idColumn.setColDataType(idType);
        
        ColumnDefinition nameColumn = new ColumnDefinition();
        nameColumn.setColumnName("name");
        ColDataType nameType = new ColDataType();
        nameType.setDataType("varchar");
        nameColumn.setColDataType(nameType);
        
        ColumnDefinition ageColumn = new ColumnDefinition();
        ageColumn.setColumnName("age");
        ColDataType ageType = new ColDataType();
        ageType.setDataType("int");
        ageColumn.setColDataType(ageType);
        
        columns1.add(idColumn);
        columns1.add(nameColumn);
        columns1.add(ageColumn);
        
        table1.setColumnDefinitions(columns1);
        
        // Set up column index map
        HashMap<String, Integer> columnMap1 = new HashMap<>();
        columnMap1.put("table1.id", 0);
        columnMap1.put("table1.name", 1);
        columnMap1.put("table1.age", 2);
        table1.getColumnIndexMap().putAll(columnMap1);
        
        // Load table data
        table1.populateTable();
    }
    
    private void createTestTable2() throws IOException {
        // Create table2 file
        File table2File = new File(testDir, "table2.tbl");
        try (FileWriter writer = new FileWriter(table2File)) {
            writer.write("1|Developer\n");
            writer.write("2|Manager\n");
            writer.write("3|Analyst\n");
            writer.write("6|Designer\n");
            writer.write("7|Tester\n");
        }
        
        // Create Table object
        table2 = new Table("table2", 2, table2File, testDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns2 = new ArrayList<>();
        
        ColumnDefinition idColumn = new ColumnDefinition();
        idColumn.setColumnName("id");
        ColDataType idType = new ColDataType();
        idType.setDataType("int");
        idColumn.setColDataType(idType);
        
        ColumnDefinition roleColumn = new ColumnDefinition();
        roleColumn.setColumnName("role");
        ColDataType roleType = new ColDataType();
        roleType.setDataType("varchar");
        roleColumn.setColDataType(roleType);
        
        columns2.add(idColumn);
        columns2.add(roleColumn);
        
        table2.setColumnDefinitions(columns2);
        
        // Set up column index map
        HashMap<String, Integer> columnMap2 = new HashMap<>();
        columnMap2.put("table2.id", 0);
        columnMap2.put("table2.role", 1);
        table2.getColumnIndexMap().putAll(columnMap2);
        
        // Load table data
        table2.populateTable();
    }
    
    @Test
    public void testEvaluateJoinInMemory() throws IOException {
        // Test in-memory join
        Table joinedTable = HybridHash.evaluateJoin(table1, table2, "table1", "table2", "id", null);
        
        // Verify the join result
        assertNotNull("Joined table should not be null", joinedTable);
        assertEquals("Joined table should have correct name", "table1|table2", joinedTable.getTableName());
        assertEquals("Joined table should have 5 columns", 5, joinedTable.getColumnCount());
        
        // Load the tuples from the joined table
        List<String> tuples = getJoinedTableTuples(joinedTable);
        
        // Verify the number of rows (inner join should have 3 matching rows)
        assertEquals("Should have 3 rows in join result", 3, tuples.size());
        
        // Verify join contains expected results
        HashSet<String> expectedTuples = new HashSet<>();
        expectedTuples.add("1|John|25|1|Developer");
        expectedTuples.add("2|Jane|30|2|Manager");
        expectedTuples.add("3|Bob|35|3|Analyst");
        
        for (String tuple : tuples) {
            assertTrue("Join result should contain expected tuple: " + tuple, 
                      expectedTuples.contains(tuple));
        }
    }
    
    @Test
    public void testEvaluateJoinWithSwapDirectory() throws IOException {
        // Test join using swap directory for disk-based processing
        Table joinedTable = HybridHash.evaluateJoin(table1, table2, "table1", "table2", "id", swapDir);
        
        // Verify the join result
        assertNotNull("Joined table should not be null", joinedTable);
        assertEquals("Joined table should have correct name", "table1|table2", joinedTable.getTableName());
        assertEquals("Joined table should have 5 columns", 5, joinedTable.getColumnCount());
        
        // Load the tuples from the joined table
        List<String> tuples = getJoinedTableTuples(joinedTable);
        
        // Verify the number of rows (inner join should have 3 matching rows)
        assertEquals("Should have 3 rows in join result", 3, tuples.size());
        
        // Verify join contains expected results
        HashSet<String> expectedTuples = new HashSet<>();
        expectedTuples.add("1|John|25|1|Developer");
        expectedTuples.add("2|Jane|30|2|Manager");
        expectedTuples.add("3|Bob|35|3|Analyst");
        
        for (String tuple : tuples) {
            assertTrue("Join result should contain expected tuple: " + tuple, 
                      expectedTuples.contains(tuple));
        }
        
        // Verify bucket files were cleaned up
        for (File file : swapDir.listFiles()) {
            if (file.getName().contains("bucket")) {
                fail("Bucket files should be cleaned up: " + file.getName());
            }
        }
    }
    
    @Test
    public void testEvaluateJoinWithLargeTable1() throws IOException {
        // Create larger version of table1 to force table2 to be used for hash map
        File largeTable1File = new File(testDir, "large_table1.tbl");
        try (FileWriter writer = new FileWriter(largeTable1File)) {
            for (int i = 1; i <= 20; i++) {
                writer.write(i + "|Name" + i + "|" + (20 + i) + "\n");
            }
        }
        
        Table largeTable1 = new Table("large_table1", 3, largeTable1File, testDir);
        largeTable1.setColumnDefinitions(table1.getColumnDefinitions());
        
        // Use the same column index map but update table name
        HashMap<String, Integer> columnMap = new HashMap<>();
        columnMap.put("large_table1.id", 0);
        columnMap.put("large_table1.name", 1);
        columnMap.put("large_table1.age", 2);
        largeTable1.getColumnIndexMap().putAll(columnMap);
        
        largeTable1.populateTable();
        
        // Test join where table1 is larger than table2
        Table joinedTable = HybridHash.evaluateJoin(largeTable1, table2, "large_table1", "table2", "id", null);
        
        // Verify the join result
        assertNotNull("Joined table should not be null", joinedTable);
        assertEquals("Joined table should have correct name", "large_table1|table2", joinedTable.getTableName());
        
        // Load the tuples from the joined table
        List<String> tuples = getJoinedTableTuples(joinedTable);
        
        // Verify the number of rows (inner join should have matches for ids 1, 2, 3, 6, 7)
        assertEquals("Should have 5 rows in join result", 5, tuples.size());
        
        // Verify join contains expected results for some sample rows
        boolean foundId1 = false;
        boolean foundId7 = false;
        
        for (String tuple : tuples) {
            if (tuple.startsWith("1|Name1|21|1|Developer")) {
                foundId1 = true;
            } else if (tuple.startsWith("7|Name7|27|7|Tester")) {
                foundId7 = true;
            }
        }
        
        assertTrue("Join should contain match for id=1", foundId1);
        assertTrue("Join should contain match for id=7", foundId7);
        
        // Clean up
        largeTable1File.delete();
    }
    
    @Test
    public void testEvaluateJoinWithNoMatches() throws IOException {
        // Create tables with no matching keys
        File noMatchTable1File = new File(testDir, "no_match_table1.tbl");
        try (FileWriter writer = new FileWriter(noMatchTable1File)) {
            writer.write("10|NoMatch1|25\n");
            writer.write("20|NoMatch2|30\n");
        }
        
        Table noMatchTable1 = new Table("no_match_table1", 3, noMatchTable1File, testDir);
        noMatchTable1.setColumnDefinitions(table1.getColumnDefinitions());
        
        HashMap<String, Integer> columnMap = new HashMap<>();
        columnMap.put("no_match_table1.id", 0);
        columnMap.put("no_match_table1.name", 1);
        columnMap.put("no_match_table1.age", 2);
        noMatchTable1.getColumnIndexMap().putAll(columnMap);
        
        noMatchTable1.populateTable();
        
        // Test join where no keys match
        Table joinedTable = HybridHash.evaluateJoin(noMatchTable1, table2, "no_match_table1", "table2", "id", null);
        
        // Verify the join result
        assertNotNull("Joined table should not be null", joinedTable);
        assertEquals("Joined table should have correct name", "no_match_table1|table2", joinedTable.getTableName());
        
        // Load the tuples from the joined table
        List<String> tuples = getJoinedTableTuples(joinedTable);
        
        // Verify the number of rows (inner join should have no matches)
        assertEquals("Should have 0 rows in join result", 0, tuples.size());
        
        // Clean up
        noMatchTable1File.delete();
    }
    
    private List<String> getJoinedTableTuples(Table joinedTable) {
        // Read all tuples from the joined table file
        try {
            joinedTable.populateTable();
            return joinedTable.getTuples();
        } catch (IOException e) {
            fail("Failed to read joined table tuples: " + e.getMessage());
            return null;
        }
    }
    
    @After
    public void tearDown() {
        // Clean up test files
        File table1File = new File(testDir, "table1.tbl");
        if (table1File.exists()) {
            table1File.delete();
        }
        
        File table2File = new File(testDir, "table2.tbl");
        if (table2File.exists()) {
            table2File.delete();
        }
        
        File joinFile = new File(testDir, "table1|table2.tbl");
        if (joinFile.exists()) {
            joinFile.delete();
        }
        
        File largeJoinFile = new File(testDir, "large_table1|table2.tbl");
        if (largeJoinFile.exists()) {
            largeJoinFile.delete();
        }
        
        File noMatchJoinFile = new File(testDir, "no_match_table1|table2.tbl");
        if (noMatchJoinFile.exists()) {
            noMatchJoinFile.delete();
        }
        
        // Clean any swap files
        if (swapDir.exists()) {
            for (File file : swapDir.listFiles()) {
                file.delete();
            }
        }
    }
}