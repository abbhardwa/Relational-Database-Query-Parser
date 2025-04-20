package edu.buffalo.cse562;

import static org.junit.Assert.*;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.schema.Table;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.PrimaryTreeMap;

public class BuildIndexesTest {
    private File indexDirectory;
    private File dataDirectory;
    private CreateTable testCreateTable;
    private HashMap<String, File> originalTablesNameAndFileMap;
    
    @Before
    public void setUp() throws IOException {
        // Create temporary directories
        dataDirectory = new File("src/test/resources/testdata");
        if (!dataDirectory.exists()) {
            dataDirectory.mkdirs();
        }
        
        indexDirectory = new File("src/test/resources/testindex");
        if (!indexDirectory.exists()) {
            indexDirectory.mkdirs();
        }
        
        // Save original state of tablesNameAndFileMap
        originalTablesNameAndFileMap = Main.tablesNameAndFileMap;
        Main.tablesNameAndFileMap = new HashMap<>();
        
        // Create test table file
        File tableFile = new File(dataDirectory, "test_index.tbl");
        try (FileWriter writer = new FileWriter(tableFile)) {
            writer.write("1|John|Doe|25\n");
            writer.write("2|Jane|Smith|30\n");
            writer.write("3|Bob|Johnson|35\n");
            writer.write("4|Alice|Williams|40\n");
            writer.write("5|David|Brown|45\n");
        }
        
        // Add to tablesNameAndFileMap
        Main.tablesNameAndFileMap.put("test_index", tableFile);
        
        // Create test CreateTable statement
        testCreateTable = new CreateTable();
        Table table = new Table();
        table.setName("test_index");
        testCreateTable.setTable(table);
        
        // Define columns
        List<ColumnDefinition> columns = new ArrayList<>();
        
        ColumnDefinition idColumn = new ColumnDefinition();
        idColumn.setColumnName("id");
        ColDataType idType = new ColDataType();
        idType.setDataType("int");
        idColumn.setColDataType(idType);
        columns.add(idColumn);
        
        ColumnDefinition firstNameColumn = new ColumnDefinition();
        firstNameColumn.setColumnName("firstname");
        ColDataType firstNameType = new ColDataType();
        firstNameType.setDataType("varchar");
        firstNameColumn.setColDataType(firstNameType);
        columns.add(firstNameColumn);
        
        ColumnDefinition lastNameColumn = new ColumnDefinition();
        lastNameColumn.setColumnName("lastname");
        ColDataType lastNameType = new ColDataType();
        lastNameType.setDataType("varchar");
        lastNameColumn.setColDataType(lastNameType);
        columns.add(lastNameColumn);
        
        ColumnDefinition ageColumn = new ColumnDefinition();
        ageColumn.setColumnName("age");
        ColDataType ageType = new ColDataType();
        ageType.setDataType("int");
        ageColumn.setColDataType(ageType);
        columns.add(ageColumn);
        
        testCreateTable.setColumnDefinitions(columns);
    }
    
    @Test
    public void testBuildIndexWithPrimaryKey() throws IOException {
        // Create a primary key index
        List<Index> indexes = new ArrayList<>();
        Index pkIndex = new Index();
        pkIndex.setType("PRIMARY KEY");
        
        List<String> pkColumnNames = new ArrayList<>();
        pkColumnNames.add("id");
        pkIndex.setColumnsNames(pkColumnNames);
        
        indexes.add(pkIndex);
        testCreateTable.setIndexes(indexes);
        
        // Build the index
        BuildIndexes.buildIndex(testCreateTable, indexDirectory);
        
        // Check that the index file was created
        File indexFile = new File(indexDirectory, "test_index.index");
        assertTrue("Index file should exist", indexFile.exists());
        
        // Verify index content using RecordManager
        RecordManager recordManager = null;
        try {
            recordManager = RecordManagerFactory.createRecordManager(indexFile.getAbsolutePath());
            PrimaryTreeMap<String, ArrayList<String>> indexMap = 
                recordManager.treeMap("test_index.id|");
            
            // Verify key presence and content
            assertNotNull("Index map should not be null", indexMap);
            assertTrue("Index should contain key '1|'", indexMap.containsKey("1|"));
            assertTrue("Index should contain key '2|'", indexMap.containsKey("2|"));
            assertTrue("Index should contain key '3|'", indexMap.containsKey("3|"));
            assertTrue("Index should contain key '4|'", indexMap.containsKey("4|"));
            assertTrue("Index should contain key '5|'", indexMap.containsKey("5|"));
            
            // Verify data for a specific key
            ArrayList<String> records = indexMap.get("1|");
            assertNotNull("Records for key '1|' should not be null", records);
            assertEquals("Should have 1 record for key '1|'", 1, records.size());
            assertEquals("Record content should match", "1|John|Doe|25", records.get(0));
        } finally {
            if (recordManager != null) {
                recordManager.close();
            }
        }
    }
    
    @Test
    public void testBuildIndexWithSecondaryIndex() throws IOException {
        // Create a secondary index
        List<Index> indexes = new ArrayList<>();
        Index secIndex = new Index();
        secIndex.setType("INDEX");
        secIndex.setName("age_idx");
        
        List<String> secColumnNames = new ArrayList<>();
        secColumnNames.add("age");
        secIndex.setColumnsNames(secColumnNames);
        
        indexes.add(secIndex);
        testCreateTable.setIndexes(indexes);
        
        // Build the index
        BuildIndexes.buildIndex(testCreateTable, indexDirectory);
        
        // Check that the index file was created
        File indexFile = new File(indexDirectory, "test_index.index");
        assertTrue("Index file should exist", indexFile.exists());
        
        // Verify index content using RecordManager
        RecordManager recordManager = null;
        try {
            recordManager = RecordManagerFactory.createRecordManager(indexFile.getAbsolutePath());
            PrimaryTreeMap<String, ArrayList<String>> indexMap = 
                recordManager.treeMap("test_index.age_idx.indexkey");
            
            // Verify key presence and content
            assertNotNull("Index map should not be null", indexMap);
            assertTrue("Index should contain key '25'", indexMap.containsKey("25"));
            assertTrue("Index should contain key '30'", indexMap.containsKey("30"));
            assertTrue("Index should contain key '35'", indexMap.containsKey("35"));
            assertTrue("Index should contain key '40'", indexMap.containsKey("40"));
            assertTrue("Index should contain key '45'", indexMap.containsKey("45"));
            
            // Verify data for a specific key
            ArrayList<String> records = indexMap.get("25");
            assertNotNull("Records for key '25' should not be null", records);
            assertEquals("Should have 1 record for key '25'", 1, records.size());
            assertEquals("Record content should match", "1|John|Doe|25", records.get(0));
        } finally {
            if (recordManager != null) {
                recordManager.close();
            }
        }
    }
    
    @Test
    public void testBuildIndexWithCompositePrimaryKey() throws IOException {
        // Create a composite primary key index
        List<Index> indexes = new ArrayList<>();
        Index pkIndex = new Index();
        pkIndex.setType("PRIMARY KEY");
        
        List<String> pkColumnNames = new ArrayList<>();
        pkColumnNames.add("firstname");
        pkColumnNames.add("lastname");
        pkIndex.setColumnsNames(pkColumnNames);
        
        indexes.add(pkIndex);
        testCreateTable.setIndexes(indexes);
        
        // Build the index
        BuildIndexes.buildIndex(testCreateTable, indexDirectory);
        
        // Check that the index file was created
        File indexFile = new File(indexDirectory, "test_index.index");
        assertTrue("Index file should exist", indexFile.exists());
        
        // Verify index content using RecordManager
        RecordManager recordManager = null;
        try {
            recordManager = RecordManagerFactory.createRecordManager(indexFile.getAbsolutePath());
            PrimaryTreeMap<String, ArrayList<String>> indexMap = 
                recordManager.treeMap("test_index.firstname|lastname|");
            
            // Verify key presence and content
            assertNotNull("Index map should not be null", indexMap);
            assertTrue("Index should contain John|Doe| key", indexMap.containsKey("John|Doe|"));
            assertTrue("Index should contain Jane|Smith| key", indexMap.containsKey("Jane|Smith|"));
            
            // Verify data for a specific key
            ArrayList<String> records = indexMap.get("John|Doe|");
            assertNotNull("Records should not be null", records);
            assertEquals("Should have 1 record", 1, records.size());
            assertEquals("Record content should match", "1|John|Doe|25", records.get(0));
        } finally {
            if (recordManager != null) {
                recordManager.close();
            }
        }
    }
    
    @After
    public void tearDown() {
        // Restore original tablesNameAndFileMap
        Main.tablesNameAndFileMap = originalTablesNameAndFileMap;
        
        // Clean up test files
        File indexFile = new File(indexDirectory, "test_index.index");
        if (indexFile.exists()) {
            indexFile.delete();
        }
        
        File tableFile = new File(dataDirectory, "test_index.tbl");
        if (tableFile.exists()) {
            tableFile.delete();
        }
        
        File indexDataFile = new File(indexDirectory, "test_index.tbl");
        if (indexDataFile.exists()) {
            indexDataFile.delete();
        }
        
        // Delete index directory if possible
        if (indexDirectory.exists() && indexDirectory.list().length == 0) {
            indexDirectory.delete();
        }
    }
}