package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

public class WhereOperationTest {
    private File testDataDir;
    private File dataFile;
    private Table table;

    @Before
    public void setUp() throws IOException {
        // Create test data directory and file
        testDataDir = new File("src/test/resources/testdata");
        testDataDir.mkdirs();
        dataFile = new File(testDataDir, "employees.tbl");
        
        // Create test data
        try (FileWriter writer = new FileWriter(dataFile)) {
            writer.write("1|John|30|5000\n");
            writer.write("2|Jane|25|4000\n");
            writer.write("3|Bob|35|6000\n");
            writer.write("4|Alice|28|4500\n");
        }

        // Set up table
        table = new Table("employees", 4, dataFile, testDataDir);
        
        // Set up column definitions
        ArrayList<ColumnDefinition> columns = new ArrayList<>();
        
        ColumnDefinition col1 = new ColumnDefinition();
        col1.setColumnName("id");
        columns.add(col1);
        
        ColumnDefinition col2 = new ColumnDefinition();
        col2.setColumnName("name");
        columns.add(col2);
        
        ColumnDefinition col3 = new ColumnDefinition();
        col3.setColumnName("age");
        columns.add(col3);
        
        ColumnDefinition col4 = new ColumnDefinition();
        col4.setColumnName("salary");
        columns.add(col4);
        
        table.setColumnDefinitions(columns);
        table.populateColumnIndexMap();
        table.populateTable();
    }

    @Test
    public void testEqualsCondition() {
        // Create equals condition: age = 30
        Column ageColumn = new Column();
        ageColumn.setColumnName("age");
        LongValue thirtyValue = new LongValue(30);
        EqualsTo equalsCondition = new EqualsTo();
        equalsCondition.setLeftExpression(ageColumn);
        equalsCondition.setRightExpression(thirtyValue);

        WhereOperation whereOp = new WhereOperation(table, equalsCondition);
        Table result = whereOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have one matching record", 1, result.getTuples().size());
        assertEquals("Should find John who is 30", "1|John|30|5000", result.getTuples().get(0));
    }

    @Test
    public void testGreaterThanCondition() {
        // Create condition: salary > 5000
        Column salaryColumn = new Column();
        salaryColumn.setColumnName("salary");
        LongValue salaryValue = new LongValue(5000);
        GreaterThan gtCondition = new GreaterThan();
        gtCondition.setLeftExpression(salaryColumn);
        gtCondition.setRightExpression(salaryValue);

        WhereOperation whereOp = new WhereOperation(table, gtCondition);
        Table result = whereOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertEquals("Should have one matching record", 1, result.getTuples().size());
        assertEquals("Should find Bob with salary > 5000", "3|Bob|35|6000", result.getTuples().get(0));
    }

    @Test
    public void testNoMatchingRecords() {
        // Create condition that matches no records: age = 50
        Column ageColumn = new Column();
        ageColumn.setColumnName("age");
        LongValue fiftyValue = new LongValue(50);
        EqualsTo equalsCondition = new EqualsTo();
        equalsCondition.setLeftExpression(ageColumn);
        equalsCondition.setRightExpression(fiftyValue);

        WhereOperation whereOp = new WhereOperation(table, equalsCondition);
        Table result = whereOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertTrue("No records should match", result.getTuples().isEmpty());
    }

    @Test
    public void testAllRecordsMatch() {
        // Create condition that matches all records: age > 20
        Column ageColumn = new Column();
        ageColumn.setColumnName("age");
        LongValue twentyValue = new LongValue(20);
        GreaterThan gtCondition = new GreaterThan();
        gtCondition.setLeftExpression(ageColumn);
        gtCondition.setRightExpression(twentyValue);

        WhereOperation whereOp = new WhereOperation(table, gtCondition);
        Table result = whereOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertEquals("All records should match", 4, result.getTuples().size());
        assertEquals("Original data should be preserved", table.getTuples(), result.getTuples());
    }

    @Test
    public void testEmptyTable() throws IOException {
        // Create empty table
        try (FileWriter writer = new FileWriter(dataFile)) {}
        table.populateTable();

        Column ageColumn = new Column();
        ageColumn.setColumnName("age");
        LongValue thirtyValue = new LongValue(30);
        EqualsTo equalsCondition = new EqualsTo();
        equalsCondition.setLeftExpression(ageColumn);
        equalsCondition.setRightExpression(thirtyValue);

        WhereOperation whereOp = new WhereOperation(table, equalsCondition);
        Table result = whereOp.evaluate();

        assertNotNull("Result should not be null", result);
        assertTrue("Result should be empty", result.getTuples().isEmpty());
    }
}