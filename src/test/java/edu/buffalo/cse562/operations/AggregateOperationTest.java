package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class AggregateOperationTest {
    private Table testTable;
    private File testDir;
    
    @Before
    public void setUp() throws IOException {
        testDir = new File("src/test/resources/testdata");
        testTable = new Table("test_aggregate", 3, new File(testDir, "test.tbl"), testDir);
        
        // Add numeric data for aggregation
        ArrayList<String> tuples = new ArrayList<>();
        tuples.add("1|10|100");
        tuples.add("2|20|200");
        tuples.add("3|30|300");
        testTable.setTuples(tuples);
        
        // Set up column index map
        testTable.getColumnIndexMap().put("col1", 0);
        testTable.getColumnIndexMap().put("col2", 1);
        testTable.getColumnIndexMap().put("col3", 2);
    }
    
    @Test
    public void testExecuteSimpleAggregationCount() throws IOException {
        // Create count operation
        List<SelectExpressionItem> selectItems = new ArrayList<>();
        
        // Create COUNT function
        Function countFunction = new Function();
        countFunction.setName("COUNT");
        
        // Set function parameters (column to count)
        Column col = new Column();
        col.setColumnName("col1");
        
        List<Expression> expressions = new ArrayList<>();
        expressions.add(col);
        
        Function.NamedExpressionList namedExpressionList = new Function.NamedExpressionList();
        namedExpressionList.setExpressions(expressions);
        countFunction.setParameters(namedExpressionList);
        
        SelectExpressionItem item = new SelectExpressionItem();
        item.setExpression(countFunction);
        selectItems.add(item);
        
        // Execute aggregation
        AggregateOperation aggregateOp = new AggregateOperation(selectItems, null);
        Table result = aggregateOp.execute(testTable);
        
        // Verify result
        assertNotNull(result);
        assertEquals(1, result.getTuples().size());
        assertEquals("3", result.getTuples().get(0)); // Count of 3 records
    }
    
    @Test
    public void testExecuteSimpleAggregationSum() throws IOException {
        // Create sum operation
        List<SelectExpressionItem> selectItems = new ArrayList<>();
        
        // Create SUM function
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        
        // Set function parameters (column to sum)
        Column col = new Column();
        col.setColumnName("col2");
        
        List<Expression> expressions = new ArrayList<>();
        expressions.add(col);
        
        Function.NamedExpressionList namedExpressionList = new Function.NamedExpressionList();
        namedExpressionList.setExpressions(expressions);
        sumFunction.setParameters(namedExpressionList);
        
        SelectExpressionItem item = new SelectExpressionItem();
        item.setExpression(sumFunction);
        selectItems.add(item);
        
        // Execute aggregation
        AggregateOperation aggregateOp = new AggregateOperation(selectItems, null);
        Table result = aggregateOp.execute(testTable);
        
        // Verify result
        assertNotNull(result);
        assertEquals(1, result.getTuples().size());
        assertEquals("60", result.getTuples().get(0)); // Sum of 10+20+30
    }
    
    @Test
    public void testExecuteSimpleAggregationAvg() throws IOException {
        // Create avg operation
        List<SelectExpressionItem> selectItems = new ArrayList<>();
        
        // Create AVG function
        Function avgFunction = new Function();
        avgFunction.setName("AVG");
        
        // Set function parameters (column to average)
        Column col = new Column();
        col.setColumnName("col2");
        
        List<Expression> expressions = new ArrayList<>();
        expressions.add(col);
        
        Function.NamedExpressionList namedExpressionList = new Function.NamedExpressionList();
        namedExpressionList.setExpressions(expressions);
        avgFunction.setParameters(namedExpressionList);
        
        SelectExpressionItem item = new SelectExpressionItem();
        item.setExpression(avgFunction);
        selectItems.add(item);
        
        // Execute aggregation
        AggregateOperation aggregateOp = new AggregateOperation(selectItems, null);
        Table result = aggregateOp.execute(testTable);
        
        // Verify result
        assertNotNull(result);
        assertEquals(1, result.getTuples().size());
        assertEquals("20.00", result.getTuples().get(0)); // Average of 10+20+30 = 60/3 = 20
    }
    
    @Test
    public void testExecuteSimpleAggregationMin() throws IOException {
        // Create min operation
        List<SelectExpressionItem> selectItems = new ArrayList<>();
        
        // Create MIN function
        Function minFunction = new Function();
        minFunction.setName("MIN");
        
        // Set function parameters (column for min)
        Column col = new Column();
        col.setColumnName("col2");
        
        List<Expression> expressions = new ArrayList<>();
        expressions.add(col);
        
        Function.NamedExpressionList namedExpressionList = new Function.NamedExpressionList();
        namedExpressionList.setExpressions(expressions);
        minFunction.setParameters(namedExpressionList);
        
        SelectExpressionItem item = new SelectExpressionItem();
        item.setExpression(minFunction);
        selectItems.add(item);
        
        // Execute aggregation
        AggregateOperation aggregateOp = new AggregateOperation(selectItems, null);
        Table result = aggregateOp.execute(testTable);
        
        // Verify result
        assertNotNull(result);
        assertEquals(1, result.getTuples().size());
        assertEquals("10", result.getTuples().get(0)); // Min of 10, 20, 30
    }
    
    @Test
    public void testExecuteSimpleAggregationMax() throws IOException {
        // Create max operation
        List<SelectExpressionItem> selectItems = new ArrayList<>();
        
        // Create MAX function
        Function maxFunction = new Function();
        maxFunction.setName("MAX");
        
        // Set function parameters (column for max)
        Column col = new Column();
        col.setColumnName("col2");
        
        List<Expression> expressions = new ArrayList<>();
        expressions.add(col);
        
        Function.NamedExpressionList namedExpressionList = new Function.NamedExpressionList();
        namedExpressionList.setExpressions(expressions);
        maxFunction.setParameters(namedExpressionList);
        
        SelectExpressionItem item = new SelectExpressionItem();
        item.setExpression(maxFunction);
        selectItems.add(item);
        
        // Execute aggregation
        AggregateOperation aggregateOp = new AggregateOperation(selectItems, null);
        Table result = aggregateOp.execute(testTable);
        
        // Verify result
        assertNotNull(result);
        assertEquals(1, result.getTuples().size());
        assertEquals("30", result.getTuples().get(0)); // Max of 10, 20, 30
    }
    
    @Test
    public void testExecuteWithMultipleAggregations() throws IOException {
        // Create multiple aggregations
        List<SelectExpressionItem> selectItems = new ArrayList<>();
        
        // Add COUNT function
        Function countFunction = new Function();
        countFunction.setName("COUNT");
        
        Column col1 = new Column();
        col1.setColumnName("col1");
        
        List<Expression> expressions1 = new ArrayList<>();
        expressions1.add(col1);
        
        Function.NamedExpressionList namedExpressionList1 = new Function.NamedExpressionList();
        namedExpressionList1.setExpressions(expressions1);
        countFunction.setParameters(namedExpressionList1);
        
        SelectExpressionItem item1 = new SelectExpressionItem();
        item1.setExpression(countFunction);
        selectItems.add(item1);
        
        // Add SUM function
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        
        Column col2 = new Column();
        col2.setColumnName("col2");
        
        List<Expression> expressions2 = new ArrayList<>();
        expressions2.add(col2);
        
        Function.NamedExpressionList namedExpressionList2 = new Function.NamedExpressionList();
        namedExpressionList2.setExpressions(expressions2);
        sumFunction.setParameters(namedExpressionList2);
        
        SelectExpressionItem item2 = new SelectExpressionItem();
        item2.setExpression(sumFunction);
        selectItems.add(item2);
        
        // Execute aggregation
        AggregateOperation aggregateOp = new AggregateOperation(selectItems, null);
        Table result = aggregateOp.execute(testTable);
        
        // Verify result
        assertNotNull(result);
        assertEquals(1, result.getTuples().size());
        assertEquals("3|60", result.getTuples().get(0)); // Count of 3, Sum of 60
    }
    
    @Test
    public void testExecuteWithEmptyTable() throws IOException {
        // Create empty table
        Table emptyTable = new Table("empty_test", 3, new File(testDir, "empty.tbl"), testDir);
        emptyTable.setTuples(new ArrayList<>());
        emptyTable.getColumnIndexMap().put("col1", 0);
        emptyTable.getColumnIndexMap().put("col2", 1);
        emptyTable.getColumnIndexMap().put("col3", 2);
        
        // Create sum operation
        List<SelectExpressionItem> selectItems = new ArrayList<>();
        
        // Create SUM function
        Function sumFunction = new Function();
        sumFunction.setName("SUM");
        
        Column col = new Column();
        col.setColumnName("col2");
        
        List<Expression> expressions = new ArrayList<>();
        expressions.add(col);
        
        Function.NamedExpressionList namedExpressionList = new Function.NamedExpressionList();
        namedExpressionList.setExpressions(expressions);
        sumFunction.setParameters(namedExpressionList);
        
        SelectExpressionItem item = new SelectExpressionItem();
        item.setExpression(sumFunction);
        selectItems.add(item);
        
        // Execute aggregation
        AggregateOperation aggregateOp = new AggregateOperation(selectItems, null);
        Table result = aggregateOp.execute(emptyTable);
        
        // Verify result
        assertNotNull(result);
        assertEquals(1, result.getTuples().size());
        assertEquals("0", result.getTuples().get(0)); // Sum of empty set is 0
    }
    
    @After
    public void tearDown() {
        // Clean up if needed
    }
}