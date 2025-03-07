package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import java.util.List;
import static org.junit.Assert.*;

public class WhereOperationTest {
    
    private List<String[]> sampleData;
    private WhereOperation whereOp;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        sampleData = TestUtils.createSampleData(10);
        whereOp = new WhereOperation();
    }
    
    @After
    public void tearDown() {
        TestUtils.restoreStreams();
        TestUtils.clearOutput();
    }
    
    @Test
    public void testSimpleEquality() {
        // WHERE id = 5
        Column idCol = new Column();
        idCol.setColumnName("id");
        Expression condition = new EqualsTo();
        ((EqualsTo)condition).setLeftExpression(idCol);
        ((EqualsTo)condition).setRightExpression(new LongValue(5));
        
        List<String[]> result = whereOp.evaluate(sampleData, condition);
        
        assertFalse(result.isEmpty());
        for (String[] row : result) {
            assertEquals("5", row[0]);
        }
    }
    
    @Test
    public void testGreaterThan() {
        // WHERE value > 500
        Column valueCol = new Column();
        valueCol.setColumnName("value");
        Expression condition = new GreaterThan();
        ((GreaterThan)condition).setLeftExpression(valueCol);
        ((GreaterThan)condition).setRightExpression(new LongValue(500));
        
        List<String[]> result = whereOp.evaluate(sampleData, condition);
        
        for (String[] row : result) {
            assertTrue(Double.parseDouble(row[2]) > 500);
        }
    }
    
    @Test
    public void testLessThanOrEqual() {
        // WHERE value <= 300
        Column valueCol = new Column();
        valueCol.setColumnName("value");
        Expression condition = new MinorEquals();
        ((MinorEquals)condition).setLeftExpression(valueCol);
        ((MinorEquals)condition).setRightExpression(new LongValue(300));
        
        List<String[]> result = whereOp.evaluate(sampleData, condition);
        
        for (String[] row : result) {
            assertTrue(Double.parseDouble(row[2]) <= 300);
        }
    }
    
    @Test
    public void testStringLike() {
        // WHERE name LIKE '%5%'
        Column nameCol = new Column();
        nameCol.setColumnName("name");
        Expression condition = new LikeExpression();
        ((LikeExpression)condition).setLeftExpression(nameCol);
        ((LikeExpression)condition).setRightExpression(new StringValue("%5%"));
        
        List<String[]> result = whereOp.evaluate(sampleData, condition);
        
        for (String[] row : result) {
            assertTrue(row[1].contains("5"));
        }
    }
    
    @Test
    public void testAndCondition() {
        // WHERE value > 300 AND flag = true
        Column valueCol = new Column();
        valueCol.setColumnName("value");
        Column flagCol = new Column();
        flagCol.setColumnName("flag");
        
        Expression leftCond = new GreaterThan();
        ((GreaterThan)leftCond).setLeftExpression(valueCol);
        ((GreaterThan)leftCond).setRightExpression(new LongValue(300));
        
        Expression rightCond = new EqualsTo();
        ((EqualsTo)rightCond).setLeftExpression(flagCol);
        ((EqualsTo)rightCond).setRightExpression(new StringValue("true"));
        
        Expression condition = new AndExpression(leftCond, rightCond);
        
        List<String[]> result = whereOp.evaluate(sampleData, condition);
        
        for (String[] row : result) {
            assertTrue(Double.parseDouble(row[2]) > 300);
            assertTrue(Boolean.parseBoolean(row[3]));
        }
    }
    
    @Test
    public void testOrCondition() {
        // WHERE value > 800 OR flag = false
        Column valueCol = new Column();
        valueCol.setColumnName("value");
        Column flagCol = new Column();
        flagCol.setColumnName("flag");
        
        Expression leftCond = new GreaterThan();
        ((GreaterThan)leftCond).setLeftExpression(valueCol);
        ((GreaterThan)leftCond).setRightExpression(new LongValue(800));
        
        Expression rightCond = new EqualsTo();
        ((EqualsTo)rightCond).setLeftExpression(flagCol);
        ((EqualsTo)rightCond).setRightExpression(new StringValue("false"));
        
        Expression condition = new OrExpression(leftCond, rightCond);
        
        List<String[]> result = whereOp.evaluate(sampleData, condition);
        
        for (String[] row : result) {
            assertTrue(Double.parseDouble(row[2]) > 800 || !Boolean.parseBoolean(row[3]));
        }
    }
    
    @Test
    public void testArithmeticComparison() {
        // WHERE value * 2 > 500
        Column valueCol = new Column();
        valueCol.setColumnName("value");
        
        Expression multiply = new Multiplication();
        ((Multiplication)multiply).setLeftExpression(valueCol);
        ((Multiplication)multiply).setRightExpression(new LongValue(2));
        
        Expression condition = new GreaterThan();
        ((GreaterThan)condition).setLeftExpression(multiply);
        ((GreaterThan)condition).setRightExpression(new LongValue(500));
        
        List<String[]> result = whereOp.evaluate(sampleData, condition);
        
        for (String[] row : result) {
            assertTrue(Double.parseDouble(row[2]) * 2 > 500);
        }
    }
    
    @Test
    public void testEmptyResult() {
        // WHERE value > 1000 (should return no rows)
        Column valueCol = new Column();
        valueCol.setColumnName("value");
        Expression condition = new GreaterThan();
        ((GreaterThan)condition).setLeftExpression(valueCol);
        ((GreaterThan)condition).setRightExpression(new LongValue(1000));
        
        List<String[]> result = whereOp.evaluate(sampleData, condition);
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testNullCondition() {
        // No condition should return all rows
        List<String[]> result = whereOp.evaluate(sampleData, null);
        assertEquals(sampleData.size(), result.size());
        assertArrayEquals(sampleData.toArray(), result.toArray());
    }
    
    @Test
    public void testEmptyInput() {
        Column valueCol = new Column();
        valueCol.setColumnName("value");
        Expression condition = new GreaterThan();
        ((GreaterThan)condition).setLeftExpression(valueCol);
        ((GreaterThan)condition).setRightExpression(new LongValue(0));
        
        List<String[]> result = whereOp.evaluate(new ArrayList<>(), condition);
        assertTrue(result.isEmpty());
    }
}