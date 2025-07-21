package edu.buffalo.cse562.util;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import static org.junit.Assert.*;

public class QueryUtilsTest {
    
    @Test
    public void testGetAllTableNamesSimpleQuery() throws JSQLParserException {
        String sql = "SELECT * FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertEquals(1, tableNames.size());
        assertEquals("customers", tableNames.get(0));
    }

    @Test
    public void testGetAllTableNamesWithJoin() throws JSQLParserException {
        String sql = "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains("customers"));
        assertTrue(tableNames.contains("orders"));
    }

    @Test
    public void testGetAllTableNamesWithMultipleJoins() throws JSQLParserException {
        String sql = "SELECT * FROM customers c " +
                    "JOIN orders o ON c.id = o.customer_id " +
                    "JOIN products p ON o.product_id = p.id";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertEquals(3, tableNames.size());
        assertTrue(tableNames.contains("customers"));
        assertTrue(tableNames.contains("orders"));
        assertTrue(tableNames.contains("products"));
    }

    @Test
    public void testGetAllTableNamesWithSubselect() throws JSQLParserException {
        String sql = "SELECT * FROM (SELECT * FROM customers) AS c";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertTrue(tableNames.contains("customers"));
    }

    @Test
    public void testGetAllTableNamesWithTableAlias() throws JSQLParserException {
        String sql = "SELECT * FROM customers AS c";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertEquals(1, tableNames.size());
        assertEquals("customers", tableNames.get(0));
    }

    @Test
    public void testGetAllTableNamesWithLeftJoin() throws JSQLParserException {
        String sql = "SELECT * FROM customers c LEFT JOIN orders o ON c.id = o.customer_id";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains("customers"));
        assertTrue(tableNames.contains("orders"));
    }

    @Test
    public void testGetAllTableNamesWithRightJoin() throws JSQLParserException {
        String sql = "SELECT * FROM customers c RIGHT JOIN orders o ON c.id = o.customer_id";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertEquals(2, tableNames.size());
        assertTrue(tableNames.contains("customers"));
        assertTrue(tableNames.contains("orders"));
    }

    @Test
    public void testGetAllTableNamesEmptyJoins() throws JSQLParserException {
        String sql = "SELECT name FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertEquals(1, tableNames.size());
        assertEquals("customers", tableNames.get(0));
    }

    @Test
    public void testHasAggregatesWithoutAggregates() throws JSQLParserException {
        String sql = "SELECT name, age FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertFalse(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithCount() throws JSQLParserException {
        String sql = "SELECT COUNT(*) FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithSum() throws JSQLParserException {
        String sql = "SELECT SUM(age) FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithAvg() throws JSQLParserException {
        String sql = "SELECT AVG(salary) FROM employees";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithMin() throws JSQLParserException {
        String sql = "SELECT MIN(price) FROM products";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithMax() throws JSQLParserException {
        String sql = "SELECT MAX(score) FROM results";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithMultipleAggregates() throws JSQLParserException {
        String sql = "SELECT COUNT(*), SUM(age), AVG(salary), MIN(age), MAX(salary) FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithNullList() {
        assertFalse(QueryUtils.hasAggregates(null));
    }

    @Test
    public void testHasAggregatesWithEmptyList() {
        List<SelectItem> emptyList = new ArrayList<>();
        assertFalse(QueryUtils.hasAggregates(emptyList));
    }

    @Test
    public void testHasAggregatesWithMixedItems() throws JSQLParserException {
        String sql = "SELECT name, COUNT(*), age, MAX(salary) FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesIgnoresCase() throws JSQLParserException {
        String sql = "SELECT COUNT(*), sum(age), AVG(salary) FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithNestedFunctions() throws JSQLParserException {
        String sql = "SELECT COUNT(DISTINCT customer_id) FROM orders";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }

    @Test 
    public void testHasAggregatesWithCalculatedFields() throws JSQLParserException {
        String sql = "SELECT name, age * 2 AS doubled_age FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertFalse(QueryUtils.hasAggregates(selectItems));
    }

    @Test
    public void testHasAggregatesWithStringContainingAggregateNames() throws JSQLParserException {
        String sql = "SELECT 'count value' AS description FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        // This should return false since 'count value' is a string literal, not a COUNT() function
        assertFalse(QueryUtils.hasAggregates(selectItems));
    }
}