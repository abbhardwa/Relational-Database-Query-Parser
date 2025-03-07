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
    public void testGetAllTableNamesWithSubselect() throws JSQLParserException {
        String sql = "SELECT * FROM (SELECT * FROM customers) AS c";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        List<String> tableNames = QueryUtils.getAllTableNames(plainSelect);
        assertTrue(tableNames.contains("customers"));
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
    public void testHasAggregatesWithAggregates() throws JSQLParserException {
        String sql = "SELECT COUNT(*), SUM(age) FROM customers";
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
    public void testHasAggregatesWithMixedItems() throws JSQLParserException {
        String sql = "SELECT name, COUNT(*), age, MAX(salary) FROM customers";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        
        @SuppressWarnings("unchecked")
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        assertTrue(QueryUtils.hasAggregates(selectItems));
    }
}