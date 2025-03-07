package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.LongValue;
import java.util.List;
import java.util.ArrayList;
import static org.junit.Assert.*;

public class IntegrationTest {
    
    private List<String[]> ordersTable;
    private List<String[]> customersTable;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        
        // Create Orders table
        ordersTable = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ordersTable.add(new String[] {
                String.valueOf(i),               // order_id
                String.valueOf(i % 20),          // customer_id
                String.valueOf(i * 10.5),        // amount
                "2023-01-" + String.format("%02d", (i % 30) + 1)  // order_date
            });
        }
        
        // Create Customers table
        customersTable = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            customersTable.add(new String[] {
                String.valueOf(i),              // customer_id
                "Customer" + i,                 // name
                String.valueOf(1000 + i * 100), // credit_limit
                String.valueOf(i % 5)           // region_id
            });
        }
    }
    
    @After
    public void tearDown() {
        TestUtils.restoreStreams();
        TestUtils.clearOutput();
    }
    
    @Test
    public void testComplexQuery() {
        // Test equivalent to SQL:
        // SELECT c.name, SUM(o.amount) as total_amount
        // FROM orders o
        // JOIN customers c ON o.customer_id = c.customer_id
        // WHERE o.amount > 500
        // GROUP BY c.name
        // HAVING total_amount > 1000
        // ORDER BY total_amount DESC
        
        // 1. Filter orders with amount > 500
        WhereOperation whereOp = new WhereOperation();
        Column amountCol = new Column();
        amountCol.setColumnName("amount");
        Expression condition = new GreaterThan();
        ((GreaterThan)condition).setLeftExpression(amountCol);
        ((GreaterThan)condition).setRightExpression(new LongValue(500));
        
        List<String[]> filteredOrders = whereOp.evaluate(ordersTable, condition);
        
        // 2. Join with customers
        HashJoin joiner = new HashJoin();
        List<String[]> joined = joiner.join(filteredOrders, customersTable, 1, 0); // Join on customer_id
        
        // 3. Project required columns (name and amount)
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] projectColumns = {5, 2}; // customer name and order amount
        List<String[]> projected = projector.project(joined, projectColumns);
        
        // 4. Group by customer name and aggregate amount
        AggregateOperations aggregator = new AggregateOperations();
        List<String[]> grouped = aggregator.groupByAndAggregate(
            projected,
            new int[]{0}, // group by name
            new int[]{1}, // aggregate amount
            new String[]{"SUM"} // sum the amounts
        );
        
        // 5. Filter groups with total_amount > 1000
        whereOp = new WhereOperation();
        Column totalAmountCol = new Column();
        totalAmountCol.setColumnName("total_amount");
        condition = new GreaterThan();
        ((GreaterThan)condition).setLeftExpression(totalAmountCol);
        ((GreaterThan)condition).setRightExpression(new LongValue(1000));
        
        List<String[]> havingFiltered = whereOp.evaluate(grouped, condition);
        
        // 6. Order by total_amount descending
        OrderByOperation orderer = new OrderByOperation();
        int[] orderColumns = {1}; // order by amount
        boolean[] ascending = {false}; // descending order
        List<String[]> result = orderer.orderBy(havingFiltered, orderColumns, ascending);
        
        // Verify results
        assertFalse(result.isEmpty());
        
        // Check ordering
        for (int i = 1; i < result.size(); i++) {
            double prev = Double.parseDouble(result.get(i-1)[1]);
            double curr = Double.parseDouble(result.get(i)[1]);
            assertTrue(prev >= curr);
            assertTrue(curr > 1000); // Verify HAVING clause
        }
    }
    
    @Test
    public void testMultipleJoinsAndAggregation() {
        // Create a Regions table for multiple joins
        List<String[]> regionsTable = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            regionsTable.add(new String[] {
                String.valueOf(i),    // region_id
                "Region" + i,         // region_name
                "Continent" + (i % 3) // continent
            });
        }
        
        // Test equivalent to SQL:
        // SELECT r.continent, COUNT(DISTINCT c.customer_id) as customer_count, SUM(o.amount) as total_amount
        // FROM orders o
        // JOIN customers c ON o.customer_id = c.customer_id
        // JOIN regions r ON c.region_id = r.region_id
        // GROUP BY r.continent
        // ORDER BY total_amount DESC
        
        // 1. Join orders and customers
        HashJoin joiner = new HashJoin();
        List<String[]> firstJoin = joiner.join(ordersTable, customersTable, 1, 0);
        
        // 2. Join with regions
        List<String[]> allJoined = joiner.join(firstJoin, regionsTable, 7, 0); // customer.region_id = regions.region_id
        
        // 3. Project needed columns (continent, customer_id, amount)
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] projectColumns = {9, 1, 2}; // continent, customer_id, amount
        List<String[]> projected = projector.project(allJoined, projectColumns);
        
        // 4. Group by continent and aggregate
        AggregateOperations aggregator = new AggregateOperations();
        List<String[]> result = aggregator.groupByAndAggregate(
            projected,
            new int[]{0},           // group by continent
            new int[]{1, 2},        // aggregate customer_id and amount
            new String[]{"COUNT_DISTINCT", "SUM"} // count distinct customers and sum amounts
        );
        
        // 5. Order by total_amount (sum) descending
        OrderByOperation orderer = new OrderByOperation();
        int[] orderColumns = {2}; // order by sum amount
        boolean[] ascending = {false};
        result = orderer.orderBy(result, orderColumns, ascending);
        
        // Verify results
        assertFalse(result.isEmpty());
        assertEquals(3, result.size()); // Should have 3 continents
        
        // Check ordering
        for (int i = 1; i < result.size(); i++) {
            double prev = Double.parseDouble(result.get(i-1)[2]);
            double curr = Double.parseDouble(result.get(i)[2]);
            assertTrue(prev >= curr);
        }
        
        // Verify customer counts are reasonable
        for (String[] row : result) {
            int customerCount = Integer.parseInt(row[1]);
            assertTrue(customerCount > 0 && customerCount <= 20); // Max 20 customers total
        }
    }
}