package edu.buffalo.cse562;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.*;
import java.util.*;
import java.text.SimpleDateFormat;
import static org.junit.Assert.*;

public class IntegrationTest {
    
    private List<String[]> ordersTable;
    private List<String[]> customersTable;
    private List<String[]> productsTable;
    private List<String[]> regionsTable;
    private SimpleDateFormat dateFormat;
    
    @Before
    public void setUp() {
        TestUtils.setUpStreams();
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        
        // Create Orders table
        ordersTable = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ordersTable.add(new String[] {
                String.valueOf(i),               // order_id
                String.valueOf(i % 20),          // customer_id
                String.valueOf(i * 10.5),        // amount
                "2023-01-" + String.format("%02d", (i % 30) + 1), // order_date
                String.valueOf(i % 10),          // product_id
                String.valueOf((i % 5) + 1)      // quantity
            });
        }
        
        // Create Customers table
        customersTable = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            customersTable.add(new String[] {
                String.valueOf(i),              // customer_id
                "Customer" + i,                 // name
                String.valueOf(1000 + i * 100), // credit_limit
                String.valueOf(i % 5),          // region_id
                i % 2 == 0 ? "true" : "false"  // active
            });
        }
        
        // Create Products table
        productsTable = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            productsTable.add(new String[] {
                String.valueOf(i),              // product_id
                "Product" + i,                  // name
                String.valueOf(50 + i * 25),    // price
                String.valueOf(100 * (i + 1)),  // stock
                String.valueOf(i % 3)           // category_id
            });
        }
        
        // Create Regions table
        regionsTable = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            regionsTable.add(new String[] {
                String.valueOf(i),    // region_id
                "Region" + i,         // region_name
                "Continent" + (i % 3) // continent
            });
        }
    }
    
    @After
    public void tearDown() {
        TestUtils.restoreStreams();
        TestUtils.clearOutput();
    }
    
    @Test
    public void testComplexCustomerAnalysis() {
        // Complex query to analyze customer behavior:
        // SELECT 
        //   c.name, r.region_name, 
        //   COUNT(DISTINCT o.order_id) as order_count,
        //   SUM(o.amount) as total_amount,
        //   AVG(o.amount) as avg_amount
        // FROM orders o
        // JOIN customers c ON o.customer_id = c.customer_id
        // JOIN regions r ON c.region_id = r.region_id
        // WHERE c.active = true AND o.amount > 100
        // GROUP BY c.name, r.region_name
        // HAVING COUNT(o.order_id) > 2
        // ORDER BY total_amount DESC
        
        // 1. Filter active customers and high-value orders
        WhereOperation whereOp = new WhereOperation();
        Expression condition = createCompoundCondition();
        List<String[]> filteredOrders = whereOp.evaluate(ordersTable, condition);
        
        // 2. Join with customers
        HashJoin joiner = new HashJoin();
        List<String[]> customerJoin = joiner.join(filteredOrders, customersTable, 1, 0);
        
        // 3. Join with regions
        List<String[]> allJoined = joiner.join(customerJoin, regionsTable, 8, 0);
        
        // 4. Project required columns
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] projectColumns = {6, 11, 0, 2}; // customer name, region name, order id, amount
        List<String[]> projected = projector.project(allJoined, projectColumns);
        
        // 5. Group by customer name and region
        AggregateOperations aggregator = new AggregateOperations();
        List<String[]> result = aggregator.groupByAndAggregate(
            projected,
            new int[]{0, 1}, // group by customer name and region
            new int[]{2, 3, 3}, // aggregate order_id and amount twice
            new String[]{"COUNT_DISTINCT", "SUM", "AVG"}
        );
        
        // 6. Filter groups with enough orders
        whereOp = new WhereOperation();
        condition = new GreaterThan();
        ((GreaterThan)condition).setLeftExpression(new Column().withColumnName("order_count"));
        ((GreaterThan)condition).setRightExpression(new LongValue(2));
        List<String[]> havingFiltered = whereOp.evaluate(result, condition);
        
        // 7. Order by total amount
        OrderByOperation orderer = new OrderByOperation();
        result = orderer.orderBy(havingFiltered, new int[]{3}, new boolean[]{false});
        
        // Verify results
        assertFalse(result.isEmpty());
        verifyAnalysisResults(result);
    }
    
    @Test
    public void testProductPerformanceAnalysis() {
        // Analyze product performance across regions:
        // SELECT 
        //   p.name, r.continent,
        //   SUM(o.quantity) as total_quantity,
        //   SUM(o.quantity * p.price) as total_revenue,
        //   COUNT(DISTINCT c.customer_id) as customer_count
        // FROM orders o
        // JOIN products p ON o.product_id = p.product_id
        // JOIN customers c ON o.customer_id = c.customer_id
        // JOIN regions r ON c.region_id = r.region_id
        // GROUP BY p.name, r.continent
        // HAVING total_revenue > 1000
        // ORDER BY total_revenue DESC
        
        // 1. Join orders with products
        HashJoin joiner = new HashJoin();
        List<String[]> productJoin = joiner.join(ordersTable, productsTable, 4, 0);
        
        // 2. Join with customers
        List<String[]> customerJoin = joiner.join(productJoin, customersTable, 1, 0);
        
        // 3. Join with regions
        List<String[]> allJoined = joiner.join(customerJoin, regionsTable, 13, 0);
        
        // 4. Project and calculate revenue
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] projectColumns = {7, 16, 5, 1, 9}; // product name, continent, quantity, price, customer_id
        List<String[]> projected = projector.project(allJoined, projectColumns);
        
        // 5. Group and aggregate
        AggregateOperations aggregator = new AggregateOperations();
        List<String[]> result = aggregator.groupByAndAggregate(
            projected,
            new int[]{0, 1}, // group by product name and continent
            new int[]{2, 2, 4}, // aggregate quantity, revenue, customer_id
            new String[]{"SUM", "SUM_PRODUCT", "COUNT_DISTINCT"}
        );
        
        // 6. Filter by revenue threshold
        WhereOperation whereOp = new WhereOperation();
        Expression condition = new GreaterThan();
        ((GreaterThan)condition).setLeftExpression(new Column().withColumnName("revenue"));
        ((GreaterThan)condition).setRightExpression(new LongValue(1000));
        List<String[]> havingFiltered = whereOp.evaluate(result, condition);
        
        // 7. Sort by revenue
        OrderByOperation orderer = new OrderByOperation();
        result = orderer.orderBy(havingFiltered, new int[]{3}, new boolean[]{false});
        
        // Verify results
        assertFalse(result.isEmpty());
        verifyProductAnalysisResults(result);
    }
    
    @Test
    public void testTimeSeriesAnalysis() {
        // Analyze sales trends over time:
        // SELECT 
        //   DATE_TRUNC('month', o.order_date) as month,
        //   r.continent,
        //   COUNT(o.order_id) as order_count,
        //   SUM(o.amount) as total_sales,
        //   COUNT(DISTINCT c.customer_id) as active_customers
        // FROM orders o
        // JOIN customers c ON o.customer_id = c.customer_id
        // JOIN regions r ON c.region_id = r.region_id
        // GROUP BY month, r.continent
        // ORDER BY month ASC, total_sales DESC
        
        // 1. Join the tables
        HashJoin joiner = new HashJoin();
        List<String[]> customerJoin = joiner.join(ordersTable, customersTable, 1, 0);
        List<String[]> allJoined = joiner.join(customerJoin, regionsTable, 8, 0);
        
        // 2. Project needed columns
        ProjectTableOperation projector = new ProjectTableOperation();
        int[] projectColumns = {3, 11, 0, 2, 1}; // date, continent, order_id, amount, customer_id
        List<String[]> projected = projector.project(allJoined, projectColumns);
        
        // 3. Group by month and continent
        AggregateOperations aggregator = new AggregateOperations();
        List<String[]> result = aggregator.groupByAndAggregate(
            projected,
            new int[]{0, 1}, // group by month and continent
            new int[]{2, 3, 4}, // aggregate order_id, amount, customer_id
            new String[]{"COUNT", "SUM", "COUNT_DISTINCT"}
        );
        
        // 4. Order by month and sales
        OrderByOperation orderer = new OrderByOperation();
        int[] orderColumns = {0, 3}; // month asc, sales desc
        boolean[] ascending = {true, false};
        result = orderer.orderBy(result, orderColumns, ascending);
        
        // Verify results
        assertFalse(result.isEmpty());
        verifyTimeSeriesResults(result);
    }
    
    private Expression createCompoundCondition() {
        // Create condition: c.active = true AND o.amount > 100
        AndExpression and = new AndExpression();
        
        EqualsTo activeCondition = new EqualsTo();
        activeCondition.setLeftExpression(new Column().withColumnName("active"));
        activeCondition.setRightExpression(new StringValue("true"));
        
        GreaterThan amountCondition = new GreaterThan();
        amountCondition.setLeftExpression(new Column().withColumnName("amount"));
        amountCondition.setRightExpression(new LongValue(100));
        
        and.setLeftExpression(activeCondition);
        and.setRightExpression(amountCondition);
        
        return and;
    }
    
    private void verifyAnalysisResults(List<String[]> results) {
        for (int i = 0; i < results.size(); i++) {
            String[] row = results.get(i);
            
            // Check customer name format
            assertTrue(row[0].startsWith("Customer"));
            
            // Check region name format
            assertTrue(row[1].startsWith("Region"));
            
            // Check order count
            int orderCount = Integer.parseInt(row[2]);
            assertTrue(orderCount > 2); // HAVING clause check
            
            // Check amount values
            double totalAmount = Double.parseDouble(row[3]);
            double avgAmount = Double.parseDouble(row[4]);
            assertTrue(totalAmount > 0);
            assertTrue(avgAmount > 0);
            assertTrue(totalAmount >= avgAmount);
            
            // Check ordering
            if (i > 0) {
                double prevAmount = Double.parseDouble(results.get(i-1)[3]);
                assertTrue(prevAmount >= totalAmount); // DESC order check
            }
        }
    }
    
    private void verifyProductAnalysisResults(List<String[]> results) {
        for (int i = 0; i < results.size(); i++) {
            String[] row = results.get(i);
            
            // Check product name format
            assertTrue(row[0].startsWith("Product"));
            
            // Check continent format
            assertTrue(row[1].startsWith("Continent"));
            
            // Check quantity and revenue
            int totalQuantity = Integer.parseInt(row[2]);
            double totalRevenue = Double.parseDouble(row[3]);
            assertTrue(totalQuantity > 0);
            assertTrue(totalRevenue > 1000); // HAVING clause check
            
            // Check customer count
            int customerCount = Integer.parseInt(row[4]);
            assertTrue(customerCount > 0);
            assertTrue(customerCount <= 20); // Max possible customers
            
            // Check ordering
            if (i > 0) {
                double prevRevenue = Double.parseDouble(results.get(i-1)[3]);
                assertTrue(prevRevenue >= totalRevenue); // DESC order check
            }
        }
    }
    
    private void verifyTimeSeriesResults(List<String[]> results) {
        String prevMonth = null;
        Double prevSalesInMonth = null;
        
        for (String[] row : results) {
            // Check date format
            String month = row[0];
            assertTrue(month.matches("\\d{4}-\\d{2}-\\d{2}"));
            
            // Check continent format
            assertTrue(row[1].startsWith("Continent"));
            
            // Check metrics
            int orderCount = Integer.parseInt(row[2]);
            double totalSales = Double.parseDouble(row[3]);
            int customerCount = Integer.parseInt(row[4]);
            
            assertTrue(orderCount > 0);
            assertTrue(totalSales > 0);
            assertTrue(customerCount > 0);
            assertTrue(customerCount <= 20);
            
            // Check ordering
            if (prevMonth != null) {
                if (prevMonth.equals(month)) {
                    // Same month, check sales DESC
                    assertTrue(prevSalesInMonth >= totalSales);
                } else {
                    // New month, reset sales comparison
                    prevSalesInMonth = totalSales;
                }
            }
            prevMonth = month;
            prevSalesInMonth = totalSales;
        }
    }
}
