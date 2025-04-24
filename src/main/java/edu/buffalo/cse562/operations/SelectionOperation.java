package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Implements selection operations on database tables.
 * Handles SELECT queries including joins, where clauses, and aggregations.
 */
public class SelectionOperation extends AbstractDatabaseOperation {
    private final Statement statement;

    /**
     * Constructs a new SelectionOperation.
     *
     * @param statement The SQL statement to execute
     * @param tableMap Map of table names to Table objects
     */
    public SelectionOperation(Statement statement, HashMap<String, Table> tableMap) {
        super(tableMap);
        this.statement = statement;
    }

    @Override
    public Table execute(Table... inputs) throws IOException {
        if (!(statement instanceof Select)) {
            throw new IllegalArgumentException("Statement must be a SELECT query");
        }

        SelectBody selectBody = ((Select) statement).getSelectBody();
        if (!(selectBody instanceof PlainSelect)) {
            throw new IllegalArgumentException("Only PlainSelect queries are supported");
        }

        PlainSelect plainSelect = (PlainSelect) selectBody;
        return processPlainSelect(plainSelect);
    }

    private Table processPlainSelect(PlainSelect plainSelect) throws IOException {
        // Get tables involved in the query
        List<Table> tablesToJoin = getTablesForJoin(plainSelect);
        
        // Process WHERE clause
        Expression whereClause = plainSelect.getWhere();
        
        // Process ORDER BY
        List orderByElements = plainSelect.getOrderByElements();
        
        // Process GROUP BY
        List groupByElements = plainSelect.getGroupByColumnReferences();
        
        // Process SELECT items
        List selectItems = plainSelect.getSelectItems();

        // Execute the query steps
        Table resultTable = processJoins(tablesToJoin, whereClause);
        resultTable = applyWherePredicate(resultTable, whereClause);
        resultTable = processGrouping(resultTable, groupByElements, selectItems);
        resultTable = processOrderBy(resultTable, orderByElements);
        
        return resultTable;
    }

    private List<Table> getTablesForJoin(PlainSelect plainSelect) {
        ArrayList<Table> tables = new ArrayList<>();
        
        // Add main FROM table
        FromItem fromItem = plainSelect.getFromItem();
        if (fromItem instanceof SubSelect) {
            tables.add(processSubSelect((SubSelect) fromItem));
        } else {
            tables.add(resolveTable(fromItem.toString()));
        }
        
        // Add JOIN tables
        if (plainSelect.getJoins() != null) {
            for (Object join : plainSelect.getJoins()) {
                tables.add(resolveTable(join.toString()));
            }
        }
        
        return tables;
    }

    private Table resolveTable(String tableName) {
        String[] parts = tableName.toLowerCase().split("\\s+");
        Table table = getTable(parts[0]);
        
        if (parts.length > 1 && parts[1].equals("as")) {
            Table aliasedTable = copyTable(table);
            aliasedTable.setTableName(parts[2]);
            registerTable(parts[2], aliasedTable);
            return aliasedTable;
        }
        
        return table;
    }

    private Table processSubSelect(SubSelect subSelect) {
        SelectBody selectBody = subSelect.getSelectBody();
        String alias = subSelect.getAlias();
        
        Select select = new Select();
        select.setSelectBody(selectBody);
        
        try {
            SelectionOperation subOperation = new SelectionOperation(select, tableMap);
            Table result = subOperation.execute();
            
            if (alias != null) {
                result.setTableName(alias);
                registerTable(alias, result);
            }
            
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Error processing subquery", e);
        }
    }

    private Table processJoins(List<Table> tables, Expression whereClause) {
        if (tables.size() == 1) {
            return tables.get(0);
        }
        
        JoinOperation joinOp = new JoinOperation(whereClause);
        try {
            return joinOp.execute(tables.toArray(new Table[0]));
        } catch (IOException e) {
            throw new RuntimeException("Error performing join operation", e);
        }
    }

    private Table applyWherePredicate(Table table, Expression whereClause) {
        if (whereClause == null) {
            return table;
        }
        
        WhereOperation whereOp = new WhereOperation(whereClause);
        try {
            return whereOp.execute(table);
        } catch (IOException e) {
            throw new RuntimeException("Error applying where predicate", e);
        }
    }

    private Table processGrouping(Table table, List groupByElements, List selectItems) {
        if (groupByElements == null && !hasAggregates(selectItems)) {
            return table;
        }
        
        GroupByOperation groupByOp = new GroupByOperation(groupByElements, selectItems);
        try {
            return groupByOp.execute(table);
        } catch (IOException e) {
            throw new RuntimeException("Error processing group by", e);
        }
    }

    private Table processOrderBy(Table table, List orderByElements) {
        if (orderByElements == null) {
            return table;
        }
        
        OrderByOperation orderByOp = new OrderByOperation(orderByElements);
        try {
            return orderByOp.execute(table);
        } catch (IOException e) {
            throw new RuntimeException("Error processing order by", e);
        }
    }

    private boolean hasAggregates(List selectItems) {
        if (selectItems == null) {
            return false;
        }
        
        for (Object item : selectItems) {
            String itemStr = item.toString().toLowerCase();
            if (itemStr.contains("sum(") || itemStr.contains("avg(") || 
                itemStr.contains("count(") || itemStr.contains("min(") || 
                itemStr.contains("max(")) {
                return true;
            }
        }
        
        return false;
    }
}