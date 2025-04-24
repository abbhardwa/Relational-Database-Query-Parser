package edu.buffalo.cse562;

import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map.Entry;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.util.CompatibilityHelper;
import edu.buffalo.cse562.util.LegacyOperationBridge;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

/**
 * Legacy selection operation implementation.
 * This class is maintained for backward compatibility.
 */
public class SelectionOperation {
    
    /**
     * This function is used for the evaluation of the select statement.
     * 
     * @param statementObject SQL statement to evaluate
     * @param tableObjectsMap Map of table names to Table objects
     * @return Resulting table after selection
     * @throws IOException If an I/O error occurs
     * @throws ParseException If parsing fails
     * @throws InterruptedException If operation is interrupted
     */
    @SuppressWarnings("rawtypes")
    public static Table selectionEvaluation(Statement statementObject, HashMap<String, Table> tableObjectsMap)
    throws IOException, ParseException, InterruptedException {

        // First, try to use the new modular structure if possible
        try {
            edu.buffalo.cse562.operations.SelectionOperation operation = 
                new edu.buffalo.cse562.operations.SelectionOperation(statementObject, tableObjectsMap);
            return operation.execute();
        } catch (Exception e) {
            // If the new implementation fails, fall back to the original code
            System.err.println("Warning: Using legacy selection operation: " + e.getMessage());
        }

        // this is the SelectBody object corresponding to the statement object
        SelectBody selectBody = ((Select) statementObject).getSelectBody();
        
        // extract the list of "ORDER BY" elements from the plain select statement
        @SuppressWarnings({ "unused" })
        List orderbyElementsList = ((PlainSelect) selectBody).getOrderByElements();
        
        // extract the list of "GROUP BY" elements from the plain select statement
        List groupbyElementsList = ((PlainSelect) selectBody).getGroupByColumnReferences();
        
        // this is the where clause for the select statement
        Expression whereExpression = ((PlainSelect) selectBody).getWhere();
        
        // this is a list of table's that need to be joined
        ArrayList<String> listOfTables = new ArrayList<String>();

        if (((PlainSelect) selectBody) != null){

            // this from item can contain a sub query, then evaluate the sub query
            if(((PlainSelect) selectBody).getFromItem().toString().contains("SELECT ") || ((PlainSelect) selectBody).getFromItem().toString().contains("select ")) {
                
                // this statement gets us the select body
                FromItem fromItem = ((PlainSelect) selectBody).getFromItem();
            
                // extract the select body out of the fromItem
                SelectBody sBody = ((SubSelect)fromItem).getSelectBody();
                
                // extract alias of the new table if any, this should be there in most cases
                String alias = ((SubSelect)fromItem).getAlias();
                
                // now we make a select statement using the select body that we extracted, this is done because our selection function accepts a statement as a parameter
                Select selectStatement = new Select();

                // set the body of the select statement
                selectStatement.setSelectBody(sBody);
                
                // get the table object evaluated through the sub query
                Table subQueryTable = selectionEvaluation((Statement)selectStatement, tableObjectsMap);
                
                // set the name of the new table obtained to be the alias that was extracted or if there is no alias then set the name as "subQueryTable"
                if(alias == null){
                    subQueryTable.setTableName("subQueryTable");
                    
                    // put the table in the tableObjects map, so that we can refer it later
                    tableObjectsMap.put("subQueryTable", subQueryTable);
                    
                } else{
                    subQueryTable.setTableName(alias);
                    
                    // put the table in the tableObjects map, so that we can refer it later
                    tableObjectsMap.put(alias, subQueryTable);
                }
                
                // add the table name string to the list of tables to join
                listOfTables.add(alias);
                
            } else
                listOfTables.add(((PlainSelect) selectBody).getFromItem().toString().toLowerCase());
        }

        if (((PlainSelect) selectBody).getJoins() != null) {
            
            // get the list of joins in the from clause
            List joinList = ((PlainSelect) selectBody).getJoins();
            
            /* handle the case for sub-query in further projects just like above */
            for (Object tableToJoin : joinList)
                listOfTables.add(tableToJoin.toString().toLowerCase());
        }
        
        // this is the list of Table objects to join, we need to construct this list from the list of joins formed above
        ArrayList<Table> tablesToJoin = new ArrayList<Table>();

        for (Object tableToJoin : listOfTables) {

            // this array is used to get any alias for the table if used
            String[] tableAliasFilterArray = tableToJoin.toString().split(" ");
        
            // get the table object being referenced here
            Table tableObjectReferenced = tableObjectsMap.get(tableAliasFilterArray[0]);
            
            // if the table in the from clause has an alias
            if (tableAliasFilterArray.length > 1) {
        
                // construct a new table object
                Table newTable = new Table(tableObjectReferenced);
                
                // change the name of the table
                newTable.setTableName(tableAliasFilterArray[2].toLowerCase());
                
                // put the Table object in the map
                tableObjectsMap.put(newTable.getTableName(), newTable);
                
                // add the new table to tablesToJoin list
                tablesToJoin.add(newTable);
            } else
                tablesToJoin.add(tableObjectsMap.get(tableAliasFilterArray[0]));
        }
        
        // this Table variable stores the resultant table obtained from joining the tables in the from clause
        Table resultTable = null;
        
        // if the number of tables to join is just 1, then we directly pass in the where expression to the selection operation
        if (tablesToJoin.size() == 1) {
            
            // in the case when I just have a single table with me to join, I just need to filter that table and that will be my resultant table
            resultTable = LegacyOperationBridge.selectionOnTable(whereExpression, tablesToJoin.get(0));
            // after evaluating the resultant table that satisfies the where clause apply aggregate operations on the table
        }

        // now we scan the final list of tables to be joined and change their column description list and column index maps
        if (tablesToJoin.size() > 1) {
            
            // iterate over the table objects and change their columnDescriptions and columnIndexMap
            for (Table table : tablesToJoin) {

                // make a new array list of column definition objects for the new table
                ArrayList<ColumnDefinition> colDefinitionList = new ArrayList<ColumnDefinition>();
                for (ColumnDefinition cd : table.getColumnDefinitions()) {
                    ColumnDefinition temp = new ColumnDefinition();
                    temp.setColumnName(table.getTableName() + "." + cd.getColumnName());
                    temp.setColDataType(cd.getColDataType());
                    colDefinitionList.add(temp);
                }

                // set the new columnDefinitionList
                table.setColumnDefinitions(colDefinitionList);

                // make a new column index map
                HashMap<String, Integer> colIndexMap = new HashMap<String, Integer>();
                
                for (Entry<String, Integer> etr : table.getColumnIndexMap().entrySet()) {
                    colIndexMap.put(table.getTableName() + "." + etr.getKey(), etr.getValue());
                }

                // set the new column index map - use reflection to maintain backward compatibility
                try {
                    java.lang.reflect.Field field = Table.class.getDeclaredField("columnIndexMap");
                    field.setAccessible(true);
                    field.set(table, colIndexMap);
                } catch (Exception e) {
                    System.err.println("Warning: Could not set column index map: " + e.getMessage());
                }
            }

            // Use the join operation from our new modular structure
            try {
                edu.buffalo.cse562.operations.JoinOperation joinOp = 
                    new edu.buffalo.cse562.operations.JoinOperation(whereExpression);
                resultTable = joinOp.execute(tablesToJoin.toArray(new Table[0]));
            } catch (Exception e) {
                System.err.println("Error in join operation: " + e.getMessage());
                e.printStackTrace();
                
                // Fall back to original join implementation if needed
                Table t1 = null;
                Table t2 = null;
                int countOfJoins = 0;
                int index = tablesToJoin.size() - 1;
                HashMap<Integer, Table> mapOfTables = new HashMap<>();
                
                int i = 0;
                for (Table table : tablesToJoin) {
                    mapOfTables.put(i, table);
                    ++i;
                }
                
                if (index > 0) {
                    while (countOfJoins != index) {
                        for (int iterativeIndex = index - 1; iterativeIndex >= 0; iterativeIndex--) {
                            t1 = mapOfTables.get(index);
                            t2 = mapOfTables.get(iterativeIndex);
                            if (t2 == null) {
                                continue;
                            }
                            
                            ArrayList<String> arrayList = LegacyOperationBridge.evaluateJoinCondition(t1, t2, whereExpression);
                            
                            if (arrayList.size() > 0 && mapOfTables.get(iterativeIndex) != null) {
                                resultTable = HashJoin.evaluateJoin(t1, t2, arrayList.get(0), arrayList.get(1), arrayList.get(2));
                                mapOfTables.put(iterativeIndex, null);
                                mapOfTables.put(index, resultTable);
                                countOfJoins++;
                            }
                        }
                    }
                }
                
                // check if the tpch7 condition for the sub-query is not null and apply it to the table
                if (whereExpression instanceof Parenthesis) {
                    Expression expression = ((Parenthesis) whereExpression).getExpression();
                    resultTable = LegacyOperationBridge.selectionOnTable(expression, resultTable);
                }
            }
        }
        
        // the following is the list of select items, that is the items that need to be computed and stored in the resulting table
        List selectItemList = ((PlainSelect)selectBody).getSelectItems();
        // this is an arrayList of select item strings
        ArrayList<String> selectList = new ArrayList<String>();
        // form the above selectList from the selectItemList
        for(Object column : selectItemList){
            selectList.add(column.toString());
        }
        
        // this variable is used to check if there is an aggregate function present in the select list
        boolean aggregatePresent = false;
        // scan the selectItem list and check if there is an aggregate present or not
        for(String selectItem : selectList){
            if(selectItem.contains("sum(") || selectItem.contains("SUM(") || selectItem.contains("avg(") 
               || selectItem.contains("AVG(") || selectItem.contains("count(") || selectItem.contains("COUNT(")
               || selectItem.contains("min(") || selectItem.contains("MIN(") || selectItem.contains("max(")
               || selectItem.contains("MAX(")){
                aggregatePresent = true;
                break;
            }
        }
        
        // if there are no group by columns present and there are no aggregates then just project the items
        if(groupbyElementsList == null && !aggregatePresent){
            return ProjectTableOperation.projectTable(resultTable, selectList, false);
        } else {
            // make the object of Aggregate class to call Aggregate functions
            AggregateOperations aggrObject = new AggregateOperations();
            // this string is a comma separated string of GroupBy items
            String groupItems;
                
            if(groupbyElementsList == null)
                groupItems = "NOGroupBy";
            else
                groupItems = groupbyElementsList.toString();
                
            // we store the select items list in an array
            String[] selectItemsArray = ((PlainSelect)selectBody).getSelectItems().toString().replaceAll("\\[", "").replaceAll("\\]", "").trim().split(",");
                
            // call the aggregate function to get the resultant table
            resultTable = aggrObject.getAggregate(resultTable, selectItemsArray, groupItems, orderbyElementsList);
        }
        
        // return the resultant table after performing all selections
        return resultTable;
    }
}
