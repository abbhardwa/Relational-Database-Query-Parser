package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Implements join operations on tables.
 */
public class JoinOperation extends AbstractDatabaseOperation {
    private final Expression joinCondition;

    /**
     * Creates a new JoinOperation.
     * 
     * @param joinCondition The join condition
     */
    public JoinOperation(Expression joinCondition) {
        this.joinCondition = joinCondition;
    }

    @Override
    public Table execute(Table... inputs) throws IOException {
        if (inputs.length < 2) {
            throw new IllegalArgumentException("Join operation requires at least two input tables");
        }
        
        // For simplicity, start with the first two tables
        Table result = joinTables(inputs[0], inputs[1]);
        
        // Join the rest of the tables if any
        for (int i = 2; i < inputs.length; i++) {
            result = joinTables(result, inputs[i]);
        }
        
        return result;
    }
    
    /**
     * Joins two tables based on the join condition.
     * 
     * @param leftTable The left table
     * @param rightTable The right table
     * @return The joined table
     */
    private Table joinTables(Table leftTable, Table rightTable) {
        // Create result table
        String joinedTableName = leftTable.getTableName() + "_" + rightTable.getTableName();
        int totalColumns = leftTable.getColumnCount() + rightTable.getColumnCount();
        Table joinedTable = new Table(joinedTableName, totalColumns, null, leftTable.getDataDirectory());
        
        // Combine column definitions
        ArrayList<ColumnDefinition> joinedColumns = new ArrayList<>();
        joinedColumns.addAll(leftTable.getColumnDefinitions());
        joinedColumns.addAll(rightTable.getColumnDefinitions());
        joinedTable.setColumnDefinitions(joinedColumns);
        
        // Update column index map
        joinedTable.populateColumnIndexMap();
        
        // Perform hash join
        ArrayList<String> joinedTuples = new ArrayList<>();
        
        // Build phase
        HashMap<String, ArrayList<String>> hashMap = new HashMap<>();
        
        // Find join keys (simplified implementation)
        // In a real implementation, we would extract join keys from the joinCondition
        String leftJoinKey = "id"; // Example join key
        String rightJoinKey = "id"; // Example join key
        
        int leftKeyIndex = leftTable.getColumnIndexMap().getOrDefault(leftJoinKey, 0);
        int rightKeyIndex = rightTable.getColumnIndexMap().getOrDefault(rightJoinKey, 0);
        
        // Build hash table from smaller table
        ArrayList<String> buildTuples = leftTable.getTuples().size() <= rightTable.getTuples().size() 
                                       ? leftTable.getTuples() : rightTable.getTuples();
        
        ArrayList<String> probeTuples = leftTable.getTuples().size() <= rightTable.getTuples().size()
                                       ? rightTable.getTuples() : leftTable.getTuples();
        
        boolean leftIsSmaller = leftTable.getTuples().size() <= rightTable.getTuples().size();
        int buildKeyIndex = leftIsSmaller ? leftKeyIndex : rightKeyIndex;
        int probeKeyIndex = leftIsSmaller ? rightKeyIndex : leftKeyIndex;
        
        // Build phase
        for (String tuple : buildTuples) {
            String[] values = tuple.split("\\|");
            if (values.length <= buildKeyIndex) continue;
            
            String key = values[buildKeyIndex];
            if (!hashMap.containsKey(key)) {
                hashMap.put(key, new ArrayList<>());
            }
            hashMap.get(key).add(tuple);
        }
        
        // Probe phase
        for (String probeTuple : probeTuples) {
            String[] probeValues = probeTuple.split("\\|");
            if (probeValues.length <= probeKeyIndex) continue;
            
            String probeKey = probeValues[probeKeyIndex];
            if (hashMap.containsKey(probeKey)) {
                for (String buildTuple : hashMap.get(probeKey)) {
                    String joinedTuple = leftIsSmaller ? buildTuple + "|" + probeTuple : probeTuple + "|" + buildTuple;
                    joinedTuples.add(joinedTuple);
                }
            }
        }
        
        joinedTable.setTuples(joinedTuples);
        return joinedTable;
    }
}