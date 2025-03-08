package edu.buffalo.cse562.aggregation;

import edu.buffalo.cse562.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility class for converting between new aggregate results and legacy table format.
 */
public class LegacyConverter {
    /**
     * Converts a GroupByResult into a legacy Table format.
     */
    public static Table toLegacyTable(GroupByResult result, String tableName) {
        List<String> tuples = new ArrayList<>();
        Table legacyTable = new Table(tableName);
        
        // Create column definitions
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        
        // Add group by columns
        for (String groupByCol : result.getGroupByColumns()) {
            ColumnDefinition colDef = new ColumnDefinition();
            colDef.setColumnName(groupByCol);
            ColDataType dataType = new ColDataType();
            dataType.setDataType("VARCHAR");
            colDef.setColDataType(dataType);
            columnDefs.add(colDef);
        }
        
        // Get a sample result to determine aggregate column definitions
        if (!result.isEmpty()) {
            GroupByKey firstKey = result.getResults().keySet().iterator().next();
            List<AggregateResult> firstResults = result.getResultsForGroup(firstKey);
            
            for (AggregateResult aggResult : firstResults) {
                ColumnDefinition colDef = new ColumnDefinition();
                ColDataType dataType = new ColDataType();
                
                if (aggResult.getCount().isPresent()) {
                    dataType.setDataType("BIGINT");
                } else {
                    dataType.setDataType("DECIMAL");
                }
                
                colDef.setColumnName(aggResult.getFunction().name().toLowerCase());
                colDef.setColDataType(dataType);
                columnDefs.add(colDef);
            }
        }
        
        legacyTable.setColumnDefinitions(columnDefs);
        legacyTable.populateColumnIndexMap();
        
        // Convert each group's results to a tuple
        for (Map.Entry<GroupByKey, List<AggregateResult>> entry : result.getResults().entrySet()) {
            List<String> values = new ArrayList<>();
            
            // Add group by values
            values.addAll(entry.getKey().getValues());
            
            // Add aggregate results
            values.addAll(entry.getValue().stream()
                .map(AggregateResult::format)
                .collect(Collectors.toList()));
            
            // Join with pipe separator
            tuples.add(String.join("|", values));
        }
        
        legacyTable.tableTuples = tuples;
        return legacyTable;
    }
}