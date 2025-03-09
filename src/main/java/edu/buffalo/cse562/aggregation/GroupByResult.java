package edu.buffalo.cse562.aggregation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class representing the result of a GROUP BY operation with aggregation.
 */
public final class GroupByResult {
    private final List<String> groupByColumns;
    private final Map<GroupByKey, List<AggregateResult>> results;
    
    private GroupByResult(List<String> groupByColumns, Map<GroupByKey, List<AggregateResult>> results) {
        this.groupByColumns = List.copyOf(groupByColumns);
        this.results = new LinkedHashMap<>(results);
    }
    
    /**
     * Creates a new GroupByResult.
     */
    public static GroupByResult of(List<String> groupByColumns, Map<GroupByKey, List<AggregateResult>> results) {
        return new GroupByResult(groupByColumns, results);
    }
    
    /**
     * Gets the column names used for grouping.
     */
    public List<String> getGroupByColumns() {
        return groupByColumns;
    }
    
    /**
     * Gets all group by keys and their aggregate results.
     */
    public Map<GroupByKey, List<AggregateResult>> getResults() {
        return results;
    }
    
    /**
     * Gets aggregate results for a specific group.
     */
    public List<AggregateResult> getResultsForGroup(GroupByKey key) {
        return results.get(key);
    }
    
    /**
     * Gets the number of groups.
     */
    public int size() {
        return results.size();
    }
    
    /**
     * Gets whether there are any results.
     */
    public boolean isEmpty() {
        return results.isEmpty();
    }
}