package edu.buffalo.cse562.aggregation;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable class representing a composite key for GROUP BY operations.
 */
public final class GroupByKey {
    private final List<String> values;
    
    private GroupByKey(List<String> values) {
        this.values = values;
    }
    
    /**
     * Creates a new GroupByKey from a list of values.
     */
    public static GroupByKey of(List<String> values) {
        return new GroupByKey(List.copyOf(values));
    }
    
    /**
     * Creates a new GroupByKey from variable arguments.
     */
    public static GroupByKey of(String... values) {
        return new GroupByKey(Arrays.asList(values));
    }
    
    /**
     * Gets the grouped values.
     */
    public List<String> getValues() {
        return values;
    }
    
    /**
     * Gets a specific value by index.
     */
    public String getValue(int index) {
        return values.get(index);
    }
    
    /**
     * Gets the number of values in the key.
     */
    public int size() {
        return values.size();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupByKey that = (GroupByKey) o;
        return Objects.equals(values, that.values);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(values);
    }
    
    @Override
    public String toString() {
        return String.join("|", values);
    }
}