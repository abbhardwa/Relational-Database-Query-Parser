package edu.buffalo.cse562.aggregation;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * Immutable class representing the result of an aggregate operation.
 */
public final class AggregateResult {
    private final AggregateFunction function;
    private final BigDecimal numericResult;
    private final Long count;
    
    private AggregateResult(AggregateFunction function, BigDecimal numericResult, Long count) {
        this.function = function;
        this.numericResult = numericResult;
        this.count = count;
    }
    
    /**
     * Creates a new numeric result (for SUM, MIN, MAX, AVG).
     */
    public static AggregateResult of(AggregateFunction function, BigDecimal value) {
        return new AggregateResult(function, value, null);
    }
    
    /**
     * Creates a new count result.
     */
    public static AggregateResult ofCount(long count) {
        return new AggregateResult(AggregateFunction.COUNT, null, count);
    }
    
    /**
     * Gets the aggregate function that produced this result.
     */
    public AggregateFunction getFunction() {
        return function;
    }
    
    /**
     * Gets the numeric result if available.
     */
    public Optional<BigDecimal> getNumericResult() {
        return Optional.ofNullable(numericResult);
    }
    
    /**
     * Gets the count if this is a COUNT result.
     */
    public Optional<Long> getCount() {
        return Optional.ofNullable(count);
    }
    
    /**
     * Gets the result formatted as a string.
     */
    public String format() {
        if (count != null) {
            return count.toString();
        }
        return numericResult.toPlainString();
    }
    
    @Override
    public String toString() {
        return String.format("%s: %s", function, format());
    }
}