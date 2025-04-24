package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import java.io.IOException;

/**
 * Interface defining operations that can be performed on database tables.
 * All database operations should implement this interface to provide a 
 * consistent execution model.
 */
public interface DatabaseOperation {
    /**
     * Executes the database operation and returns the resulting table.
     *
     * @param inputs The input table(s) for the operation
     * @return The resulting table after performing the operation
     * @throws IOException If an I/O error occurs during operation
     */
    Table execute(Table... inputs) throws IOException;
    
    /**
     * Gets a descriptive name for this operation.
     * Useful for debugging and logging.
     * 
     * @return The operation name
     */
    default String getOperationName() {
        return this.getClass().getSimpleName();
    }
}