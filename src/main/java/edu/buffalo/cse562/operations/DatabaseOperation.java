package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import java.io.IOException;

/**
 * Interface defining operations that can be performed on database tables.
 */
public interface DatabaseOperation {
    /**
     * Executes the database operation and returns the resulting table.
     *
     * @param input The input table(s) for the operation
     * @return The resulting table after performing the operation
     * @throws IOException If an I/O error occurs during operation
     */
    Table execute(Table... input) throws IOException;
}