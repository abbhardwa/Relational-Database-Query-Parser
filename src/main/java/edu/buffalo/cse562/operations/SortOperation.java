package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import java.io.IOException;
import java.util.List;

/**
 * Interface defining sort operations on tables
 */
public interface SortOperation extends DatabaseOperation {
    /**
     * Sort the provided table according to the specified orderBy columns
     *
     * @param table the table to sort
     * @param orderByColumns the list of column definitions to sort by
     * @return the sorted table
     * @throws IOException if there are IO issues while sorting
     */
    Table sortTable(Table table, List orderByColumns) throws IOException;
}