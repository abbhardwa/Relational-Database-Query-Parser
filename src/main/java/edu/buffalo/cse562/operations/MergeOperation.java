package edu.buffalo.cse562.operations;

import edu.buffalo.cse562.model.Table;
import java.io.IOException;
import java.util.List;

/**
 * Interface defining merge operations during external sorting
 */
public interface MergeOperation extends DatabaseOperation {
    /**
     * Merge multiple sorted tables into a single sorted table
     *
     * @param tables list of tables to merge
     * @param orderByColumns columns to order by during merge
     * @return the merged table
     * @throws IOException if there are IO issues during merging
     */
    Table mergeTables(List<Table> tables, List orderByColumns) throws IOException;
}