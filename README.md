# SQL Query Engine with TPC-H Support

This repository contains a custom SQL query processing engine implementation in Java along with TPC-H benchmark support tools. The project provides a framework for executing SQL queries over TPC-H benchmark data with various query optimization techniques including hash joins, aggregation operations, and indexing.

## System Architecture and Data Flow

The query engine follows a pipeline architecture where data flows through different operation classes for processing. Here's the high-level overview of how data flows through the system:

```
                                 ┌─────────────────┐
                                 │      Main       │
                                 │  (Entry Point)  │
                                 └────────┬────────┘
                                         │
                             ┌───────────▼──────────┐
                             │  QueryExecutionService│
                             │  (Query Orchestrator) │
                             └───────────┬──────────┘
                                        │
               ┌──────────────┬─────────┴───────────┬──────────────┐
               │              │                     │              │
     ┌─────────▼───────┐ ┌───▼────────┐    ┌──────▼─────┐  ┌────▼─────┐
     │  BuildIndexes   │ │   Table     │    │  Selection  │  │  Project  │
     │  (Optimization) │ │  Operations │    │  Operation  │  │   Table   │
     └─────────┬───────┘ └────┬───────┘    └──────┬─────┘  └────┬─────┘
               │              │                    │              │
               └──────────────┴──────────┬────────┴──────────────┘
                                        │
                            ┌───────────▼──────────┐
                            │   Processing Layer    │
                            │ (HashJoin/Aggregate/  │
                            │  OrderBy/HybridHash)  │
                            └────────────┬─────────┘
                                        │
                                ┌───────▼──────┐
                                │    Output    │
                                │   Results    │
                                └─────────────┘
```

### Key Component Interactions

1. **Query Flow**
   - Queries enter through `Main.java`
   - `QueryExecutionService` parses and creates execution plan
   - Operations are chained based on query requirements

2. **Data Processing Flow**
   - `Table.java` provides base data structure and access methods
   - `SelectionOperation` filters rows based on WHERE conditions
   - `ProjectTableOperation` extracts and transforms columns
   - Results flow into complex operations (joins/aggregations)

3. **Join Operations**
   - Input tables → `HashJoin`/`HybridHash`
   - Build phase creates hash tables
   - Probe phase matches records
   - Output flows to next operation

4. **Aggregation Pipeline**
   - Data streams into `AggregateOperations`
   - Groups formed using hash-based techniques
   - Aggregate functions applied
   - Results sorted if needed via `OrderByOperation`

5. **Index-Based Optimization**
   - `BuildIndexes` creates B-tree structures
   - Indexes integrated with selection operations
   - Optimized access paths for qualified queries

## Project Structure

```
.
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── edu/buffalo/cse562/        # Source code
│   │   │       ├── AggregateOperations.java
│   │   │       ├── BuildIndexes.java
│   │   │       ├── ExternalSort.java
│   │   │       ├── HashJoin.java
│   │   │       ├── HybridHash.java
│   │   │       ├── Main.java
│   │   │       ├── OrderByOperation.java
│   │   │       ├── ProjectTableOperation.java
│   │   │       ├── SelectionOperation.java
│   │   │       └── WhereOperation.java
│   │   └── resources/
│   │       └── queries/                    # TPCH queries
│   └── test/
│       └── java/                          # Test code (future)
├── tools/
│   └── dbgen/                             # TPCH data generation tool
│       ├── dbgen/                         # Data generation utilities
│       └── ref_data/                      # Reference data sets
├── pom.xml                                # Maven build configuration
└── README.md                              # This file

### Detailed Operation Flows

1. **Hash Join Flow**
```
Table1 ──┐                      ┌── Table2
         │                      │
   Build Phase              Probe Phase
         │                      │
   Hash Table 1 ←───┐    ┌────→Hash Table 2
         │         │    │       │
         └──→ HybridHash ←──────┘
                  │
                  ▼
         Matched Results
```

2. **Aggregate Operation Flow**
```
Input Records
      │
      ▼
Group Formation
      │
   ┌──┴──┐
   │     │
Hash    Sort
Tables  Groups
   │     │
   └──┬──┘
      │
Apply Functions
      │
      ▼
Final Results
```

3. **Index-Based Query Flow**
```
Query Predicate
      │
      ▼
Index Lookup
      │
   ┌──┴──┐
B-Tree   Hash
Index    Index
   │      │
   └──┬───┘
      │
 Fetch Records
      │
      ▼
Result Stream
```

## Key Components

1. **AggregateOperations**
   - Input: Raw table data or intermediate results
   - Processing: Groups data and applies aggregate functions
   - Output: Aggregated results to downstream operations
   - Data Flow: `Table` → `GroupBy` → `Aggregate` → `Results`

2. **BuildIndexes**
   - Input: Table data and index specifications
   - Processing: Creates optimized access structures
   - Output: Persistent B-tree/hash indexes
   - Data Flow: `Table` → `IndexBuilder` → `IndexStructure`

3. **ExternalSort**
   - Input: Unsorted data streams
   - Processing: Multi-way merge sort with disk support
   - Output: Sorted record stream
   - Data Flow: `Input` → `SortChunks` → `MergePhases` → `SortedOutput`

4. **HashJoin and HybridHash**
   - Input: Two tables to be joined
   - Processing: Build and probe hash tables
   - Output: Joined record pairs
   - Data Flow: `Tables` → `HashPhase` → `ProbePhase` → `JoinedResults`

5. **ProjectTableOperation**
   - Input: Raw table data
   - Processing: Column selection and transformation
   - Output: Projected record stream
   - Data Flow: `Table` → `ColumnSelector` → `ProjectedData`

6. **SelectionOperation**
   - Input: Table records
   - Processing: Applies WHERE clause filters
   - Output: Filtered record stream
   - Data Flow: `Table` → `Predicates` → `FilteredResults`

7. **OrderByOperation**
   - Input: Unordered record stream
   - Processing: Sorts based on specified columns
   - Output: Ordered results
   - Data Flow: `Input` → `Sort` → `OrderedOutput`

## Data Transformation Patterns

The system employs these common data transformation patterns:

1. **Pipeline Processing**
   ```
   Operation1 → Operation2 → Operation3 → Results
   ```
   Sequential processing where each operation's output feeds into the next.

2. **Fork-Join Pattern**
   ```
   Operation1 ──┐
                ├─→ Join → Results
   Operation2 ──┘
   ```
   Parallel operations merging results through joins.

3. **Aggregation Pattern**
   ```
   Records → Group → Transform → Combine → Results
   ```
   Progressive data reduction through grouping and aggregation:
   1. Records: Raw source data rows from table scans or previous operations
   2. Group: Records clustered by GROUP BY keys using hash tables
   3. Transform: Aggregate functions applied within groups (SUM, AVG, etc.)
   4. Combine: Merge partial results from parallel workers
   5. Results: Final aggregated output rows

4. **Index-Assisted Pattern**
   ```
   Query → IndexLookup → RecordFetch → Results
   ```
   Fast record access through indexes:
   1. Query: Parse and analyze query conditions (e.g., WHERE id = 5)
   2. IndexLookup: Use B-tree/hash index to find matching record locations
   3. RecordFetch: Retrieve full records using found locations
   4. Results: Return matching records in required order
   Using indexes to optimize data access paths.

## Dependencies and Requirements

- Java Development Kit (JDK)
- JDBM (Java Database Manager) library for B-tree index support
- JSQLParser for SQL parsing and representation
- Make/GCC (for compiling the TPC-H data generator)

## Installation and Setup

1. Clone the repository
2. Compile the TeamCode Java files:
   ```bash
   javac TeamCode/*.java
   ```

3. Build the TPC-H data generator:
   ```bash
   cd tpch_dbgen/dbgen
   make
   ```

4. Generate TPC-H data (replace SF with desired scale factor):
   ```bash
   ./dbgen -s SF
   ```

## Usage Instructions

### Running Queries

The query engine supports standard SQL queries with emphasis on TPC-H benchmark operations:

- Selection operations
- Projection and table operations
- Aggregate functions
- Hash-based joins
- ORDER BY operations
- Index-based optimizations

### Key Components

1. **AggregateOperations**: Handles SQL aggregate functions (COUNT, SUM, AVG, etc.)
2. **BuildIndexes**: Creates and manages B-tree indexes for performance optimization
3. **ExternalSort**: Implements disk-based sorting for large datasets
4. **HashJoin** and **HybridHash**: Efficient join implementations
5. **ProjectTableOperation**: Handles column projections
6. **SelectionOperation**: Processes WHERE clause filters
7. **OrderByOperation**: Manages result sorting

## TPC-H Integration

The repository includes full TPC-H support:

- Complete data generation tools (dbgen)
- Standard TPC-H queries
- Reference data and expected results
- Schema definitions and table structures

The TPC-H components allow for benchmarking and testing the query engine against standard industry workloads.

## Performance Optimizations

The engine implements several optimization techniques:

- B-tree indexing for faster lookups
- Hash-based join algorithms
- External sorting for large datasets
- Hybrid hash join for memory efficiency
- Index-based query execution plans

## Contributing

When contributing to this repository:

1. Maintain the existing code structure
2. Add appropriate Java documentation
3. Test changes against TPC-H benchmark queries
4. Follow the established coding style

## License

The custom query engine code is available under the repository's license terms. The TPC-H tools are subject to their own licensing terms as specified in the tpch_dbgen directory.