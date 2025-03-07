# SQL Query Engine with TPC-H Support

This repository contains a custom SQL query processing engine implementation in Java along with TPC-H benchmark support tools. The project provides a framework for executing SQL queries over TPC-H benchmark data with various query optimization techniques including hash joins, aggregation operations, and indexing.

## Repository Structure

The repository is organized into three main components:

- `TeamCode/` - Contains the Java implementation of the SQL query engine
  - Query Operations (Selection, Projection, Aggregation, etc.)
  - Join Operations (Hash Join, Hybrid Hash)
  - Index Building and Management
  - External Sort Implementation
  
- `tpch_dbgen/` - The TPC-H data generation tool
  - Database population scripts
  - Reference data and answer sets
  - Data generation utilities

- `tpch_queries/` - TPC-H benchmark queries
  - SQL query templates
  - Schema definitions
  - Sample queries (TPC-H queries 1-16)

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