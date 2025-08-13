# SQL Query Engine with TPC-H Support

This repository contains a custom SQL query processing engine implementation in Java along with TPC-H benchmark support tools. The project provides a framework for executing SQL queries over TPC-H benchmark data with various query optimization techniques including hash joins, aggregation operations, and indexing.

## Technologies Used

### Programming Languages
- **Java 8+** - Primary language for the query engine implementation
- **C/C++** - For TPC-H data generation utilities (dbgen)
- **Shell Scripting** - Automation and testing scripts

### Build Tools & Frameworks
- **Apache Maven 3.x** - Build automation and dependency management
- **Make/GCC** - For compiling C/C++ TPC-H data generator
- **JaCoCo 0.8.7** - Code coverage analysis

### Libraries & Dependencies
- **JSQLParser 4.5** - SQL parsing and AST representation
- **JUnit 4.13.2** - Unit testing framework
- **JDBM** - Java Database Manager for B-tree index support

### Database & Data Processing
- **TPC-H Benchmark Suite** - Standard decision support benchmark
- **B-tree Indexing** - For optimized data access
- **Hash-based Algorithms** - Join operations and data processing
- **External Sorting** - For handling large datasets

### Development Tools
- **Git** - Version control
- **Maven Compiler Plugin 3.8.1** - Java compilation

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