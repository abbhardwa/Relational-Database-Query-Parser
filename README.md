# SQL Query Engine with TPC-H Support

A high-performance SQL query engine written in Java that excels at processing TPC-H benchmark workloads. This engine is designed for analyzing large datasets efficiently through:

- Fast query processing with hash-based joins and B-tree indexing
- Memory-efficient operations using hybrid hash join and external sorting
- Full support for TPC-H benchmark queries and datasets
- Easy integration with standard SQL tools and workflows

The engine implements advanced optimization techniques to handle complex analytical queries while maintaining memory efficiency, making it suitable for both development testing and production workloads.

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

## Development Environment

### Requirements

- Java Development Kit (JDK) 8 or later
- Maven 3.6+
- Make/GCC (for TPC-H data generator)
- Git for version control

### IDE Setup

1. Import as Maven project:
   - IntelliJ IDEA: Import Project -> Select pom.xml
   - Eclipse: Import -> Existing Maven Projects
   - VS Code: Install Java Extension Pack

2. Configure JDK:
   - Set Project SDK to JDK 8 or later
   - Ensure Maven JDK matches project JDK

### Build and Test
```bash
# Build project
mvn clean install

# Run tests
mvn test

# Generate test coverage report
mvn verify
```

Code coverage reports are generated in `target/site/jacoco/`.

## Installation and Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd database-system
   ```

2. Build the project with Maven:
   ```bash
   mvn clean package
   ```

3. Build the TPC-H data generator:
   ```bash
   cd tools/dbgen/dbgen
   make
   ```

4. Generate TPC-H data (replace SF with desired scale factor, e.g. 1 for 1GB):
   ```bash
   ./dbgen -s SF -f    # Creates data files in the current directory
   ```

The build will run tests and generate a JAR file in the `target/` directory.

### Quick Start Example

1. Generate sample TPC-H data:
   ```bash
   cd tools/dbgen/dbgen
   make
   ./dbgen -s 1 -f     # Generates 1GB scale factor data
   ```

2. Run a simple TPC-H query:
   ```bash
   java -cp target/database-system-1.0-SNAPSHOT.jar edu.buffalo.cse562.Main \
     --data path/to/data/directory \
     --query "SELECT l_orderkey, l_quantity FROM lineitem WHERE l_quantity > 45"
   ```

3. Use indexes for better performance:
   ```bash
   # Create an index on l_quantity
   java -cp target/database-system-1.0-SNAPSHOT.jar edu.buffalo.cse562.BuildIndexes \
     --table lineitem \
     --column l_quantity \
     --data path/to/data/directory
   
   # Query using the index
   java -cp target/database-system-1.0-SNAPSHOT.jar edu.buffalo.cse562.Main \
     --data path/to/data/directory \
     --index \
     --query "SELECT l_orderkey FROM lineitem WHERE l_quantity > 45"
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

## Configuration and Performance Options

The query engine supports several configuration options to optimize performance for different workloads:

### Memory Settings
```bash
java -Xmx4g -cp target/database-system-1.0-SNAPSHOT.jar edu.buffalo.cse562.Main   # Set max heap to 4GB
```

### Available Command Line Options
- `--data <path>`: Directory containing data files
- `--index`: Use available indexes for query optimization
- `--memory <MB>`: Memory limit for operations in megabytes
- `--temp <path>`: Directory for temporary files
- `--explain`: Show query execution plan
- `--stats`: Display performance statistics

### Performance Optimizations
The engine implements several optimization techniques:

- B-tree indexing for faster lookups
- Hash-based join algorithms
- External sorting for large datasets
- Hybrid hash join for memory efficiency
- Index-based query execution plans

## Troubleshooting

Common issues and solutions:

### Out of Memory Errors
If you encounter OutOfMemoryError:
```
1. Increase Java heap space: java -Xmx4g -cp ...
2. Use --memory flag to limit operation memory
3. Enable external sorting: --temp /path/to/temp/dir
```

### Performance Issues
If queries are running slowly:
1. Create indexes on frequently filtered columns
2. Ensure adequate memory allocation
3. Use --explain to analyze query execution plan
4. Consider increasing buffer size for large joins

### TPC-H Data Generation
If dbgen fails:
1. Ensure make completed successfully
2. Check disk space for output files
3. Verify file permissions in output directory

For more help:
- Open an issue on GitHub
- Check test cases in `src/test/` for example usage
- Consult TPC-H documentation for data generation questions

## Version and Compatibility

[![Build Status](https://github.com/example/sql-query-engine/workflows/Build/badge.svg)](https://github.com/example/sql-query-engine/actions)
[![Coverage](https://codecov.io/gh/example/sql-query-engine/branch/main/graph/badge.svg)](https://codecov.io/gh/example/sql-query-engine)

### Version Information
- Current Version: 1.0-SNAPSHOT
- Java Compatibility: JDK 8 or later
- Maven Version: 3.6+
- JSqlParser: 4.5
- JUnit: 4.13.2

### Contributing

When contributing to this repository:

1. Maintain the existing code structure
2. Add appropriate Java documentation
3. Test changes against TPC-H benchmark queries
4. Follow the established coding style
5. Update tests for new functionality

## License

The custom query engine code is available under the repository's license terms. The TPC-H tools are subject to their own licensing terms as specified in the tools/dbgen directory.