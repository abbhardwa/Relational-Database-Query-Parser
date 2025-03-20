# SQL Query Engine with TPC-H Support

This repository contains a custom SQL query processing engine implementation in Java along with TPC-H benchmark support tools. The project provides a framework for executing SQL queries over TPC-H benchmark data with various query optimization techniques including hash joins, aggregation operations, and indexing.

## Architecture Overview

The SQL Query Engine is built with a modular architecture focusing on extensibility and performance:

### Core Components

1. **Query Execution Pipeline**
   - `QueryExecutionService`: Orchestrates the query execution workflow
   - `DatabaseOperation`: Base interface for all query operations
   - Modular operator implementations for different SQL operations

2. **Data Model**
   - `model.Table`: Core table representation and data management
   - Memory-efficient data structures for query processing
   - Support for various data types and operations

3. **Operation Implementations**
   - Specialized operators for different SQL operations:
     - `AggregateOperation`: GROUP BY and aggregate functions
     - `SelectionOperation`: WHERE clause evaluation
     - `HashJoin`: Efficient join implementation
     - `OrderByOperation`: Result sorting
     - `ProjectTableOperation`: Column projection

4. **Utility Layer**
   - `FileUtils`: Data file handling and I/O operations
   - `QueryUtils`: SQL query parsing and optimization

## Project Structure

```
.
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── edu/buffalo/cse562/
│   │   │       ├── model/                 # Data models & table definitions
│   │   │       │   └── Table.java
│   │   │       ├── operations/            # Query operation implementations
│   │   │       │   ├── AggregateOperation.java
│   │   │       │   ├── DatabaseOperation.java
│   │   │       │   └── SelectionOperation.java
│   │   │       ├── service/              # Core services
│   │   │       │   └── QueryExecutionService.java
│   │   │       ├── util/                 # Utility classes
│   │   │       │   ├── FileUtils.java
│   │   │       │   └── QueryUtils.java
│   │   │       └── *.java                # Core operation implementations
│   │   └── resources/
│   │       └── queries/                  # TPC-H benchmark queries
│   └── test/
│       ├── java/                         # Test suites
│       │   └── edu/buffalo/cse562/
│       │       ├── model/
│       │       ├── service/
│       │       └── util/
│       └── resources/
│           └── testdata/                 # Test data files
├── tools/
│   └── dbgen/                           # TPC-H data generation tools
├── pom.xml                              # Maven build configuration
└── README.md                            # Project documentation

## Development Setup

### Prerequisites

- Java Development Kit (JDK) 8 or higher
- Maven 3.6+ for dependency management and build
- Git for version control
- Make and GCC for TPC-H data generator compilation
- Minimum 4GB RAM recommended for development

### Environment Setup

1. Clone the repository and navigate to the project directory:
   ```bash
   git clone <repository-url>
   cd database-system
   ```

2. Set up Java environment variables:
   ```bash
   export JAVA_HOME=/path/to/your/jdk
   export PATH=$JAVA_HOME/bin:$PATH
   ```

3. Verify installation:
   ```bash
   java -version
   mvn -version
   ```

### Build and Test

The project uses Maven for build automation and dependency management:

1. Clean and compile the project:
   ```bash
   mvn clean compile
   ```

2. Run unit tests:
   ```bash
   mvn test
   ```

3. Generate test coverage report:
   ```bash
   mvn jacoco:report
   ```
   The coverage report will be available at `target/site/jacoco/index.html`

4. Create executable JAR:
   ```bash
   mvn package
   ```

### TPC-H Data Generation

1. Build the TPC-H data generator:
   ```bash
   cd tools/dbgen
   make clean
   make
   ```

2. Generate TPC-H data (SF is scale factor, e.g. 1 for 1GB):
   ```bash
   ./dbgen -s SF -f
   ```

3. Move generated files to your data directory:
   ```bash
   mv *.tbl /path/to/data/directory/
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

## Performance Tuning

### Memory Configuration

- Default JVM heap size is configured in `pom.xml`
- For production use, recommended settings:
  ```bash
  java -Xmx4g -Xms2g -XX:+UseG1GC -jar database-system.jar
  ```

### Query Optimization

The engine implements several optimization techniques:

1. **B-tree Indexing**
   - Automatically built for foreign key columns
   - Configurable for additional columns via `BuildIndexes`

2. **Join Optimizations**
   - Hash Join for equi-joins
   - Hybrid Hash Join for memory efficiency
   - Dynamic join order selection

3. **Memory Management**
   - External sorting for large datasets
   - Configurable buffer sizes in `FileUtils`
   - Batch processing for large result sets

4. **I/O Optimization**
   - Buffered reading/writing
   - Memory-mapped files for large tables
   - Parallel I/O operations where possible

## Development Standards

### Code Style

- Follow Java coding conventions
- Use meaningful variable and method names
- Document public APIs and complex logic
- Keep methods focused and concise (< 50 lines recommended)

### Testing Guidelines

1. **Unit Tests**
   - Required for all new functionality
   - Min 80% coverage for new code
   - Use appropriate test categories:
     - Fast tests: Unit tests
     - Slow tests: Integration tests
     - Memory tests: Large dataset tests

2. **Test Data**
   - Use `src/test/resources/testdata` for test files
   - Keep test data minimal but sufficient
   - Document test data format and purpose

### Code Review Process

1. Create feature branch from `main`
2. Follow commit message format:
   ```
   type(scope): description
   
   - type: feat, fix, docs, style, refactor, test, chore
   - scope: component affected
   - description: present tense, imperative mood
   ```
3. Submit PR with:
   - Clear description of changes
   - Test results and coverage report
   - Performance impact assessment
   - Breaking changes noted

## Troubleshooting

### Common Issues

1. **OutOfMemoryError**
   - Increase JVM heap size: `-Xmx4g`
   - Enable GC logging: `-XX:+PrintGCDetails`
   - Consider using external sort for large datasets

2. **Slow Query Performance**
   - Check index usage with debug logs
   - Verify join order optimization
   - Use EXPLAIN PLAN for query analysis

3. **Build Failures**
   - Clear Maven cache: `mvn clean`
   - Update dependencies: `mvn versions:display-dependency-updates`
   - Verify Java version compatibility

### Debug Mode

Enable debug logging by setting environment variable:
```bash
export DEBUG_LEVEL=DEBUG
```

### Support

For issues and questions:
1. Check existing GitHub issues
2. Review troubleshooting guide
3. Submit detailed bug report with:
   - Environment details
   - Steps to reproduce
   - Error messages and logs

## License and Acknowledgments

- This project is licensed under the Apache License 2.0
- TPC-H tools and benchmark are subject to [TPC License](http://www.tpc.org/information/about/copyright.asp)

### Third-party Components

- JSqlParser (Apache License 2.0)
- JUnit (Eclipse Public License 1.0)
- Maven plugins (Various Apache and MIT licenses)

## Release Process

### Version Numbers

Follow Semantic Versioning (SemVer):
- MAJOR.MINOR.PATCH
- Breaking changes increment MAJOR
- New features increment MINOR
- Bug fixes increment PATCH

### Release Steps

1. Update version in `pom.xml`
2. Update CHANGELOG.md
3. Create release tag
4. Deploy artifacts
5. Update documentation

---

For more information, visit our [Wiki](https://github.com/yourorg/database-system/wiki) or contact the maintainers.