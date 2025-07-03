# SQL Query Engine with TPC-H Support

A robust, Java-based relational database query processing engine designed to execute SQL queries efficiently with specialized support for TPC-H benchmarks. This project implements a comprehensive database system with advanced query optimization techniques, indexing mechanisms, and data processing capabilities.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation and Setup](#installation-and-setup)
- [Configuration](#configuration)
- [Usage Guide](#usage-guide)
- [SQL Features Support](#sql-features-support)
- [TPC-H Integration](#tpc-h-integration)
- [Performance Optimizations](#performance-optimizations)
- [API Documentation](#api-documentation)
- [Development Guide](#development-guide)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

This SQL query engine provides a complete relational database management system implementation with focus on:

- **Query Processing**: Full SQL query parsing, optimization, and execution
- **Storage Management**: Efficient data storage and retrieval mechanisms
- **Indexing**: B-tree and hash-based indexing for optimal performance
- **Join Operations**: Multiple join algorithms including hash join and hybrid hash join
- **Aggregation**: Support for complex aggregate operations and GROUP BY clauses
- **TPC-H Compliance**: Specialized support for TPC-H benchmark queries and data

### Key Features

- ✅ SQL query parsing using JSQLParser
- ✅ Support for SELECT, INSERT, UPDATE, DELETE operations
- ✅ Advanced join algorithms (Hash Join, Hybrid Hash Join)
- ✅ External sorting for large datasets
- ✅ B-tree indexing for fast data access
- ✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ✅ ORDER BY and GROUP BY operations
- ✅ TPC-H benchmark integration
- ✅ Memory-efficient query execution
- ✅ Extensible architecture for custom operations

## Architecture

The system follows a layered architecture pattern:

```
┌─────────────────────────────────────────┐
│            SQL Interface                │
├─────────────────────────────────────────┤
│         Query Parser (JSQLParser)       │
├─────────────────────────────────────────┤
│         Query Execution Engine          │
│  ┌─────────────┬─────────────────────┐  │
│  │ Operations  │  Storage & Indexing │  │
│  │   Layer     │       Layer         │  │
│  └─────────────┴─────────────────────┘  │
├─────────────────────────────────────────┤
│          Data Access Layer              │
└─────────────────────────────────────────┘
```

### Core Components

1. **Model Layer** (`src/main/java/edu/buffalo/cse562/model/`)
   - `Table.java`: Core table representation with metadata management

2. **Service Layer** (`src/main/java/edu/buffalo/cse562/service/`)
   - `QueryExecutionService.java`: Central query execution orchestration

3. **Operations Layer** (`src/main/java/edu/buffalo/cse562/operations/`)
   - `SelectionOperation.java`: WHERE clause processing
   - `AggregateOperation.java`: Aggregate function handling
   - `DatabaseOperation.java`: Base operation interface

4. **Legacy Operations** (Main package - `src/main/java/edu/buffalo/cse562/`)
   - Advanced algorithms for joins, sorting, and indexing

## Project Structure

```
Relational-Database-Query-Parser/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── edu/buffalo/cse562/
│   │   │       ├── model/                    # Data models
│   │   │       │   └── Table.java
│   │   │       ├── service/                  # Business logic layer
│   │   │       │   └── QueryExecutionService.java
│   │   │       ├── operations/               # Query operations
│   │   │       │   ├── DatabaseOperation.java
│   │   │       │   ├── SelectionOperation.java
│   │   │       │   └── AggregateOperation.java
│   │   │       ├── util/                     # Utility classes
│   │   │       ├── Main.java                 # Application entry point
│   │   │       ├── AggregateOperations.java  # Aggregate functions
│   │   │       ├── BuildIndexes.java         # Index management
│   │   │       ├── ExternalSort.java         # Sorting algorithms
│   │   │       ├── HashJoin.java             # Hash join implementation
│   │   │       ├── HybridHash.java           # Hybrid hash join
│   │   │       ├── OrderByOperation.java     # ORDER BY processing
│   │   │       ├── ProjectTableOperation.java # Projection operations
│   │   │       ├── SelectionOperation.java   # Selection processing
│   │   │       └── WhereOperation.java       # WHERE clause handling
│   │   └── resources/
│   │       └── queries/                      # Sample SQL queries
│   └── test/
│       └── java/
│           └── edu/buffalo/cse562/           # Unit and integration tests
│               └── service/
│                   └── QueryExecutionServiceTest.java
├── tools/
│   └── dbgen/                               # TPC-H data generation tools
│       ├── dbgen/                           # C source code for data generation
│       │   ├── makefile                     # Build configuration
│       │   ├── build.c                      # Core data generation logic
│       │   ├── config.h                     # Platform configurations
│       │   └── ...                          # Additional C source files
│       └── ref_data/                        # Reference datasets
│           └── 1/                           # Scale factor 1 data
│               └── part.tbl.1               # Sample part table data
├── pom.xml                                  # Maven build configuration
├── .gitignore                               # Git ignore rules
└── README.md                                # This documentation
```

## Prerequisites

### System Requirements

- **Java Development Kit (JDK)**: Version 8 or higher
- **Maven**: Version 3.6.0 or higher (for building)
- **GCC/Make**: For building TPC-H data generation tools
- **Memory**: Minimum 2GB RAM (4GB+ recommended for large datasets)
- **Storage**: Adequate disk space for datasets (varies by scale factor)

### Operating System Support

- ✅ Linux (Ubuntu, CentOS, RHEL)
- ✅ macOS
- ✅ Windows (with MinGW or Cygwin for TPC-H tools)

### Software Dependencies

The project uses Maven for dependency management. Key dependencies include:

- **JSQLParser 4.5**: SQL parsing and AST generation
- **JUnit 4.13.2**: Unit testing framework
- **JaCoCo 0.8.7**: Code coverage analysis

## Installation and Setup

### Quick Start

1. **Clone the Repository**
   ```bash
   git clone https://github.com/abbhardwa/Relational-Database-Query-Parser.git
   cd Relational-Database-Query-Parser
   ```

2. **Build the Java Application**
   ```bash
   mvn clean compile
   ```

3. **Run Tests** (Optional)
   ```bash
   mvn test
   ```

### TPC-H Tools Setup

1. **Navigate to TPC-H Directory**
   ```bash
   cd tools/dbgen/dbgen
   ```

2. **Build Data Generation Tools**
   ```bash
   make
   ```

3. **Generate Sample Data**
   ```bash
   # Generate scale factor 1 data (approximately 1GB)
   ./dbgen -s 1
   
   # For larger datasets:
   # ./dbgen -s 10  # ~10GB dataset
   # ./dbgen -s 100 # ~100GB dataset
   ```

### Directory Structure Setup

Create the necessary directories for data and indexes:

```bash
mkdir -p data
mkdir -p swap
mkdir -p index
```

## Configuration

### Build Configuration

The Maven `pom.xml` file contains all build configurations:

- **Java Version**: 1.8 (configurable via properties)
- **Encoding**: UTF-8
- **Test Coverage**: JaCoCo with 50% minimum line coverage
- **Compiler Settings**: Source and target compatibility

### Runtime Configuration

Configure the application using command-line arguments:

```bash
# Basic execution
java -cp target/classes edu.buffalo.cse562.Main \
  --data /path/to/data \
  query1.sql query2.sql

# With indexing support
java -cp target/classes edu.buffalo.cse562.Main \
  --data /path/to/data \
  --index /path/to/index \
  --swap /path/to/swap \
  --build \
  schema.sql queries.sql
```

## Usage Guide

### Basic Query Execution

1. **Prepare SQL Files**
   Create SQL files with your queries:
   ```sql
   -- schema.sql
   CREATE TABLE customers (
       custkey INTEGER PRIMARY KEY,
       name VARCHAR(25),
       address VARCHAR(40),
       nationkey INTEGER,
       phone CHAR(15),
       acctbal DECIMAL(15,2),
       mktsegment CHAR(10),
       comment VARCHAR(117)
   );
   
   -- query.sql
   SELECT name, acctbal 
   FROM customers 
   WHERE acctbal > 5000 
   ORDER BY acctbal DESC;
   ```

2. **Execute Queries**
   ```bash
   java -cp target/classes edu.buffalo.cse562.Main \
     --data ./data \
     schema.sql query.sql
   ```

### Advanced Usage Examples

#### Example 1: TPC-H Query Execution
```bash
# Set up TPC-H data
cd tools/dbgen/dbgen
./dbgen -s 1
mv *.tbl ../../../data/

# Run TPC-H queries
java -cp target/classes edu.buffalo.cse562.Main \
  --data ./data \
  tpch_schema.sql tpch_query01.sql
```

#### Example 2: Index-Optimized Queries
```bash
# Build phase - create indexes
java -cp target/classes edu.buffalo.cse562.Main \
  --data ./data \
  --index ./index \
  --swap ./swap \
  --build \
  schema.sql

# Query phase - use indexes
java -cp target/classes edu.buffalo.cse562.Main \
  --data ./data \
  --index ./index \
  query.sql
```

#### Example 3: Programmatic API Usage
```java
import edu.buffalo.cse562.service.QueryExecutionService;
import edu.buffalo.cse562.model.Table;

public class Example {
    public static void main(String[] args) throws Exception {
        QueryExecutionService service = new QueryExecutionService();
        
        // Register tables
        service.registerTable("customers", customerTable);
        
        // Execute query
        String sql = "SELECT * FROM customers WHERE acctbal > 5000";
        Table result = service.executeQuery(sql);
        
        // Process results
        for (String tuple : result.getTuples()) {
            System.out.println(tuple);
        }
    }
}
```

## SQL Features Support

### Supported SQL Operations

| Feature | Status | Notes |
|---------|--------|-------|
| SELECT | ✅ | Full support with projections |
| WHERE | ✅ | Complex predicates supported |
| JOIN | ✅ | INNER, OUTER joins via hash algorithms |
| GROUP BY | ✅ | With aggregate functions |
| ORDER BY | ✅ | ASC/DESC with multiple columns |
| INSERT | ✅ | Single and batch inserts |
| UPDATE | ✅ | With WHERE conditions |
| DELETE | ✅ | With WHERE conditions |
| CREATE TABLE | ✅ | With column definitions and indexes |
| Aggregate Functions | ✅ | COUNT, SUM, AVG, MIN, MAX |
| Subqueries | ⚠️ | Limited support |
| Window Functions | ❌ | Not implemented |
| UNION/INTERSECT | ❌ | Not implemented |

### Data Types Support

- INTEGER/INT
- DECIMAL/NUMERIC
- VARCHAR/CHAR
- DATE
- Custom precision and scale support

### Aggregate Functions

```sql
-- Supported aggregate operations
SELECT 
    COUNT(*) as total_customers,
    SUM(acctbal) as total_balance,
    AVG(acctbal) as avg_balance,
    MIN(acctbal) as min_balance,
    MAX(acctbal) as max_balance
FROM customers
GROUP BY mktsegment;
```

## TPC-H Integration

### TPC-H Benchmark Overview

The Transaction Processing Performance Council Benchmark H (TPC-H) is a decision support benchmark that consists of a suite of business-oriented ad-hoc queries and concurrent data modifications.

### Supported TPC-H Components

#### 1. Data Generation
- **DBGEN**: Generates benchmark data at various scale factors
- **Scale Factors**: 1, 10, 100, 1000+ supported
- **Tables**: All 8 TPC-H tables (part, supplier, partsupp, customer, orders, lineitem, nation, region)

#### 2. Schema Support
```sql
-- Example TPC-H table definition
CREATE TABLE lineitem (
    l_orderkey INTEGER,
    l_partkey INTEGER,
    l_suppkey INTEGER,
    l_linenumber INTEGER,
    l_quantity DECIMAL(15,2),
    l_extendedprice DECIMAL(15,2),
    l_discount DECIMAL(15,2),
    l_tax DECIMAL(15,2),
    l_returnflag CHAR(1),
    l_linestatus CHAR(1),
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct CHAR(25),
    l_shipmode CHAR(10),
    l_comment VARCHAR(44),
    PRIMARY KEY (l_orderkey, l_linenumber)
);
```

#### 3. Query Support
The engine supports most TPC-H queries (Q1-Q22) with optimizations for:
- Complex joins across multiple tables
- Aggregate operations with grouping
- Date range filtering
- Sorting and ranking operations

#### 4. Performance Benchmarking
```bash
# Generate different scale factors for performance testing
./dbgen -s 1    # ~1GB dataset
./dbgen -s 10   # ~10GB dataset
./dbgen -s 100  # ~100GB dataset

# Run benchmark queries
time java -cp target/classes edu.buffalo.cse562.Main \
  --data ./data \
  --index ./index \
  tpch_schema.sql tpch_queries.sql
```

## Performance Optimizations

### Query Optimization Techniques

#### 1. Indexing Strategies
- **B-tree Indexes**: For range queries and ordered access
- **Hash Indexes**: For equality predicates
- **Composite Indexes**: For multi-column lookups

```java
// Index creation example
BuildIndexes.buildIndex(createTableStatement, indexDirectory);
```

#### 2. Join Algorithms
- **Hash Join**: For equi-joins with moderate data sizes
- **Hybrid Hash Join**: Memory-efficient for large datasets
- **Index-based Joins**: When appropriate indexes exist

#### 3. Sorting Optimizations
- **External Sort**: For datasets larger than memory
- **Multi-way Merge**: Efficient external sorting implementation
- **Memory Management**: Configurable buffer sizes

#### 4. Memory Management
```java
// Configurable buffer sizes for optimal performance
public static final int BUFFER_SIZE = 32768;
public static final int SORT_BUFFER_SIZE = 1024 * 1024; // 1MB
```

### Performance Tuning Guidelines

1. **Memory Allocation**
   ```bash
   # Increase JVM heap size for large datasets
   java -Xmx8g -Xms2g -cp target/classes edu.buffalo.cse562.Main ...
   ```

2. **Index Strategy**
   - Create indexes on frequently queried columns
   - Use composite indexes for multi-column predicates
   - Build indexes during off-peak hours

3. **Query Optimization**
   - Place selective predicates early in WHERE clauses
   - Use appropriate join order for multi-table queries
   - Leverage ORDER BY optimizations with indexes

## API Documentation

### Core Classes

#### Table Class
```java
public class Table {
    // Constructor
    public Table(String tableName, int columnCount, File dataFile, File dataDirectory)
    
    // Data operations
    public void populateTable() throws IOException
    public void populateColumnIndexMap()
    public String getNextTuple() throws IOException
    
    // Getters/Setters
    public String getTableName()
    public ArrayList<String> getTuples()
    public HashMap<String, Integer> getColumnIndexMap()
}
```

#### QueryExecutionService Class
```java
public class QueryExecutionService {
    // Query execution
    public Table executeQuery(String sql) throws Exception
    
    // Table management
    public void registerTable(String tableName, Table table)
    public Table getTable(String tableName)
}
```

### Operation Interfaces

#### SelectionOperation
```java
public class SelectionOperation {
    public SelectionOperation(Statement statement, HashMap<String, Table> tableMap)
    public Table execute() throws IOException
}
```

## Development Guide

### Setting up Development Environment

1. **IDE Configuration**
   - IntelliJ IDEA or Eclipse recommended
   - Import as Maven project
   - Configure Java 8+ SDK

2. **Code Style**
   - Follow Java naming conventions
   - Use meaningful variable names
   - Add Javadoc comments for public methods
   - Maintain consistent indentation (4 spaces)

3. **Build Process**
   ```bash
   # Compile code
   mvn compile
   
   # Run tests
   mvn test
   
   # Generate coverage report
   mvn test jacoco:report
   
   # Package application
   mvn package
   ```

### Adding New Features

#### 1. Adding New SQL Operations
```java
// Example: Adding DISTINCT support
public class DistinctOperation implements DatabaseOperation {
    @Override
    public Table execute() throws IOException {
        // Implementation
    }
}
```

#### 2. Adding New Index Types
```java
// Example: Adding bitmap indexes
public class BitmapIndex {
    public void buildIndex(Table table, String columnName) {
        // Implementation
    }
}
```

#### 3. Adding New Join Algorithms
```java
// Example: Adding sort-merge join
public class SortMergeJoin {
    public static Table performJoin(Table leftTable, Table rightTable, 
                                   String joinCondition) {
        // Implementation
    }
}
```

### Code Architecture Guidelines

1. **Separation of Concerns**
   - Keep parsing logic separate from execution
   - Isolate storage operations from query processing
   - Maintain clear interfaces between layers

2. **Error Handling**
   ```java
   try {
       // Operation
   } catch (IOException e) {
       logger.error("I/O error during operation", e);
       throw new DatabaseException("Operation failed", e);
   }
   ```

3. **Resource Management**
   ```java
   try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
       // Use reader
   } // Automatically closed
   ```

## Testing

### Unit Testing

The project includes comprehensive unit tests using JUnit 4:

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=QueryExecutionServiceTest

# Run tests with coverage
mvn test jacoco:report
```

### Test Structure

```java
public class QueryExecutionServiceTest {
    @Test
    public void testSimpleSelect() throws ParseException {
        String sql = "SELECT * FROM customers";
        Table result = queryService.executeQuery(sql);
        assertNotNull("Result should not be null", result);
    }
    
    @Test
    public void testSelectWithWhere() throws ParseException {
        String sql = "SELECT * FROM customers WHERE state = 'CA'";
        Table result = queryService.executeQuery(sql);
        assertEquals("Should return CA customers", 2, result.getTuples().size());
    }
}
```

### Integration Testing

Test with real TPC-H data:

```bash
# Generate test data
cd tools/dbgen/dbgen
./dbgen -s 0.01  # Small dataset for testing

# Run integration tests
mvn test -Dtest=TpchIntegrationTest
```

### Performance Testing

```bash
# Benchmark with different scale factors
./benchmark.sh 1 10 100

# Profile memory usage
java -XX:+PrintGCDetails -Xloggc:gc.log \
  -cp target/classes edu.buffalo.cse562.Main ...
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Compilation Errors
**Problem**: `cannot find symbol` errors
```
[ERROR] cannot find symbol: class Table
```

**Solution**: Ensure all imports are correct and dependencies are resolved:
```bash
mvn clean compile
```

#### 2. Memory Issues
**Problem**: `OutOfMemoryError` during large dataset processing
```
java.lang.OutOfMemoryError: Java heap space
```

**Solution**: Increase JVM heap size:
```bash
export MAVEN_OPTS="-Xmx8g -Xms2g"
java -Xmx8g -cp target/classes edu.buffalo.cse562.Main ...
```

#### 3. File Permission Issues
**Problem**: Cannot read/write data files
```
java.io.FileNotFoundException: Permission denied
```

**Solution**: Check file permissions:
```bash
chmod 755 data/
chmod 644 data/*.tbl
```

#### 4. TPC-H Build Issues
**Problem**: `make` fails in dbgen directory
```
gcc: command not found
```

**Solution**: Install build tools:
```bash
# Ubuntu/Debian
sudo apt-get install build-essential

# CentOS/RHEL
sudo yum groupinstall "Development Tools"

# macOS
xcode-select --install
```

#### 5. Large Dataset Performance
**Problem**: Queries take too long on large datasets

**Solutions**:
- Use appropriate indexes:
  ```bash
  java ... --build schema.sql  # Build indexes first
  ```
- Optimize JVM settings:
  ```bash
  java -XX:+UseG1GC -XX:MaxGCPauseMillis=200 ...
  ```
- Use external sorting for large ORDER BY operations

### Debug Mode

Enable verbose logging for troubleshooting:

```bash
java -Djava.util.logging.config.file=logging.properties \
  -cp target/classes edu.buffalo.cse562.Main ...
```

### Performance Monitoring

Monitor query execution:

```bash
# Enable JVM monitoring
java -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.port=9999 \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -cp target/classes edu.buffalo.cse562.Main ...
```

## Contributing

We welcome contributions to improve the SQL query engine! Here's how to get started:

### Development Workflow

1. **Fork the Repository**
   ```bash
   git clone https://github.com/your-username/Relational-Database-Query-Parser.git
   cd Relational-Database-Query-Parser
   ```

2. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make Changes**
   - Follow the coding standards
   - Add appropriate tests
   - Update documentation

4. **Test Your Changes**
   ```bash
   mvn test
   mvn test jacoco:report  # Ensure coverage >= 50%
   ```

5. **Submit Pull Request**
   - Provide clear description of changes
   - Include test results
   - Reference any related issues

### Contribution Guidelines

#### Code Quality Standards
- **Test Coverage**: Maintain >= 50% line coverage
- **Documentation**: Add Javadoc for public methods
- **Code Style**: Follow existing patterns and conventions
- **Error Handling**: Implement proper exception handling

#### Areas for Contribution
- **SQL Feature Extensions**: Add support for new SQL operations
- **Performance Optimizations**: Improve query execution efficiency
- **Index Types**: Implement new indexing strategies
- **Testing**: Expand test coverage and add integration tests
- **Documentation**: Improve code documentation and examples

#### Submitting Issues
When reporting bugs:
- Include Java version and OS information
- Provide steps to reproduce the issue
- Include relevant log output
- Attach sample data if applicable

### Code Review Process
1. All changes require review by maintainers
2. Automated tests must pass
3. Code coverage requirements must be met
4. Documentation must be updated for API changes

## License

This project is available under the repository's license terms. 

### Third-Party Components
- **TPC-H Tools**: Subject to TPC-H license terms (see `tools/dbgen/` directory)
- **JSQLParser**: Licensed under Apache License 2.0
- **JUnit**: Licensed under Eclipse Public License

### Usage Rights
The custom query engine implementation is available for:
- Educational purposes
- Research and academic use
- Commercial applications (subject to license terms)

For specific licensing questions, please refer to the LICENSE file in the repository root.

---

**Last Updated**: December 2024  
**Version**: 1.0  
**Maintainers**: Database Systems Research Team