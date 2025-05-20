/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562.model;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Represents a database table with its structure and data.
 * This class encapsulates all the properties and operations related to a database table.
 */
public class Table {
    private String tableName;
    private int columnCount;
    private File dataFile;
    private File dataDirectory;
    private ArrayList<ColumnDefinition> columnDefinitions;
    private HashMap<String, Integer> columnIndexMap;
    private ArrayList<String> tuples;
    private FileReader fileReader;
    private BufferedReader bufferedReader;

    /**
     * Constructs a new Table instance.
     *
     * @param tableName The name of the table
     * @param columnCount Number of columns in the table
     * @param dataFile File containing table data
     * @param dataDirectory Directory containing the table data file
     */
    public Table(String tableName, int columnCount, File dataFile, File dataDirectory) {
        this.tableName = tableName;
        this.columnCount = columnCount;
        this.dataFile = dataFile;
        this.dataDirectory = dataDirectory;
        this.columnDefinitions = new ArrayList<>();
        this.columnIndexMap = new HashMap<>();
        this.tuples = new ArrayList<>();
        this.fileReader = null;
        this.bufferedReader = null;
    }

    /**
     * Copy constructor to clone a table.
     *
     * @param tableToClone The table to be cloned
     */
    public Table(Table tableToClone) {
        this.tableName = tableToClone.tableName;
        this.columnCount = tableToClone.columnCount;
        this.dataFile = tableToClone.dataFile;
        this.dataDirectory = tableToClone.dataDirectory;
        this.columnDefinitions = tableToClone.columnDefinitions;
        this.columnIndexMap = tableToClone.columnIndexMap;
        this.tuples = tableToClone.tuples;
        this.fileReader = null;
        this.bufferedReader = null;
    }

    /**
     * Populates the table's tuples by reading from the data file.
     *
     * @throws IOException If an I/O error occurs
     */
    public void populateTable() throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(dataFile))) {
            String tuple;
            while ((tuple = br.readLine()) != null) {
                tuples.add(tuple);
            }
        }
    }

    /**
     * Reads and prints all tuples in the table.
     */
    public void readTable() {
        for (String tuple : tuples) {
            System.out.println(tuple);
        }
    }

    /**
     * Populates the column index map based on column definitions.
     */
    public void populateColumnIndexMap() {
        int indexCounter = 0;
        for (ColumnDefinition columnDef : columnDefinitions) {
            columnIndexMap.put(columnDef.getColumnName().toLowerCase(), indexCounter++);
        }
    }

    /**
     * Initializes buffered reader for table file access.
     *
     * @throws FileNotFoundException If the table file cannot be found
     */
    public void initializeBufferedReader() throws FileNotFoundException {
        this.fileReader = new FileReader(dataFile);
        this.bufferedReader = new BufferedReader(fileReader, 32768);
    }

    /**
     * Returns the next tuple from the table file.
     *
     * @return The next tuple as a String, or null if end of file is reached
     * @throws IOException If an I/O error occurs
     */
    public String getNextTuple() throws IOException {
        return bufferedReader != null ? bufferedReader.readLine() : null;
    }

    /**
     * Closes the file readers.
     *
     * @throws IOException If an I/O error occurs
     */
    public void closeReaders() throws IOException {
        if (bufferedReader != null) {
            bufferedReader.close();
        }
        if (fileReader != null) {
            fileReader.close();
        }
    }

    // Getters and setters

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public File getDataFile() {
        return dataFile;
    }

    public File getDataDirectory() {
        return dataDirectory;
    }

    public ArrayList<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions;
    }

    public void setColumnDefinitions(ArrayList<ColumnDefinition> columnDefinitions) {
        this.columnDefinitions = columnDefinitions;
    }

    public HashMap<String, Integer> getColumnIndexMap() {
        return columnIndexMap;
    }

    public ArrayList<String> getTuples() {
        return tuples;
    }

    public void setTuples(ArrayList<String> tuples) {
        this.tuples = tuples;
    }
}