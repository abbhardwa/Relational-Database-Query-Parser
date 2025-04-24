package edu.buffalo.cse562.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for parsing command line arguments.
 */
public class CommandLineParser {
    private File dataDirectory;
    private File swapDirectory;
    private File indexDirectory;
    private boolean buildPhaseFlag;
    private List<File> sqlFileList;

    /**
     * Creates a new CommandLineParser instance.
     */
    public CommandLineParser() {
        this.sqlFileList = new ArrayList<>();
        this.buildPhaseFlag = false;
    }
    
    /**
     * Parses command line arguments.
     * 
     * @param args Command line arguments to parse
     */
    public void parse(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--data")) {
                dataDirectory = new File(args[++i]);
            } else if (args[i].equals("--swap")) {
                swapDirectory = new File(args[++i]);
            } else if (args[i].equals("--build")) {
                buildPhaseFlag = true;
            } else if (args[i].equals("--index")) {
                indexDirectory = new File(args[++i]);
            } else {
                sqlFileList.add(new File(args[i]));
            }
        }
    }

    /**
     * Validates the parsed arguments.
     * 
     * @throws IllegalArgumentException If required arguments are missing
     */
    public void validate() {
        if (dataDirectory == null || !dataDirectory.isDirectory()) {
            throw new IllegalArgumentException("Please provide a valid data directory with --data argument");
        }
    }

    /**
     * Gets the data directory.
     * 
     * @return The data directory
     */
    public File getDataDirectory() {
        return dataDirectory;
    }

    /**
     * Gets the swap directory.
     * 
     * @return The swap directory
     */
    public File getSwapDirectory() {
        return swapDirectory;
    }

    /**
     * Gets the index directory.
     * 
     * @return The index directory
     */
    public File getIndexDirectory() {
        return indexDirectory;
    }

    /**
     * Checks if build phase flag is set.
     * 
     * @return true if build phase is enabled, false otherwise
     */
    public boolean isBuildPhaseEnabled() {
        return buildPhaseFlag;
    }

    /**
     * Gets the list of SQL files.
     * 
     * @return List of SQL files
     */
    public List<File> getSqlFileList() {
        return sqlFileList;
    }
}