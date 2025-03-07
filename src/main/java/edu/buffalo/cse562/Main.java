package edu.buffalo.cse562;

import edu.buffalo.cse562.model.Table;
import edu.buffalo.cse562.service.QueryExecutionService;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Main entry point for the database query parser application.
 */
public class Main {
    private static QueryExecutionService queryService;

    public static void main(String[] args) {
        try {
            // Initialize query service
            queryService = new QueryExecutionService();

            // Process command line arguments
            File dataDir = null;
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("--data")) {
                    dataDir = new File(args[++i]);
                }
            }

            if (dataDir == null || !dataDir.isDirectory()) {
                throw new IllegalArgumentException("Please provide a valid data directory with --data argument");
            }

            // Process each SQL file
            for (int i = 0; i < args.length; i++) {
                if (args[i].endsWith(".sql")) {
                    processSqlFile(new File(args[i]), dataDir);
                }
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void processSqlFile(File sqlFile, File dataDir) throws Exception {
        try (FileReader reader = new FileReader(sqlFile)) {
            CCJSqlParser parser = new CCJSqlParser(reader);
            Statement statement;

            while ((statement = parser.Statement()) != null) {
                if (statement instanceof CreateTable) {
                    processCreateTable((CreateTable) statement, dataDir);
                } else {
                    // Execute query and display results
                    Table result = queryService.executeQuery(statement.toString());
                    if (result != null) {
                        displayResults(result);
                    }
                }
            }
        }
    }

    private static void processCreateTable(CreateTable createTable, File dataDir) {
        String tableName = createTable.getTable().getName().toLowerCase();
        File tableFile = Paths.get(dataDir.getPath(), tableName + ".dat").toFile();
        if (!tableFile.exists()) {
            tableFile = Paths.get(dataDir.getPath(), tableName + ".tbl").toFile();
        }

        Table table = new Table(
            tableName,
            createTable.getColumnDefinitions().size(),
            tableFile,
            dataDir
        );
        
        table.setColumnDefinitions(new ArrayList<>(createTable.getColumnDefinitions()));
        table.populateColumnIndexMap();
        
        try {
            table.populateTable();
            queryService.registerTable(tableName, table);
        } catch (IOException e) {
            System.err.println("Error loading table " + tableName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void displayResults(Table result) {
        if (result != null && result.getTuples() != null) {
            for (String tuple : result.getTuples()) {
                System.out.println(tuple);
            }
        }
    }
}
}
		// if both of these are null then allocate the reader object for the file corresponding to the table
		if(this.br == null && this.fr == null)
			this.allocateBufferedReaderForTableFile();

		// this string is used to read a '|' delimited tuple of the .dat or .tbl file
		String scanString = null;
		// read the string using the BufferedReader
		scanString = br.readLine();
		// if the scanned string is a null object that means we have reached the end of file and we reallocate the buffered reader object for the table file
		if(scanString == null){
			br.close();
			br = null;
			fr = null;
		}

		return scanString;
	}

	// this function is used to tell if a particular attribute or column is present in the Table's column description list or not
	public boolean checkColumnNamePresentOrNot(String columnName){

		// iterate through the column definition list of the table and return true or false accordingly
		for(ColumnDefinition cd : this.columnDescriptionList){
			if(cd.getColumnName().equals(columnName))
				return true;
		}

		return false;
	}
}

/* this is the Main class for the project */
public class Main {

	// this File object is the one that points to the data directory, this directory consists of all the .dat or the .tbl files in which our data is stored
	public static File dataDirectory = null;

	// this File object is the one that points to the swap directory, this is the directory to which we can write during the course of our project execution
	public static File swapDirectory = null;

	// this File object is the one that points to the index directory, this is the directory to which we can write our indexes in the pre-computation phase
	public static File indexDirectory = null;

	// this is the ArrayList that stores the File objects corresponding to all the .sql files supplied on input
	public static ArrayList<File> sqlFileList = new ArrayList<File>();

	// this HashMap stores the (table_name, table_file_path) pairs so that the look up for the tables becomes easy
	public static HashMap<String, File> tablesNameAndFileMap = new HashMap<String, File>();

	// this boolean variable is used to check if we are in the pre-computation phase or not
	public static boolean buildPhaseFlag = false;
	
	// this HashMap stores the (table_name, table_TableObject) pairs, which is used to get the referenced Table object directly given a table's name
	public static HashMap<String, Table> tableObjectsMap = new HashMap<String, Table>();

	// this HashMap stores the (table_name, recordManager) pairs so that the look up for the ReconrdManager becomes easy
	public static HashMap<String, RecordManager> tablesNameAndRecordManager = new HashMap<String, RecordManager>();
	
	// this HashMap stores the tableName, recId pairs so as to get the Long recId for that Table
	public static HashMap<String, Long> tablesNameAndRecordId = new HashMap<String, Long>();
		
	// this HashMap stores the (table_name, ArrayLIst<Indexes>) pairs so that the mapping of the Indexes corresponding to table becomes easy
	public static HashMap<String, List> tablesNameAndIndexesMap = new HashMap<String, List>();
		
	// this HashMap stores the (table_name, PrimaryTreeMap) pairs so that the mapping of the BTree of indexes corresponding to table becomes easy
	public static HashMap<String,HashMap<String,PrimaryTreeMap<String, ArrayList<String>>>> tablesNameAndBTreeMap = new HashMap<String, HashMap<String,PrimaryTreeMap<String,ArrayList<String>>>>();

		@SuppressWarnings({ })
	public static void main(String[] args) throws IOException, InterruptedException, ParseException {
		
		long t1 = System.currentTimeMillis();
		
		// iterate over the args[] array and set the above variables
		for (int i = 0; i < args.length; ++i) {

			if (args[i].equals("--data")) {
				dataDirectory = new File(args[i + 1]);
				++i;
			} else if (args[i].equals("--swap")) {
				swapDirectory = new File(args[i + 1]);
				++i;
			} else if(args[i].equals("--build")){
				buildPhaseFlag = true;
			} else if(args[i].equals("--index")){
				indexDirectory = new File(args[i+1]);
				++i;
			} else {
				sqlFileList.add(new File(args[i]));
			}
		}

		for(File file:dataDirectory.listFiles()){
			if(file.getName().contains("|") || file.getName().contains("tbl")){
				file.delete();
			}
		}
		

		// check if the buildPhaseFlag is set to true or not, if yes then we need to carry out indexing
		if(buildPhaseFlag){
			
			// after setting all the variables populate the HashMap with (table_name, table_file_path) pairs
			for (File tableFile : dataDirectory.listFiles()) {
				// this is the actual name of the file
				String fileName = tableFile.getName().toLowerCase();
				// this is the name of the table without the suffix
				String tableName = fileName.substring(0,fileName.lastIndexOf("."));

				if (fileName.endsWith(".tbl") || fileName.endsWith(".dat")) {
					if (!tablesNameAndFileMap.containsKey(tableName)) {
						tablesNameAndFileMap.put(tableName, tableFile);
					}
				}
			}
			
			// write all the tables in swap directory
			for(File dataFile : dataDirectory.listFiles()){
				
				File datFile = new File(indexDirectory + System.getProperty("file.separator") + dataFile.getName());
				BufferedReader br = new BufferedReader(new FileReader(dataFile));
				String tuple;
				
				BufferedWriter bwr = new BufferedWriter(new FileWriter(datFile,true));
				
				while((tuple = br.readLine()) != null){
					bwr.write(tuple + "\n");
				}
				
				br.close();
				bwr.close();
			}
			
			// get the first .sql file in the list of .sql files as its the tpch_schemas file
			File schemasFile = sqlFileList.get(0);

			// use CCJSQLParser's object to parse this file
			FileReader fr = new FileReader(schemasFile);
			CCJSqlParser parser = new CCJSqlParser(fr);

			// this Statement object is used to parse the schemas file
			Statement stmtObject;
			while((stmtObject = parser.Statement()) != null){

				// check if the statement is an instance of a CreateTable statement, if it is then we need to create indexes in the indexDirectory
				if(stmtObject instanceof CreateTable){

					// get the corresponding CreateTable statement
					CreateTable ctStmt = (CreateTable) stmtObject;
					// create the index by calling this function
					BuildIndexes.buildIndex(ctStmt, indexDirectory);
				}
			}

		} else{
			
			// after setting all the variables populate the HashMap with (table_name, table_file_path) pairs
			for (File tableFile : indexDirectory.listFiles()) {
				// this is the actual name of the file
				String fileName = tableFile.getName().toLowerCase();
				// this is the name of the table without the suffix
				String tableName = fileName.substring(0,fileName.lastIndexOf("."));

				if(!fileName.contains("expected")) {
					if (fileName.endsWith(".tbl") || fileName.endsWith(".dat")) {
						if (!tablesNameAndFileMap.containsKey(tableName)) {
							tablesNameAndFileMap.put(tableName, tableFile);
						}
					}
				}
			}
			
			
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			
			//System.out.println((tablesNameAndFileMap));
			/*for(String tableName:tablesNameAndFileMap.keySet()){
				
				System.out.println(tableName);
			}*/
		//---------------------------------------------------	
			
			// This code is populating the tablesNameAndRecordManager Map which contains table name and its RecordManager
			for(String tableName:tablesNameAndFileMap.keySet()){
				RecordManager tableRecordManager = RecordManagerFactory.createRecordManager(indexDirectory + System.getProperty("file.separator") + tableName + ".index");
				tablesNameAndRecordManager.put(tableName, tableRecordManager);
				//System.out.println(tableRecordManager.toString());
			}
			
			// This code is populting the tablesNameAndRecordId Map which contains table name and RecordId of its RecordManager
			Long recId = 100L;
			for(String tableName:tablesNameAndFileMap.keySet()){
				tablesNameAndRecordManager.get(tableName).setNamedObject(tableName,recId);
				tablesNameAndRecordId.put(tableName, recId);
				recId++;
				//System.out.println(tableRecordManager.toString());
			}
			
			
			// here we get the schema file to fetch the primary and secondary indexes 
			// get the first .sql file in the list of .sql files
						File schemasFile = sqlFileList.get(0);
						
						// use CCJSQLParser's object to parse this file
						FileReader fileReader = new FileReader(schemasFile);
						CCJSqlParser parser = new CCJSqlParser(fileReader);
						
						// this Statement object is used to parse the schemas file
						Statement stmtObject;
						while((stmtObject = parser.Statement()) != null){
							
							// check if the statement is an instance of a CreateTable statement, if it is then we need to create indexes in the indexDirectory
							if(stmtObject instanceof CreateTable){
								
								// get the corresponding CreateTable statement
								CreateTable ctStmt = (CreateTable) stmtObject;
								
								// this code populates the tablesNameAndIndexesMap which contains tableName and corresponding List of Indexes
								tablesNameAndIndexesMap.put(ctStmt.getTable().getName().toLowerCase(),ctStmt.getIndexes());
							}
							
						}
						
			
						//for(Entry<String, List> etr:tablesNameAndIndexesMap.entrySet()){
						//	System.out.println(etr.getKey());
							//if(etr.getValue().size()>1)
						//	System.out.println(etr.getValue());//.contains(""));
						//}		
						
						
			// 	the following code fetches the indexes and then populates the tablesNameAndBTreeMap which contains tableName and HashMap of all of <index,BTree>
									
			for(String tableName:tablesNameAndRecordManager.keySet()){
				
				RecordManager rm = tablesNameAndRecordManager.get(tableName);
			
				HashMap<String, PrimaryTreeMap<String, ArrayList<String>>> indexAndPrimaryStoreMap = new HashMap<String, PrimaryTreeMap<String,ArrayList<String>>>();
				//System.out.println(tableName);
				//System.out.println(tablesNameAndIndexesMap);
				// iterate over the index list and build primary and secondary indexes as necessary
				for(Object ob : tablesNameAndIndexesMap.get(tableName)){
					// get the index corresponding to the object
					Index indexObject = (Index) ob;
					// get the type of index i.e PRIMARY KEY, UNIQUE or INDEX
					String indexType = indexObject.getType();
					
					
					if(indexType.equals("PRIMARY KEY")){
						// get the list of columns that form the primary key
						List indexColumns = indexObject.getColumnsNames();
						
						// form the cumulative primary key that will be used to create a PrimaryStoreMap, the name of the primary tree map would be "|" separated names of the primary keys
						String primaryKey = "";
						for(Object column : indexColumns)
							primaryKey += column.toString() + "|";
						
						// create a PrimaryStoreMap corresponding to the table for the respective columns which serve as the Primary Key
						PrimaryTreeMap<String, ArrayList<String>> primaryTreeMap = rm.treeMap(tableName + "." + primaryKey);

						// this Map is between index and PrimaryStoreMap for this table
						
						indexAndPrimaryStoreMap.put(tableName + "." + primaryKey, primaryTreeMap);
								
						//tablesNameAndBTreeMap.put(tableName,indexAndPrimaryStoreMap);
								
								
					}
					else if(indexType.equals("INDEX")){
						
						// create a PrimaryStoreMap corresponding to the table with the INDEX(column name) and column name as the key for the map
						PrimaryTreeMap<String, ArrayList<String>> primaryTreeIndexMap = rm.treeMap(tableName + "." + indexObject.getName() + ".indexkey");
						
						// this Map if between index and PrimaryStoreMap for this table
						//if(indexAndPrimaryStoreMap.size()>0){
							
						//}
						//else{
						//HashMap<String, PrimaryStoreMap<String, ArrayList<String>>> indexAndPrimaryStoreMap = new HashMap<String, PrimaryStoreMap<String,ArrayList<String>>>();
							indexAndPrimaryStoreMap.put(tableName + "." + indexObject.getName() + ".indexkey", primaryTreeIndexMap);
						//}
								
						
					}
					
					
				}
				tablesNameAndBTreeMap.put(tableName,indexAndPrimaryStoreMap);
				
				
			}
			
			
			/*for(Entry<String, HashMap<String, PrimaryStoreMap<String, ArrayList<String>>>> etr:tablesNameAndBTreeMap.entrySet()){
				System.out.println(etr.getKey());
				//if(etr.getValue().size()>1)
				System.out.println(etr.getValue().keySet());//.contains(""));
			}	*/
			
			//----------------------------------------------
			
			///////////////////////////
			
			/*//System.out.println(sqlFileList);
			 schemasFile = sqlFileList.get(1);
			fileReader = new FileReader(schemasFile);
			parser = new CCJSqlParser(fileReader);
			
			
			//Statement stmtObject;
			while((stmtObject = parser.Statement()) != null){
				
				if(stmtObject instanceof Select){
					SelectBody selectBody = ((Select)stmtObject).getSelectBody();
					
					Expression whereExpression = ((PlainSelect) selectBody).getWhere();
					
					//System.out.println(WhereOperation.extractNonJoinExp(whereExpression));
					
					WhereOperation.indexSelection(WhereOperation.extractNonJoinExp(whereExpression), tablesNameAndIndexesMap, tablesNameAndBTreeMap);
				
				}
			}
			*/
			
			////////////////////////////+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
			
			
			
			
			
			
			
			
			// start scanning the .sql files provided in the input
			for (File sqlFile : sqlFileList) {

				// this is a FileReader object used in parsing the SQL file
				FileReader fr = new FileReader(sqlFile);
				CCJSqlParser parserObject = new CCJSqlParser(fr);

				// this is the statement object returned by the CCJSqlParser when scanning the .sql file
				Statement statementObject;
				Update updateStatement = null;
				
				while ((statementObject = parserObject.Statement()) != null) {

					// check if the statement just scanned is an instance of the CreateTable statement
					if (statementObject instanceof CreateTable) {

						// make a new Table object corresponding to the CreateTable statement encountered
						CreateTable ctStmt = (CreateTable) statementObject;

						// this String object stores the name of the table, convert the name to LowerCase
						String tableName = ctStmt.getTable().getName().toLowerCase();

						// this Table is a reference to the table that is present inside the create table statement
						Table newTableObject = new Table(tableName, ctStmt.getColumnDefinitions().size(),tablesNameAndFileMap.get(tableName), indexDirectory);

						// populate the tuple list of the table
						newTableObject.populateTable();

						// set the attributes of this new table object
						newTableObject.columnDescriptionList = (ArrayList<ColumnDefinition>) ctStmt.getColumnDefinitions();

						// populate the column index map of the table object
						newTableObject.populateColumnIndexMap();

						// insert the pair of (table_name, table_TableObject) in the tableObjectsMap
						tableObjectsMap.put(tableName, newTableObject);


					}else if(statementObject instanceof Insert){
						
						// get the Insert statement object from the statement
						Insert insertStatement = (Insert)statementObject;
						
						// get the name of the table in which to insert the string
						String tableName = insertStatement.getTable().getName().toLowerCase();
						
						// get the table object corresponding to the tableName
						Table tableToInsertIn = tableObjectsMap.get(tableName);
						
						// get the list of items to be inserted in the table's list
						List insertItems = ((ExpressionList) insertStatement.getItemsList()).getExpressions();
						
						// this is the string to be inserted, this has to be a '|' separated string
						String insertString = "";
						for(Object items : insertItems){
							
							String itemString = items.toString();
							String itemToInsert;
							
							if(itemString.contains("'")){
								itemToInsert = itemString.substring(itemString.indexOf("'") + 1, itemString.lastIndexOf("'"));
							} else{
								itemToInsert = itemString;
							}
							
							insertString += itemToInsert + "|";
						}
						
						insertString = insertString.substring(0, insertString.lastIndexOf("|"));
						
						// add the string to be inserted in the table's tuple list
						tableToInsertIn.tableTuples.add(insertString);
						//File newfile = new File(tableToInsertIn.tableDataDirectoryPath + System.getProperty("file.separator") + "sample.txt");
						FileWriter fw = new FileWriter(tableToInsertIn.tableFilePath , true);
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write(insertString + "\n");
						bw.close();
						
					}  else if(statementObject instanceof Delete){
						
                        // get the Delete statement object from the statement
                        Delete deleteStatement = (Delete)statementObject;
                        
                        // get the name of the table from which to delete the tuples
                        String tableName = deleteStatement.getTable().getName().toLowerCase();
                        
                        // get the table object corresponding to the tableName
                        Table tableToDeleteFrom = tableObjectsMap.get(tableName);                      
                        
                        // get the where clause from the statement
                        Expression deleteWhereClause = deleteStatement.getWhere();
             
                        
                        // this is the string to be inserted, this has to be a '|' separated string
                        Table DeleteTable = WhereOperation.selectionOnTable(deleteWhereClause,tableToDeleteFrom);
                        
                        // we will iterate on the list of tuples in the table
                        HashSet<String> deleteTupleSet = new HashSet<>(DeleteTable.tableTuples);
                        
                        // this list consists of the tuples that need to be deleted
                        ArrayList<String> resultDeleteTupleList=new ArrayList<>();
                        
                        // iterate on the list of the 
                        for(String str: tableToDeleteFrom.tableTuples)
                        {
                        	if(deleteTupleSet.contains(str))
                        		continue;
                        	else
                        		resultDeleteTupleList.add(str);
                        }

                        tableToDeleteFrom.tableTuples = resultDeleteTupleList;
                        
                        FileWriter fw = new FileWriter(tableToDeleteFrom.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						
						for(String str:resultDeleteTupleList) {
						bw.write(str + "\n");
						}
						
						bw.close();
						
                    } else if (statementObject instanceof Update) {

                        // get the Update statement object from the statement
                        updateStatement = (Update)statementObject;
                        
                        // get the name of the table from which to update the tuples
                        String tableName = updateStatement.getTable().getName().toLowerCase();
                        
                        // get the table object corresponding to the tableName
                        Table tableToUpdateFrom = tableObjectsMap.get(tableName);                      
                        
                        // get the where clause from the statement
                        Expression updateWhereClause = updateStatement.getWhere();
                        ArrayList<Expression> expList = new ArrayList<Expression>();
                        
                        if(updateWhereClause instanceof AndExpression){
                        	expList.add(((AndExpression)updateWhereClause).getLeftExpression());
                        	expList.add(((AndExpression)updateWhereClause).getRightExpression());
                        }
                        else{                     
                        	expList.add(updateWhereClause);
                        }
                        Table updateTable = null;
                        
                        ArrayList<String> tuplesForThisTable = null;
                        /*ArrayList<String> tuplesForThisTable = */
    					
    					if(tuplesForThisTable == null){
    						
    					//	updateTable = WhereOperation.selectionOnTable(updateWhereClause,tableToUpdateFrom);
    					
    					}
    					else{
    						/*File resultantTableFile = new File(etr.getKey().tableDataDirectoryPath+System.getProperty("file.separator") + etr.getKey().tableName + "|.tbl");
    					if(!resultantTableFile.exists())
    						resultantTableFile.createNewFile();*/
    						updateTable = new Table(tableToUpdateFrom.tableName, 
    								tableToUpdateFrom.noOfColumns,
    								null,
    								tableToUpdateFrom.tableDataDirectoryPath);
    						updateTable.columnDescriptionList = tableToUpdateFrom.columnDescriptionList;
    						updateTable.columnIndexMap = tableToUpdateFrom.columnIndexMap;
    						updateTable.tableTuples = tuplesForThisTable;
    						
    					}
                        
                        List columnList = updateStatement.getColumns();
                        int columnIndex = tableToUpdateFrom.columnIndexMap.get(columnList.get(0).toString().toLowerCase());
                        String newValue = updateStatement.getExpressions().get(0).toString();
                        String type = tableToUpdateFrom.columnDescriptionList.get(tableToUpdateFrom.columnIndexMap.get(tableToUpdateFrom.columnDescriptionList.get(columnIndex).getColumnName())).getColDataType().getDataType();
                        if(type.equalsIgnoreCase("char")|| type.equalsIgnoreCase("VARCHAR")||type.equalsIgnoreCase("string")){
                        	newValue = newValue.substring(1,newValue.length()-1);
                        }
                        else if (type.equalsIgnoreCase("date")){
                        	newValue = newValue.substring(6, newValue.length() - 2);
                        }
                      /*
                        // we will iterate on the list of tuples in the table
                        HashSet<String> updateTupleSet = new HashSet<>(updateTable.tableTuples);
                        
                        ArrayList<String> tableTupleList=null;
                        
                       
	                   // this list consists of the tuples that need to be updated
	                   tableTupleList=tableToUpdateFrom.tableTuples;
                       
                        
                        // this list consists of the tuples that need to be updated
                        ArrayList<String> resultUpdateTupleList=new ArrayList<>();
                        
                        // iterate on the list of the 
                        for(String str: tableTupleList)
                        {
                        	if(updateTupleSet.contains(str))
                        		continue;
                        	else
                        		resultUpdateTupleList.add(str);
                        }
                        
                        
                        for(String tuple:tableTupleList) {
                        	
                        	if (updateTupleSet.contains(tuple)) {
                        		String[] tupleArr = tuple.split("\\|");
                        		String newTuple = "";
                        		for (int i=0;i<tupleArr.length;i++) {
                        			if (i==columnIndex ){
                        				
                        				newTuple = newTuple+newValue+"|";
                        			}
                        			else {
                        				newTuple = newTuple+tupleArr[i]+"|";
                        			}
                        			
                        		}
                        		resultUpdateTupleList.add(newTuple.substring(0, newTuple.length()-2));
                        	}
                        }
                        
                        tableToUpdateFrom.tableTuples = resultUpdateTupleList;
*/                        
                        WhereOperation.indexUpdation(expList,Main.tablesNameAndIndexesMap,Main.tablesNameAndBTreeMap,columnIndex,newValue,updateStatement.getTable().getName());
                       //System.out.println(tablesNameAndRecordId);
                       if(updateStatement != null){
                           	Long recid = Main.tablesNameAndRecordId.get(updateStatement.getTable().getName().toLowerCase());
                           	System.out.println(recid);
	           				Object newObj = Main.tablesNameAndRecordManager.get(updateStatement.getTable().getName().toLowerCase()).fetch(recid);
	           				System.out.println("hello");
	           				System.out.println(newObj.toString());
	           				Main.tablesNameAndRecordManager.get(updateStatement.getTable().getName().toLowerCase()).update(recid,newObj);
	           				Main.tablesNameAndRecordManager.get(updateStatement.getTable().getName().toLowerCase()).commit();
           				}
                        
                        
                    } else if (statementObject instanceof Select) {

						// call the selection evaluation operator after encountering the select statement
						Table resultTable = SelectionOperation.selectionEvaluation(statementObject, tableObjectsMap);
												
						Limit limit = ((PlainSelect)(((Select) statementObject).getSelectBody())).getLimit();
						
						if(limit != null){
							AggregateOperations.LimitOnTable(resultTable, (int)limit.getRowCount());
						}
						else{
							
							for(String stream:resultTable.tableTuples){
								System.out.println(stream);
							}
						}
						
						long t2 = System.currentTimeMillis();
						//System.out.println("Time take : " + ((double)(t2-t1))/1000);
						t1 = t2;
					}
					
				}
				

				
			}
		}
	}
}