package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class BuildIndexes {
	
	// this function will be used to build the index for a specific table
	public static void buildIndex(CreateTable ctStmt, File indexDirectory) throws IOException{
		
		// first get the name of the Table that we are indexing
		String tableName = ctStmt.getTable().getName().toLowerCase();
		// get the data file for the table name above
		File tableDataFile = Main.tablesNameAndFileMap.get(tableName);
		// store all the tuples in this file in the ArrayList
		ArrayList<String> tableTuples = new ArrayList<>();
		
		// read all the tuples from the table's data file and store them in the ArrayList above
		FileReader fr = new FileReader(tableDataFile);
		BufferedReader br = new BufferedReader(fr);
		String readString;
		
		while((readString = br.readLine()) != null)
			tableTuples.add(readString);
		br.close();
		
		// this is the column index map for the table
		int position = 0;
		HashMap<String, Integer> columnIndexMap = new HashMap<>();
		for(Object cd : ctStmt.getColumnDefinitions()){
			columnIndexMap.put(((ColumnDefinition)cd).getColumnName().toLowerCase(), position++);
		}
		
		// this is the RecordManager object corresponding to the table
		RecordManager tableRecordManager = RecordManagerFactory.createRecordManager(indexDirectory + System.getProperty("file.separator") + tableName + ".index");
		
		// after having all the tuples in the ArrayList get the list of indexes that need to be built on the table
		List tableIndexList = ctStmt.getIndexes();
		
		// iterate over the index list and build primary and secondary indexes as necessary
		for(Object ob : tableIndexList){
			// get the index corresponding to the object
			Index indexObject = (Index) ob;
			// get the type of index i.e PRIMARY KEY, UNIQUE or INDEX
			String indexType = indexObject.getType();
			
			if(indexType.equals("PRIMARY KEY")){
				// get the list of columns that form the primary key
				List indexColumns = indexObject.getColumnsNames();
				
				// form the cumulative primary key that will be used to create a PrimaryTreeMap, the name of the primary tree map would be "|" separated names of the primary keys
				String primaryKey = "";
				for(Object column : indexColumns)
					primaryKey += column.toString() + "|";
				
				// create a PrimaryTreeMap corresponding to the table for the respective columns which serve as the Primary Key
				PrimaryTreeMap<String, ArrayList<String>> primaryTreeMap = tableRecordManager.treeMap(tableName + "." + primaryKey);
				
				// this count is used for determining the point when we commit the transaction of writing to the index
				int tupleCount = 0;
				
				// scan the ArrayList of tuples and populate the map
				for(String tuple : tableTuples){

					// increment the tuple count
					++tupleCount;
					
					// split the tuple
					String[] splitTuple = tuple.split("\\|");
					
					// this is the primary key of the splitted tuple
					String splitTuplePrimaryKey = "";
					for(Object column :  indexColumns){
						splitTuplePrimaryKey += splitTuple[columnIndexMap.get(column.toString())] + "|";
					}
					
					if(primaryTreeMap.containsKey(splitTuplePrimaryKey)){
						primaryTreeMap.get(splitTuplePrimaryKey).add(tuple);
					} else{
						// create a new ArrayList corresponding to the key
						ArrayList<String> newList = new ArrayList<>();
						newList.add(tuple);
						primaryTreeMap.put(splitTuplePrimaryKey, newList);
					}
					
					// commit the transaction once the number of tuples reaches 1000
					if(tupleCount == 1000)
						tableRecordManager.commit();
				}
				
				// commit all the undone changes
				tableRecordManager.commit();
				
			} else if(indexType.equals("INDEX")){
				
				// create a PrimaryTreeMap corresponding to the table with the INDEX(column name) and column name as the key for the map
				PrimaryTreeMap<String, ArrayList<String>> primaryTreeIndexMap = tableRecordManager.treeMap(tableName + "." + indexObject.getName() + ".indexkey");
				
				// this is a count that is used in dumping the data out to the map
				int tupleCount = 0;
				
				// iterate on the tuples of the table's tuple list
				for(String tuple : tableTuples){
					
					// increment the tupleCount
					++tupleCount;
					
					// split the tuple on the basis of "|" delimiter
					String[] splitTuple = tuple.split("\\|");
					
					// this is the key to be inserted in the map
					String key = splitTuple[columnIndexMap.get(indexObject.getColumnsNames().get(0).toString())];
					
					if(primaryTreeIndexMap.containsKey(key)){
						primaryTreeIndexMap.get(key).add(tuple);
					} else{
						// create a new ArrayList corresponding to the key
						ArrayList<String> newList = new ArrayList<>();
						newList.add(tuple);
						primaryTreeIndexMap.put(key, newList);
					}
					
					// commit the transaction once the number of tuples reaches 1000
					if(tupleCount == 1000)
						tableRecordManager.commit();
				}
				
				// commit all undone changes
				tableRecordManager.commit();
				
			} else{
				
			}
			
		}
	}
}
