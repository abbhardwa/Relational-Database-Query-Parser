/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562;

import java.util.HashMap;
import java.io.IOException;
import java.util.ArrayList;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class HashJoin {

	// this function is used to evaluate the hash join of the tables based on the joining attribute, the return type of the function is the joined table's corresponding Table object
	public static Table evaluateJoin(Table t1, Table t2, String t1Name, String t2Name, String joiningAttribute) throws IOException {
		
		// this is the Table object corresponding to the table obtained by joining t1 and t2 on the joining attribute
		Table joinedTable = new Table(t1.tableName+"|"+t2.tableName,t1.noOfColumns+t2.noOfColumns,null,t1.tableDataDirectoryPath);
		
		// form the column description list of the joined table by iterating over the column definition lists of t1 and t2
		ArrayList<ColumnDefinition> joinedTableColumnDefinitionList = new ArrayList<ColumnDefinition>();
		
		for(ColumnDefinition cd : t1.columnDescriptionList){
			ColumnDefinition temp = new ColumnDefinition();
			temp.setColumnName(cd.getColumnName());
			temp.setColDataType(cd.getColDataType());
			joinedTableColumnDefinitionList.add(temp);
		}
		for(ColumnDefinition cd : t2.columnDescriptionList){
			ColumnDefinition temp = new ColumnDefinition();
			temp.setColumnName(cd.getColumnName());
			temp.setColDataType(cd.getColDataType());
			joinedTableColumnDefinitionList.add(temp);
		}
		
		joinedTable.columnDescriptionList = joinedTableColumnDefinitionList;
		
		// populate the column index map of the joined table
		joinedTable.populateColumnIndexMap();
		
		// get the indexes of the joining attribute in table1 and table2 to index the tuples in the correct manner
		int joiningAttributeIndexTable1 = t1.columnIndexMap.get(t1Name + "." + joiningAttribute.toLowerCase());
		int joiningAttributeIndexTable2 = t2.columnIndexMap.get(t2Name + "." + joiningAttribute.toLowerCase()); 
		
		// this HashMap< String, ArrayList<String> > is interpreted as follows, HashMap<joinAttribute, List of Strings that contain that attribute>
		HashMap<String, ArrayList<String> > hashJoinTable = new HashMap<String, ArrayList<String>>();
			
		// if the size of Table1 is greater than the size of Table2 then we store the Table2 in the HashMap
		if(t1.tableTuples.size() > t2.tableTuples.size()){
			
			// extract the list of tuples of Table t2
			ArrayList<String> table2TupleList = t2.tableTuples;
			// form the HashMap by scanning all the strings in t2
			for(String tupleString : table2TupleList){
				
				// this array stores the different attribute values that form the tuple
				String[] tupleComponents = tupleString.split("\\|");
				
				if(hashJoinTable.containsKey(tupleComponents[joiningAttributeIndexTable2]))
					hashJoinTable.get(tupleComponents[joiningAttributeIndexTable2]).add(tupleString);
				else{
					// make a new ArrayList of String objects which would hold the tuples that have same value for the key attribute
					ArrayList<String> newTupleStringsList = new ArrayList<String>();
					newTupleStringsList.add(tupleString);
					hashJoinTable.put(tupleComponents[joiningAttributeIndexTable2], newTupleStringsList);
				}
			}
				
			// extract the list of tuples of Table t1
			ArrayList<String> table1TupleList = t1.tableTuples;
			// probe the HashMap formed from table2's tuples with the tuples of table t1
			for(String tupleString : table1TupleList){
				
				// this array stores the different attribute values that form the tuple
				String[] tupleComponents = tupleString.split("\\|");
					
				// now probe the hash table to form the join
				if( hashJoinTable.containsKey( tupleComponents[joiningAttributeIndexTable1] ) ){
					
					// get the list of strings to join the tuple with
					ArrayList<String> joiningTuples = hashJoinTable.get( tupleComponents[joiningAttributeIndexTable1] );
					
					// perform the join operation
					for(String joinString : joiningTuples){
						// this is the code that is implemented for pipe separation
						if(tupleString.charAt(tupleString.length() - 1) == '|'){
							joinedTable.tableTuples.add(tupleString + joinString);
						} else {
							joinedTable.tableTuples.add(tupleString + "|" + joinString);
						}
					}
					
				}
			}
		} else{
			
			// extract the list of tuples of Table t1
			ArrayList<String> table1TupleList = t1.tableTuples;
			// form the HashMap by scanning all the strings in t1
			for(String tupleString : table1TupleList){
				
				// this array stores the different attribute values that form the tuple
				String[] tupleComponents = tupleString.split("\\|");
				
				if(hashJoinTable.containsKey(tupleComponents[joiningAttributeIndexTable1]))
					hashJoinTable.get(tupleComponents[joiningAttributeIndexTable1]).add(tupleString);
				else{
					// make a new ArrayList of String objects which would hold the tuples that have same value for the key attribute
					ArrayList<String> newTupleStringsList = new ArrayList<String>();
					newTupleStringsList.add(tupleString);
					hashJoinTable.put(tupleComponents[joiningAttributeIndexTable1], newTupleStringsList);
				}
			}
				
			// extract the list of tuples of Table t1
			ArrayList<String> table2TupleList = t2.tableTuples;
			// probe the HashMap formed from table2's tuples with the tuples of table t1
			for(String tupleString : table2TupleList){
				
				// this array stores the different attribute values that form the tuple
				String[] tupleComponents = tupleString.split("\\|");
					
				// now probe the hash table to form the join
				if( hashJoinTable.containsKey( tupleComponents[joiningAttributeIndexTable2] ) ){
					
					// get the list of strings to join the tuple with
					ArrayList<String> joiningTuples = hashJoinTable.get( tupleComponents[joiningAttributeIndexTable2] );
					
					// perform the join operation
					for(String joinString : joiningTuples){
						// this is the code that is implemented for pipe separation
						if(tupleString.charAt(tupleString.length() - 1) == '|'){
							joinedTable.tableTuples.add(joinString + tupleString);
						} else {
							joinedTable.tableTuples.add(joinString + "|" + tupleString);
						}
					}
					
				}
			}
		}
	
	return joinedTable;
	}
}