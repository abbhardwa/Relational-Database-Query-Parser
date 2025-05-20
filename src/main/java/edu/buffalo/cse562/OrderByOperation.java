/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class OrderByOperation {

	public static Table orderBy(Table table, List orderByList,int size) throws IOException {
				
		/*System.out.println("============================");
		System.out.println(table.columnDescriptionList);
		System.out.println("-------------");
		System.out.println(table.columnIndexMap);
		System.out.println("--------------");
		System.out.println(table.tableTuples);
		System.out.println("===========================");*/
		
		
		// contains the order by attribute list
		String  orderByArr[] = new String[size];
		
		int i=0;
		
		
		for(Object o:orderByList){
			orderByArr[i] = o.toString();
			i++;
		}
		
		String type[] = new String[size];
		
		
		ArrayList<String> array = new ArrayList<String>();
		
			String tuple = null;
			
			int []index = new int[orderByArr.length];
			
			boolean []order = new boolean[orderByArr.length];
			for (int j = 0; j < orderByArr.length; j++) {
				String column = orderByArr[j];
				String colArr[]=column.split(" ");
			
				//System.out.println(table.columnIndexMap.get(colArr[0].trim()));
				
					//System.out.println(colArr[0]);
					//System.out.println(table.columnIndexMap);
					index[j] =  table.columnIndexMap.get(colArr[0]);
					type[j] = table.columnDescriptionList.get(table.columnIndexMap.get(table.columnDescriptionList.get(index[j]).getColumnName())).getColDataType().getDataType();
					if(colArr.length>1){
						if(colArr[1].equalsIgnoreCase("desc")){

							order[j] = true;
						}
					}
				
			}
			
			/*for(String t:type){
				System.out.println("Type-->"+t);
			}*/
			
			for(String tuple1: table.tableTuples){
				
				String str[] = tuple1.split("\\|");
				array.add(tuple1);
 
			}

			Comparator comp = null;
			int cout = 0;
			for (int j = index.length-1; j >= 0; j--) {
				cout++;
			
			
			comp = new AscendingCompare(index[j],order[j],type[j]);
			
			
			Collections.sort(array,comp);

			}
			

		
			
				// this Table contains the resultant table after applying Selection Operation
				Table resultantTable = new Table(table.tableName+"OrderBy",table.noOfColumns,null, table.tableDataDirectoryPath);
				
				resultantTable.columnDescriptionList = table.columnDescriptionList;
				resultantTable.columnIndexMap = table.columnIndexMap;
		
				
				/*for (int j = 0; j < array.size(); j++) {
				
					bwr.write(array.get(j));
					bwr.write("\n");
					bwr.flush();
				}
			*/
			/*for(String s:array){
				System.out.println(s);
			}
			System.out.println("-----------");*/
		
			resultantTable.tableTuples=	array;
		return resultantTable;
	}
		
	
}


class AscendingCompare implements Comparator<String>{
	
	int index = 0;
	boolean order = false;
	String type = null;

	AscendingCompare(int index,boolean order,String type){
		this.index = index;
		this.order = order;
		this.type = type;
	//	System.out.println(type);
	}
	
	
	@Override
	public int compare(String o1, String o2) {
		String s1[] = o1.split("\\|");
		String s2[] = o2.split("\\|");
		
		if(type.equalsIgnoreCase("char")|| type.equalsIgnoreCase("VARCHAR")||type.equalsIgnoreCase("string")){
		//System.out.println("in");
		
			if(s1[index].equals(s2[index])){
					return 0;
			}
			
			else {
				int x =0;
				if(order==false){
					x = s1[index].compareTo(s2[index]); 
					
				}
				else{
					x = s2[index].compareTo(s1[index]);
				}
				
				return x;
			}
		
		}
		else if(type.equalsIgnoreCase("int")|| type.equalsIgnoreCase("decimal")){
			//System.out.println("in ");
			if(Double.parseDouble(s1[index].trim())==Double.parseDouble(s2[index].trim())){
				return 0;
			}
			
			else {
				int x =0;
				if(order==false){
					
			//		System.out.println(Double.parseDouble(s1[index].trim())+"---"+Double.parseDouble(s2[index].trim()));
					if(Double.parseDouble(s1[index].trim())>Double.parseDouble(s2[index].trim())){
						x = 1;
						
					}
					else if(Double.parseDouble(s1[index].trim())<Double.parseDouble(s2[index].trim())){
						x=-1;
					}
					else{
						x=0;
					}
					
				}
				else{
					//System.out.println(s1[index]);
					
					if(Double.parseDouble(s1[index].trim())>Double.parseDouble(s2[index].trim())){
						x = -1;
					}
					else if(Double.parseDouble(s1[index].trim())<Double.parseDouble(s2[index].trim())){
						x=1;
					}
					else{
						x=0;
					}
					
				}
				
				return x;
			}
		}
	
		return 0;
		
	}
}
