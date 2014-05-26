package edu.buffalo.cse562;

import java.util.Collections;
import java.util.List;
import java.util.HashSet;
import java.util.Iterator;
import java.io.FileWriter;
import java.util.ArrayList;
import java.io.IOException;
import java.math.BigDecimal;
import java.io.BufferedWriter;
import java.util.LinkedHashMap;

import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class AggregateOperations 
{

	//Function to calculate the Group BY Clause and store the values in a Hash Map

	public LinkedHashMap<String,Object> getGroupBy(Table newTable,String groupBy,String columnName,int columnNo,String aggregateFunc) throws IOException
	{

		double columnAdd=0;
columnName=columnName.toLowerCase();
		int flag=0;
		//Creating a LinkedHashMap to Store Group<Key,Value> Pair
		LinkedHashMap<String, Object> groupByMap=new LinkedHashMap<String,Object>();
		LinkedHashMap<String,String> countMap=new LinkedHashMap<String,String>();

		String editedGroupBy=groupBy.substring(1,groupBy.lastIndexOf("]"));

		String[] checkGroupBy=editedGroupBy.split(",");
		String[] groupBycolumnName = new String[10];

		for(int i=0;i<checkGroupBy.length;i++)
		{
			groupBycolumnName[i]=checkGroupBy[i].toLowerCase();
		}

		ArrayList<String>canString=newTable.tableTuples;

		for(String readTuple:canString)
		{
			String MatchedColumn=new String();

			String[] tupleList=readTuple.split("\\|");	
			if(checkGroupBy.length==1)
			{
				MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].toLowerCase().trim())];
			}
			else	

			{	
				MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].trim())];

				for(int i=1;i<checkGroupBy.length;i++)
				{
					MatchedColumn=MatchedColumn+"|"+tupleList[newTable.columnIndexMap.get(groupBycolumnName[i].trim())];

				}

			}

			//Column inside the Group By Clause

			//Checks whether the function is SUM"
			if(aggregateFunc.trim().contains("SUM")||aggregateFunc.trim().contains("sum"))
			{	

				columnAdd=(Double.parseDouble(tupleList[(newTable.columnIndexMap.get(columnName))]));
				if(groupByMap.containsKey(MatchedColumn)){ 
					groupByMap.put(MatchedColumn.trim(),Double.parseDouble(groupByMap.get(MatchedColumn).toString())+columnAdd);
				}
				else{
					groupByMap.put(MatchedColumn.trim(),columnAdd);
				}
			}

			//check whether the aggregate function is MIN
			else if(aggregateFunc.trim().contains("MIN")||aggregateFunc.trim().contains("min"))
			{
				columnAdd=Double.parseDouble(tupleList[(newTable.columnIndexMap.get(columnName))]);
				double comp=columnAdd;

				if(groupByMap.containsKey(MatchedColumn))
				{
					if(Double.parseDouble(groupByMap.get(MatchedColumn).toString())>comp)
					{
						groupByMap.put(MatchedColumn.trim(),comp);
					}
				}
				else 
				{
					groupByMap.put(MatchedColumn.trim(),comp);
				}
			}

			//check whether the aggregate function is MAX

			else if(aggregateFunc.trim().contains("MAX")||aggregateFunc.trim().contains("max"))
			{
				columnAdd=Double.parseDouble(tupleList[(newTable.columnIndexMap.get(columnName))]);
				double comp=columnAdd;
				if(groupByMap.containsKey(MatchedColumn))
				{
					if(Double.parseDouble(groupByMap.get(MatchedColumn).toString())<comp)
					{
						groupByMap.put(MatchedColumn.trim(),comp);
					}
				}
				else 
				{
					groupByMap.put(MatchedColumn.trim(),comp);
				}
			}
			else if(aggregateFunc.trim().contains("COUNT")||aggregateFunc.trim().contains("count")||aggregateFunc.trim().contains("distinct")||aggregateFunc.trim().contains("DISTINCT"))
			{
				if(aggregateFunc.trim().contains("distinct")||aggregateFunc.trim().contains("DISTINCT"))
				{

					flag=1;
					if(!columnName.contains("."))
					{
						columnName="LineItem"+"."+columnName;
					}
					 
                     String column=tupleList[(newTable.columnIndexMap.get(columnName.toLowerCase()))];
					if(countMap.containsKey(MatchedColumn))
					{

						countMap.put(MatchedColumn.trim(),(countMap.get(MatchedColumn)+"|"+column).trim());
					}
					else
					{

						countMap.put(MatchedColumn.trim(),column.trim());
					}
				}
				else
				{

					if(groupByMap.containsKey(MatchedColumn))
					{
						groupByMap.put(MatchedColumn.trim(),Integer.parseInt(groupByMap.get(MatchedColumn.trim()).toString())+1);


					}
					else 
					{
						groupByMap.put(MatchedColumn.trim(),1);


					}
				}
			}
		}


		if(flag==1)
		{
			Iterator iter=countMap.keySet().iterator();

			while(iter.hasNext())
			{
				HashSet<String> DupSet=new HashSet<String>();
				String key =iter.next().toString();  
				String value =countMap.get(key).toString(); 
				String[] StrArr=value.split("\\|");
				for(String s:StrArr)
				{
					DupSet.add(s);
				}
				groupByMap.put(key.trim(),(DupSet.size()));
			}

		}
		return groupByMap;
	}


	//Function to calculate the Group BY Clause and store the values in a Hash Map

	public LinkedHashMap<String,Object> getPostCal(Table newTable,String groupBy,String aggregateFunc,String posixExpression) throws IOException, ParseException
	{

		//Creating a LinkedHashMap to Store Group<Key,Value> Pair
		LinkedHashMap<String, Object> groupByMap=new LinkedHashMap<String,Object>();


		String MatchedColumn=new String();

		//String array to store all the values in the column
		String arr[]=new String[10];
		String[] tupleList=null;
		//postfix contains a list of all columns in a post fix form
		ArrayList<String> arrList = WhereOperation.braceExp(posixExpression);
		String array[] = new String[arrList.size()];
		array = arrList.toArray(array);
		ArrayList<String> readTupleList=new ArrayList<String>();

		//String[] postFix=SelectionOperation.convertToPos(posixExpression.split(" "));
		String[] postFix=WhereOperation.convertToPos(array);

		String editedGroupBy=groupBy.substring(1,groupBy.lastIndexOf("]"));

		String[] checkGroupBy=editedGroupBy.split(",");
		String[] groupBycolumnName = new String[10];

		for(int i=0;i<checkGroupBy.length;i++)
		{
			groupBycolumnName[i]=checkGroupBy[i];
		}

		readTupleList=newTable.tableTuples;
		for(String canString :readTupleList)
		{ 
			tupleList=canString.split("\\|");

			if(checkGroupBy.length==1)
			{
				MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].trim())];
			}
			else
			{					
				MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].trim())];

				for(int i=1;i<checkGroupBy.length;i++)
				{
					MatchedColumn=MatchedColumn+"|"+tupleList[newTable.columnIndexMap.get(groupBycolumnName[i].trim())];

				}
			}

			for(int k=0;k<postFix.length;k++)
			{
				if(newTable.columnIndexMap.containsKey(postFix[k].trim()))
				{
					arr[k]=tupleList[newTable.columnIndexMap.get(postFix[k].trim())];
				}
				else
				{
					arr[k]=postFix[k].trim();
				}
			}


			if(aggregateFunc.equalsIgnoreCase("SUM"))
			{
				if(groupByMap.containsKey(MatchedColumn.trim()))	
				{
					double result=(WhereOperation.evaluate(arr));
					groupByMap.put(MatchedColumn.trim(),Double.parseDouble(groupByMap.get(MatchedColumn.trim()).toString())+result);

				}
				else
				{
					double result=WhereOperation.evaluate(arr);
					groupByMap.put(MatchedColumn.trim(),(double)result);
				}


			}


		}
		return groupByMap;
	}

	public double sum(Table newTable,String[] selectList,String columnName) throws IOException
	{

		double sum=0;
		String[] pos=columnName.split(" ");
		String[] postFix=WhereOperation.convertToPos(pos);
		String[] arr=new String[postFix.length];
		String[] tupleList=null;
		ArrayList<String> readTupleList=newTable.tableTuples;
		sum=0;
		readTupleList=newTable.tableTuples;

		for(String canString:readTupleList)
		{	tupleList=canString.split("\\|");

		for(int k=0;k<postFix.length;k++)
		{
			if(newTable.columnIndexMap.containsKey(postFix[k]))
			{
				arr[k]=tupleList[newTable.columnIndexMap.get(postFix[k])];
			}
			else
			{
				arr[k]=postFix[k];
			}
		}

		double val=WhereOperation.evaluate(arr);
		sum=sum+val;
		}
		return sum;
	}

	//Aggregate Function to get the Sum
	//Returns a table containing the column names(Group BY ones) and the resultant aggregate Function

	@SuppressWarnings("resource")
	public  Table getAggregate(Table newTable,String[] selectList,String check,List orderByList) throws IOException, ParseException, InterruptedException
	{	
		Table newTable1=null;
		Table ProjectedTable=null;
		
		String[] tupleList=null;
		LinkedHashMap<String,Object> map;
		ArrayList<LinkedHashMap<String ,Object>> globalList=new ArrayList<LinkedHashMap<String,Object>>();
		int count=0;
		Double Sum=0.0;
		Table GroupByTable=null;
		String columnName=selectList[0];
		ArrayList<String> arrListResult=new ArrayList<String>();
		ArrayList<String> ListProjected=new ArrayList<String>();

		ArrayList<String> readTupleList=new ArrayList<String>();
		String editedGroupBy=null;
		String[] groupBy=null;

		if(!check.contains("NOGroupBy"))
		{
			editedGroupBy=check.substring(1,check.lastIndexOf("]"));
			groupBy=editedGroupBy.split(",");	
		}
		else
		{
			groupBy=check.split(",");	
		}

		for (int i = 0; i < groupBy.length; i++) {
			groupBy[i] = groupBy[i].trim();
		}

		for (int i = 0; i < selectList.length; i++) {
			selectList[i] = selectList[i].trim();
		}

		ArrayList<Object> arrTuple = null;
		String str =null;
		arrTuple=new ArrayList<Object>();
		double Sum1=0;
		LinkedHashMap<String,Object> postMap;
		String line=null;

		String arrStr=new String();

		int columnNo=0;
		Double min=0.0;			
		Double max=0.0;


		// check if the group by file exists or not, if not then create it
		

		// this is the groupBy table corresponding to the group by file created above
		GroupByTable = new Table(newTable.tableName + "Group", newTable.noOfColumns, null,newTable.tableDataDirectoryPath);	

		//System.out.println(newTable.columnIndexMap);
		if(!groupBy[0].contains("NOGroupBy"))
		{
			for(int i=0;i<groupBy.length;i++){

				ColumnDefinition colDescTypePair=new ColumnDefinition();
				ColDataType col=new ColDataType();
				colDescTypePair.setColumnName(groupBy[i]);
				String type=newTable.columnDescriptionList.get(newTable.columnIndexMap.get(groupBy[i].toLowerCase())).getColDataType().getDataType().toString();

				col.setDataType(type);
				colDescTypePair.setColDataType(col);
				GroupByTable.columnDescriptionList.add(colDescTypePair);
			}
		}
		
		//Checks if the groupBycolumn is null
		if(groupBy[0].contains("NOGroupBy")){

			for(int i=0;i<selectList.length;i++)
			{	
				if(selectList[i].contains("SUM")||selectList[i].contains("AVG")||selectList[i].contains("sum")||
						selectList[i].contains("avg")||selectList[i].contains("min")||selectList[i].contains("max")
						||selectList[i].contains("COUNT")|| selectList[i].contains("count")||selectList[i].contains("MIN")
						||selectList[i].contains("MAX")){

					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
				}	

				if(columnName.trim().contains("*")&&(selectList[i].contains("SUM")||selectList[i].contains("sum"))||columnName.trim().contains("-")||columnName.contains("+")||columnName.contains("/"))
				{

					Sum1=sum(newTable,selectList,columnName);
					arrStr=arrStr+Sum1+"|";
					String FinalSum=arrStr.substring(0,arrStr.lastIndexOf("|"));

					System.out.println(FinalSum);
				}

				if(selectList[i].contains("SUM")||selectList[i].contains("sum")&&
						!columnName.contains("*")&&!columnName.contains("-")&&!columnName.contains("+"))
				{

					columnNo=newTable.columnIndexMap.get(columnName.trim());

					readTupleList=GroupByTable.tableTuples;
					for(String canString:readTupleList)
					{							

						tupleList=canString.split("\\|");
						Sum=Sum+Double.parseDouble(tupleList[columnNo]);
						canString=newTable.returnTuple();

					}
					
				}

				if(selectList[i].contains("COUNT(")||selectList[i].contains("COUNT(*)")||selectList[i].contains("AVG(")||selectList[i].contains("count(")||selectList[i].contains("count(*)")||selectList[i].contains("avg("))
				{

					readTupleList=newTable.tableTuples;
					//columnNo=newTable.columnIndexMap.get(columnName.trim());

					
					Integer count1=0;
					for(String canString:readTupleList)

					{
						count1++;

					}
					System.out.println(count1);

				}

				if(selectList[i].contains("AVG")||selectList[i].contains("avg")){

					Float avg=((float)Sum1/(float)count);
					

				}

				if(selectList[i].contains("MIN")||selectList[i].contains("min"))
				{
					readTupleList=GroupByTable.tableTuples;
					tupleList=readTupleList.get(0).split("\\|");
					columnNo=newTable.columnIndexMap.get(columnName);
					min=Double.parseDouble(tupleList[columnNo]);

					for(String canString:readTupleList)
					{			

						tupleList=canString.split("\\|");

						if(min>=Double.parseDouble(tupleList[columnNo])){

							min=Double.parseDouble(tupleList[columnNo]);
						}


					}
					
				}


				if(selectList[i].contains("MAX")||selectList[i].contains("max"))
				{

					readTupleList=GroupByTable.tableTuples;
					tupleList=readTupleList.get(0).split("\\|");
					columnNo=newTable.columnIndexMap.get(columnName);
					max=Double.parseDouble(tupleList[columnNo]);

					for(String canString:readTupleList)					
					{			
						tupleList=canString.split("\\|");

						if(max<=Double.parseDouble(tupleList[columnNo]))
						{
							max=Double.parseDouble(tupleList[columnNo]);

						}
					}
					
				}
			}
			return GroupByTable;

		}

		//checks for the group by items. enters if there is a group By clause
		else if(!check.equalsIgnoreCase("NOGroupBy")){	

			ArrayList<String> arrList=new ArrayList<String>();

			StringBuilder sb = new StringBuilder("");

			for(int i=0;i<selectList.length;i++)
			{	


				arrList.add((selectList[i]));

				if(selectList[i].contains("SUM")||selectList[i].contains("sum"))
				{	
					//System.out.println("getAggregate in SUM function");
					if(selectList[i].contains("AS")||selectList[i].contains("as"))
					{
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);

					}
					else
					{
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(selectList[i]);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);

					}

					// checks if there is any calculation needed inside the Sum Function
					if(columnName.contains("*")||columnName.contains("-")||columnName.contains("+")||columnName.contains("/")||columnName.contains("("))
					{
						postMap=getPostCal(newTable,check,"SUM",columnName);
						globalList.add(postMap);
					}

					else if(!columnName.contains("*")||columnName.contains("-")||columnName.contains("+")
							||columnName.contains("/")){  
						//Storing the aggregate function

						columnNo=newTable.columnIndexMap.get(columnName);
						String aggString=selectList[i].substring(0,selectList[i].indexOf("("));
						map=getGroupBy(newTable,check,columnName, columnNo, aggString);
						globalList.add(map);

					}
				}

				if(selectList[i].contains("COUNT(")||selectList[i].contains("count(")){	

					LinkedHashMap<String,Object> CountMap=new LinkedHashMap<String, Object>();

					if(selectList[i].contains(" AS ")||selectList[i].contains(" as "))
					{
						if(selectList[i].contains("DISTINCT")||selectList[i].contains("distinct"))
						{
							str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
							columnName=selectList[i].substring(selectList[i].indexOf(" ")+1,selectList[i].lastIndexOf(")"));
							ColumnDefinition colDescTypePair=new ColumnDefinition();
							ColDataType col=new ColDataType();
							colDescTypePair.setColumnName(str);
							col.setDataType("DECIMAL");
							colDescTypePair.setColDataType(col);
							GroupByTable.columnDescriptionList.add(colDescTypePair);

						}
						else
						{
							str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
							columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
							ColumnDefinition colDescTypePair=new ColumnDefinition();
							ColDataType col=new ColDataType();
							colDescTypePair.setColumnName(str);
							col.setDataType("DECIMAL");
							colDescTypePair.setColDataType(col);
							GroupByTable.columnDescriptionList.add(colDescTypePair);
							if(columnName.equals("*"));
							{
								columnName=selectList[0];
							}
							columnNo=newTable.columnIndexMap.get(columnName.trim());
						}
					}
					else
					{
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						if(columnName.contains("DISTINCT")||columnName.contains("distinct"))
						{

							columnName=columnName.substring(columnName.indexOf(" "),columnName.length());

						}
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(selectList[i]);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);
						if(columnName.equals("*"))
						{				
							columnName=selectList[0];
						}
						columnNo=newTable.columnIndexMap.get(columnName.trim().toLowerCase());
					}
					if(selectList[i].contains("distinct")||selectList[i].contains("DISTINCT"))
					{
						//System.out.println("column Name="+columnName);
						CountMap=getGroupBy(newTable,check, columnName.trim(), columnNo,"distinct");
					}
					else
					{
						CountMap=getGroupBy(newTable,check, columnName, columnNo,"COUNT");
					}

					globalList.add(CountMap);
				}

				if(selectList[i].contains("AVG")||selectList[i].contains("avg")){

					if(selectList[i].contains("AS")||selectList[i].contains("as")){
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						columnNo=newTable.columnIndexMap.get(columnName.trim());
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);
					}else{

						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						columnNo=newTable.columnIndexMap.get(columnName.trim());
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(columnName.trim());
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);
					}

					LinkedHashMap<String, Object> SumMap=getGroupBy(newTable, check, columnName, columnNo,"SUM");
					LinkedHashMap<String, Object> countMap=getGroupBy(newTable, check, columnName, columnNo,"COUNT");

					LinkedHashMap<String,Object> avgMap=new LinkedHashMap<String,Object>();

					@SuppressWarnings("rawtypes")
					Iterator AvgIterator = SumMap.keySet().iterator();  

					while (AvgIterator.hasNext()) 
					{  
						String key = AvgIterator.next().toString();  
						Double value =(Double.parseDouble(SumMap.get(key).toString())/Integer.parseInt(countMap.get(key).toString()));
						avgMap.put(key, value);

					}

					globalList.add(avgMap);

				}

				if(selectList[i].contains("MIN")||selectList[i].contains("min"))
				{

					if(selectList[i].contains("AS")||selectList[i].contains("as"))
					{
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}
					else
					{
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(columnName.trim());
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}

					LinkedHashMap<String,Object> MinMap=getGroupBy(newTable,check, columnName, columnNo,"MIN");
					globalList.add(MinMap);



				}

				if(selectList[i].contains("MAX")||selectList[i].contains("max"))
				{
					if(selectList[i].contains("AS")||selectList[i].contains("as"))
					{
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}
					else
					{
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
						ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(columnName.trim());
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
						GroupByTable.columnDescriptionList.add(colDescTypePair);
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}

					LinkedHashMap<String,Object> MaxMap=getGroupBy(newTable,check, columnName, columnNo,"MAX");
					globalList.add(MaxMap);
				}

			}

			GroupByTable.populateColumnIndexMap();

			ArrayList<String> arrnew=new ArrayList<String>();

			for(String s:arrList)
			{
				if(s.contains(" AS ")||s.contains(" as "))
				{
					arrnew.add(s.split(" ")[s.split(" ").length-1]);
				}
				else
				{
					arrnew.add(s);
				}
			}

			//System.out.println(globalList.get(0));
			LinkedHashMap<String,Object> resultLinkedHashMap;
			Iterator resultIter;

			int size=globalList.size();
			String result=null;

			resultLinkedHashMap=globalList.get(0);
			resultIter=resultLinkedHashMap.keySet().iterator();
			while(resultIter.hasNext())
			{
				String key=resultIter.next().toString().trim();
				result=new String();
				for(int i=0;i<size;i++)
				{
					if(i==0)
					{
						result=key+"|"+globalList.get(i).get(key.trim())+"|";
					}
					else
					{
						result=result+"|"+globalList.get(i).get(key.trim())+"|";

					}
				}
				if(result.trim().charAt(result.length() - 1)=='|')

				{
					result= result.substring(0, result.length() - 1);
				}
				arrListResult.add(result);
			}



			/**** ABHINAV WORKAROUND FOR TPCH-7 ****/
			GroupByTable.tableTuples = arrListResult;
			ProjectedTable= ProjectTableOperation.projectTable(GroupByTable,arrnew,true);

		
			if(orderByList != null){
				//Table sortedTable= ExternalSort.performExternalMergeSort(newTable1, orderByList);

				Table sortedTable=OrderByOperation.orderBy(ProjectedTable, orderByList,orderByList.size());
				/*for(String str1:sortedTable.tableTuples)
				{
					System.out.println(str1);
				}*/
				
				return sortedTable;
			}

		}	

		

		return ProjectedTable ;
	}

	public static void LimitOnTable(Table tableToApplyLimitOn, int tupleLimit) throws IOException {


		// this is the limit file object

		// this is the Table object corresponding to the file that consists of the limited number of tuples
		for(int i=0;i<tupleLimit && i < tableToApplyLimitOn.tableTuples.size() ;++i)
		{

			System.out.println(tableToApplyLimitOn.tableTuples.get(i));

		}

		//bwr.close();

		//return limitTable;

	}

}
