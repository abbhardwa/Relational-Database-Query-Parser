/*
 * Copyright (c) 2023 Abhinav Bhardwaj
 */

package edu.buffalo.cse562;

import java.io.IOException;
import java.util.ArrayList;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/* this class is used to perform the projection operation */
public class ProjectTableOperation {

	/* this method is used to return a table with the project operation applied */
	public static Table projectTable(Table tableToProject,
			ArrayList<String> selectList, boolean flag)
			throws IOException, ParseException {

		// this is the projected table's ArrayList
		ArrayList<String> projectedArrayList = new ArrayList<String>();

		Table ProjectedTable = new Table(tableToProject.tableName
				+ "ProjectedTable.tbl", selectList.size(), null,
				tableToProject.tableDataDirectoryPath);

		if (flag == false) {
			for (String str : selectList) {
				String[] temp = str.split(" ");
				if (str.contains(" AS ") || str.contains(" as ")) {
					if (str.contains(" * ") || str.contains(" + ")
							|| str.contains(" - ") || str.contains(" / ")) {

						ColumnDefinition colDescTypePair = new ColumnDefinition();
						ColDataType col = new ColDataType();
						colDescTypePair.setColumnName(temp[temp.length - 1]);
						col.setDataType("Decimal");
						colDescTypePair.setColDataType(col);
						ProjectedTable.columnDescriptionList
								.add(colDescTypePair);
					} else {

						ColumnDefinition colDescTypePair = new ColumnDefinition();
						ColDataType col = new ColDataType();
						colDescTypePair.setColumnName(temp[temp.length - 1]);
						String type = tableToProject.columnDescriptionList
								.get(tableToProject.columnIndexMap.get(temp[0]))
								.getColDataType().getDataType();
						col.setDataType(type);
						colDescTypePair.setColDataType(col);
						ProjectedTable.columnDescriptionList
								.add(colDescTypePair);
					}

				} else {
					ColumnDefinition colDescTypePair = new ColumnDefinition();
					ColDataType col = new ColDataType();
					colDescTypePair.setColumnName(temp[0]);

					String type = "Decimal";
					col.setDataType(type);
					colDescTypePair.setColDataType(col);
					ProjectedTable.columnDescriptionList.add(colDescTypePair);
				}

			}
		}

		if (flag == true) {

			for (String str : selectList) {

				if (str.contains(" AS ") || str.contains(" as ")) {
					String[] temp = str.split(" ");

					ColumnDefinition colDescTypePair = new ColumnDefinition();
					ColDataType col = new ColDataType();
					colDescTypePair.setColumnName(temp[temp.length - 1]);
					String type = tableToProject.columnDescriptionList
							.get(tableToProject.columnIndexMap
									.get(temp[temp.length - 1]))
							.getColDataType().getDataType();
					col.setDataType(type);
					colDescTypePair.setColDataType(col);
					ProjectedTable.columnDescriptionList.add(colDescTypePair);
				}

				else if (str.contains("SUM") || str.contains("AVG")
						|| str.contains("sum") || str.contains("avg")
						|| str.contains("min") || str.contains("max")
						|| str.contains("COUNT") || str.contains("count")
						|| str.contains("MIN") || str.contains("MAX")) {
					ColumnDefinition colDescTypePair = new ColumnDefinition();
					ColDataType col = new ColDataType();
					colDescTypePair.setColumnName(str);
					col.setDataType("Int");
					colDescTypePair.setColDataType(col);
					ProjectedTable.columnDescriptionList.add(colDescTypePair);

				} else {
					ColumnDefinition colDescTypePair = new ColumnDefinition();
					ColDataType col = new ColDataType();
					colDescTypePair.setColumnName(str);

					String type = tableToProject.columnDescriptionList
							.get(tableToProject.columnIndexMap.get(str))
							.getColDataType().getDataType();

					col.setDataType(type);
					colDescTypePair.setColDataType(col);
					ProjectedTable.columnDescriptionList.add(colDescTypePair);
				}

			}

		}
		// ProjectedTable.populateColumnIndexMap();

		String[] tupleList;
		String newString = "";
		String columnName = null;
		/**** ABHINAV WORKAROUND FOR TPCH-7 ****/
		ArrayList<String> arr = tableToProject.tableTuples;
		for (int i = 0; i < arr.size(); i++) {
			newString = "";
			tupleList = arr.get(i).split("\\|");
			for (String str : selectList) {
				if (str.contains("*") || str.contains("*") || str.contains("-")
						|| str.contains("+") || str.contains("/")) {

					if (str.contains(" as ") || str.contains(" AS ")) {
						columnName = str.substring(0, str.indexOf(" AS "))
								.trim();

					} else {
						columnName = str;
					}

					ArrayList<String> arrrStr = WhereOperation
							.braceExp(columnName);

					String[] pos = new String[arrrStr.size()];
					arrrStr.toArray(pos);
					String[] postFix = WhereOperation.convertToPos(pos);

					String[] arrNew = new String[postFix.length];

					for (int k = 0; k < postFix.length; k++) {

						if (tableToProject.columnIndexMap
								.containsKey(postFix[k])) {
							arrNew[k] = tupleList[tableToProject.columnIndexMap
									.get(postFix[k])];
						} else {
							arrNew[k] = postFix[k];
						}
					}
					double val = WhereOperation.evaluate(arrNew);
					newString = newString + val + "|";

				} else {
					if (flag == true) {
						String columnAdd = tupleList[tableToProject.columnIndexMap
								.get(str)];
						newString = newString + columnAdd + "|";
					} else {
						if (str.contains("count") && str.contains("distinct")) {
							columnName = str.substring(str.indexOf(" ") + 1,
									str.length());
						} else {
							columnName = str.substring(0, str.indexOf(" "));
						}
						String columnAdd = tupleList[tableToProject.columnIndexMap
								.get(columnName)];
						newString = newString + columnAdd + "|";

					}
				}
			}

			if (newString.trim().charAt(newString.length() - 1) == '|')

			{
				newString = newString.substring(0, newString.length() - 1);
			}
			projectedArrayList.add(newString);
		}
		ProjectedTable.populateColumnIndexMap();
		ProjectedTable.tableTuples = projectedArrayList;
		return ProjectedTable;
	}

}
