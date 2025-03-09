package edu.buffalo.cse562.util;

import edu.buffalo.cse562.model.Table;
import java.util.Comparator;
import java.util.List;

/**
 * Comparator implementation for comparing table rows based on order by columns
 */
public class TableComparator implements Comparator<String> {
    private final Table table;
    private final List orderByList;
    private final int size;
    
    private int[] indexArr;
    private boolean[] orderArr;  // true for DESC, false for ASC
    private String[] typeArr;
    private int orderArrIndex;
    
    /**
     * Creates a new TableComparator
     *
     * @param table the table containing column metadata
     * @param orderByList list of columns to order by
     * @param size size of orderByList
     */
    public TableComparator(Table table, List orderByList, int size) {
        this.table = table;
        this.orderByList = orderByList;
        this.size = size;
        this.orderArrIndex = orderByList.size() - 1;
        populateOrderingAttributes();
        moveToNextOrdering();
    }
    
    private boolean moveToNextOrdering() {
        if (orderArrIndex >= 0) {
            return true;
        }
        return false;
    }
    
    private void populateOrderingAttributes() {
        String[] orderByArr = new String[size];
        int i = 0;
        
        for (Object o : orderByList) {
            orderByArr[i] = o.toString();
            i++;
        }
        
        typeArr = new String[size];
        indexArr = new int[orderByArr.length];
        orderArr = new boolean[orderByArr.length];
        
        for (int j = 0; j < orderByArr.length; j++) {
            String column = orderByArr[j];
            String[] colArr = column.split(" ");
            
            indexArr[j] = table.columnIndexMap.get(colArr[0]);
            typeArr[j] = table.columnDescriptionList.get(table.columnIndexMap.get(
                table.columnDescriptionList.get(indexArr[j]).getColumnName())).getColDataType().toString();
            
            if (colArr.length > 1 && colArr[1].equalsIgnoreCase("desc")) {
                orderArr[j] = true;
            }
        }
    }
    
    @Override
    public int compare(String o1, String o2) {
        String[] s1 = o1.split("\\|");
        String[] s2 = o2.split("\\|");
        
        for (int i = 0; i < indexArr.length; i++) {
            int idx = indexArr[i];
            String type = typeArr[i];
            boolean isDesc = orderArr[i];
            
            int result = compareValues(s1[idx], s2[idx], type, isDesc);
            if (result != 0) {
                return result;
            }
        }
        
        return 0;
    }
    
    private int compareValues(String val1, String val2, String type, boolean isDesc) {
        if (type.equalsIgnoreCase("char") || type.equalsIgnoreCase("VARCHAR") || type.equalsIgnoreCase("String")) {
            return compareStrings(val1, val2, isDesc);
        } else if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("decimal")) {
            return compareNumbers(val1, val2, isDesc);
        }
        return 0;
    }
    
    private int compareStrings(String s1, String s2, boolean isDesc) {
        int result = s1.compareTo(s2);
        return isDesc ? -result : result;
    }
    
    private int compareNumbers(String s1, String s2, boolean isDesc) {
        double d1 = Double.parseDouble(s1);
        double d2 = Double.parseDouble(s2);
        
        if (d1 > d2) return isDesc ? -1 : 1;
        if (d1 < d2) return isDesc ? 1 : -1;
        return 0;
    }
}