package Core;

import Constants.Constants;
import Utils.BPlusTreeImpl;
import Utils.BTreeImpl;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;


public class TableImpl {

    public static int getTotalPageCount(RandomAccessFile file) {
        int numPages = 0;
        try {
            numPages = (int) (file.length() / ((long) (Constants.PAGE_SIZE)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return numPages;
    }

    public static void runDropCommand(String tableName) {
        try {
            runDeleteCommand(Constants.DAVISBASE_TABLE_NAME, new String[]{"table_name", "=", tableName}, Constants.DIRECTORY_CATALOG);
            runDeleteCommand(Constants.DAVISBASE_COLUMN_NAME, new String[]{"table_name", "=", tableName}, Constants.DIRECTORY_CATALOG);

            File existingFile = new File(Constants.USER_DIRECTORY, tableName + Constants.TABLE_FILE_EXTENSION);
            existingFile.delete();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void runDeleteCommand(String table, String[] cmp, String dir) {
        try {
            ArrayList<Integer> rowIdsArray = new ArrayList<Integer>();

            if (cmp.length == 0 || !"rowid".equals(cmp[0])) {
                //get the rowids to be updated
                RecordsDTO selectedRecordsDTO = runSelectCommand(table, new String[]{"*"}, cmp, false);
                rowIdsArray.addAll(selectedRecordsDTO.content.keySet());
            } else
                // we already have a rowid, just add it to the list
                rowIdsArray.add(Integer.parseInt(cmp[2]));

            for (int rowId : rowIdsArray) {
                //open the file for table
                RandomAccessFile file = new RandomAccessFile(dir + table + Constants.TABLE_FILE_EXTENSION, "rw");
                int numOfPages = getTotalPageCount(file);
                int page = 0;

                //find the page where data is located
                for (int currentPage = 1; currentPage <= numOfPages; currentPage++)
                    if (BPlusTreeImpl.isKeyPresent(file, currentPage, rowId) && BPlusTreeImpl.getTypeOfPage(file, currentPage) == Constants.PAGE_RECORDS) {
                        page = currentPage;
                        break;
                    }

                // if not found return error
                if (page == 0) {
                    System.out.println("There was no data found in the table!");
                    return;
                }

                //get all the nodes on that page
                short[] nodes = BPlusTreeImpl.getCells(file, page);
                int k = 0;

                //iterate over all the cells
                for (int cellNumber = 0; cellNumber < nodes.length; cellNumber++) {
                    //get location for current cell
                    long currLocation = BPlusTreeImpl.getCellPos(file, page, cellNumber);

                    //retrieve all the values
                    String[] values = getValues(file, currLocation);

                    //get the current row id
                    int currRowId = Integer.parseInt(values[0]);

                    //if not current row id, move the cell
                    if (currRowId != rowId) {
                        BPlusTreeImpl.setOffsetForCell(file, page, k, nodes[cellNumber]);
                        k++;
                    }
                }

                //change cell number
                BPlusTreeImpl.setCellId(file, page, (byte) k);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Retrieve values from a certain location in the file
     *
     * @param fileObj  the RandomAccessFile containing the data
     * @param location the location in the file to retrieve the values from
     * @return an array of String containing the retrieved values
     */
    public static String[] getValues(RandomAccessFile fileObj, long location) {

        String[] deserializedValues = null;
        try {

            SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.TIMESTAMP_PATTERN);

            fileObj.seek(location + 2);
            int rowId = fileObj.readInt();
            int numCols = fileObj.readByte();

            byte[] typeCode = new byte[numCols];
            fileObj.read(typeCode);

            deserializedValues = new String[numCols + 1];

            deserializedValues[0] = Integer.toString(rowId);

            for (int i = 1; i <= numCols; i++) {
                switch (typeCode[i - 1]) {
                    case Constants.NULL_VALUE:
                        fileObj.readByte();
                        deserializedValues[i] = "null";
                        break;

                    case Constants.SHORT_NULL:
                        fileObj.readShort();
                        deserializedValues[i] = "null";
                        break;

                    case Constants.INTEGER_NULL:
                        fileObj.readInt();
                        deserializedValues[i] = "null";
                        break;

                    case Constants.LONG_NULL:
                        fileObj.readLong();
                        deserializedValues[i] = "null";
                        break;

                    case Constants.TINY_INT_VALUE:
                        deserializedValues[i] = Integer.toString(fileObj.readByte());
                        break;

                    case Constants.SHORT_INT_VALUE:
                        deserializedValues[i] = Integer.toString(fileObj.readShort());
                        break;

                    case Constants.INTEGER_VALUE:
                        deserializedValues[i] = Integer.toString(fileObj.readInt());
                        break;

                    case Constants.LONG_VALUE:
                        deserializedValues[i] = Long.toString(fileObj.readLong());
                        break;

                    case Constants.FLOAT_VALUE:
                        deserializedValues[i] = String.valueOf(fileObj.readFloat());
                        break;

                    case Constants.DOUBLE_VALUE:
                        deserializedValues[i] = String.valueOf(fileObj.readDouble());
                        break;

                    case Constants.DATETIME_VALUE:
                        Long temp = fileObj.readLong();
                        Date dateTime = new Date(temp);
                        deserializedValues[i] = dateFormat.format(dateTime);
                        break;

                    case Constants.DATE_VALUE:
                        temp = fileObj.readLong();
                        Date date = new Date(temp);
                        deserializedValues[i] = dateFormat.format(date).substring(0, 10);
                        break;

                    //text case
                    default:
                        int len = typeCode[i - 1] - 0x0C;
                        byte[] bytes = new byte[len];
                        fileObj.read(bytes);
                        deserializedValues[i] = new String(bytes);
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return deserializedValues;
    }


    public static void runCreateTableCommand(String tableName, String[] tableColumns) {
        try {
            // adding rowid as the first column
            String[] newCol = new String[tableColumns.length + 1];
            newCol[0] = "rowid INT UNIQUE";
            System.arraycopy(tableColumns, 0, newCol, 1, tableColumns.length);


            //create a file for the new table
            RandomAccessFile fileObj = new RandomAccessFile(Constants.USER_DIRECTORY + tableName + Constants.TABLE_FILE_EXTENSION, "rw");
            fileObj.setLength(Constants.PAGE_SIZE);
            fileObj.seek(0);
            fileObj.writeByte(Constants.PAGE_RECORDS);
            fileObj.close();

            //insert values in davisbase_tables
            String[] values = {"0", tableName, String.valueOf(0)};
            RunInsertIntoCommand(Constants.DAVISBASE_TABLE_NAME, values, Constants.DIRECTORY_CATALOG);

            //parse column data and insert into davisbase_columns
            for (int i = 0; i < newCol.length; i++) {
                String[] splitSubstring = newCol[i].split(" ");
                String isNullable;
                String isUnique = "NO";

                if (splitSubstring.length > 2) {
                    isNullable = "NO";
                    if (splitSubstring[2].toUpperCase().trim().equals("UNIQUE")) isUnique = "YES";
                    else isUnique = "NO";
                } else isNullable = "YES";

                //insert value into davisbase_columns
                String[] splitValueArray = {"0", tableName, splitSubstring[0], splitSubstring[1].toUpperCase(), String.valueOf(i + 1), isNullable, isUnique};
                RunInsertIntoCommand("davisbase_columns", splitValueArray, Constants.DIRECTORY_CATALOG);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runUpdateCommand(String tableName, String[] compareCommand, String[] setCommand, String tableDirectory) {
        try {
            ArrayList<Integer> rowids = new ArrayList<Integer>();

            //get the rowids to be updated
            if (compareCommand.length == 0 || !"rowid".equals(compareCommand[0])) {

                RecordsDTO recordsDTO = runSelectCommand(tableName, new String[]{"*"}, compareCommand, false);
                rowids.addAll(recordsDTO.content.keySet());
            } else rowids.add(Integer.parseInt(compareCommand[2]));

            for (int key : rowids) {
                RandomAccessFile file = new RandomAccessFile(tableDirectory + tableName + Constants.TABLE_FILE_EXTENSION, "rw");
                int numOfPages = getTotalPageCount(file);

                //iterate over all the pages to check which page contains our key
                int page = 0;
                for (int currentPage = 1; currentPage <= numOfPages; currentPage++) {
                    if (BPlusTreeImpl.isKeyPresent(file, currentPage, key) && BPlusTreeImpl.getTypeOfPage(file, currentPage) == Constants.PAGE_RECORDS) {
                        page = currentPage;
                    }
                }

                if (page == 0) {
                    System.out.println("Key not found");
                    return;
                }

                //get all the keys on the current page
                int[] keys = BPlusTreeImpl.getArrayOfKey(file, page);
                int cellNo = 0;

                //search for our key
                for (int i = 0; i < keys.length; i++)
                    if (keys[i] == key) cellNo = i;

                //get the location of our key
                int offset = BPlusTreeImpl.getOffsetForCell(file, page, cellNo);
                long loc = BPlusTreeImpl.getCellPos(file, page, cellNo);

                //get all columns, saved values and data types for current key
                String[] cols = getColumnName(tableName);
                String[] values = getValues(file, loc);
                String[] type = getDataType(tableName);

                //handle date data type
                for (int i = 0; i < type.length; i++)
                    if (type[i].equals("DATE") || type[i].equals("DATETIME")) values[i] = "'" + values[i] + "'";

                //search for our column
                int x = 0;
                for (int i = 0; i < cols.length; i++)
                    if (cols[i].equals(setCommand[0])) {
                        x = i;
                        break;
                    }

                //runUpdateCommand column value
                values[x] = setCommand[2];


                //check for null constraint
                String[] isNullable = isTableNullable(tableName);
                for (int i = 0; i < isNullable.length; i++) {
                    if (values[i].equals("null") && isNullable[i].equals("NO")) {
                        System.out.println("NULL-value constraint violation");
                        return;
                    }
                }

                //runUpdateCommand the value in file
                byte[] byteArray = new byte[cols.length - 1];
                int plsize = calculatePayloadSize(tableName, values, byteArray);
                BPlusTreeImpl.updateChildCell(file, page, offset, plsize, key, byteArray, values);

                file.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void RunInsertIntoCommand(String tableName, String[] tableValues, String tableDir) {
        try {
            RandomAccessFile file = new RandomAccessFile(tableDir + tableName + Constants.TABLE_FILE_EXTENSION, "rw");
            RunInsertIntoCommand(file, tableName, tableValues);
            file.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void RunInsertIntoCommand(RandomAccessFile file, String table, String[] values) {
        // Fetch table's data types
        String[] dateType = getDataType(table);
        // Fetch whether table's fields can contain null
        String[] isNullable = isTableNullable(table);
        // Fetch whether table's fields should contain unique values
        String[] isUnique = isTableUnique(table);
        int rowId = 0;
        // If the provided table is a system table, get the latest rowId
        if (Constants.DAVISBASE_TABLE_NAME.equals(table) || Constants.DAVISBASE_COLUMN_NAME.equals(table)) {
            int numOfPages = getTotalPageCount(file); // Number of pages in file
            int pages = 1;
            for (int p = 1; p <= numOfPages; p++) {
                int rm = BPlusTreeImpl.getRightMostKey(file, p);
                if (rm == 0) pages = p;
            }
            int[] keys = BPlusTreeImpl.getArrayOfKey(file, pages);
            for (int i = 0; i < keys.length; i++)
                if (keys[i] > rowId) rowId = keys[i];

        } else {
            // If it's not a system table, perform a runSelectCommand to get the latest rowId
            RecordsDTO rowIdRecordsDTO = runSelectCommand(Constants.DAVISBASE_TABLE_NAME, new String[]{"cur_row_id"}, new String[]{"table_name", "=", table}, false);
            rowId = Integer.parseInt(rowIdRecordsDTO.content.entrySet().iterator().next().getValue()[2]);
        }
        values[0] = String.valueOf(rowId + 1);

        // Check for null values and handle null value constraint violation
        for (int i = 0; i < isNullable.length; i++)
            if (values[i].equals("null") && isNullable[i].equals("NO")) {
                System.out.println("NULL-value constraint violation");
                System.out.println();
                return;
            }

        // Check for unique constraints and handle unique constraint violation
        for (int i = 0; i < isUnique.length; i++)
            if (isUnique[i].equals("YES")) {
                try {
                    String[] columnName = getColumnName(table);
                    String[] cmp = {columnName[i], "=", values[i]};
                    RecordsDTO recordsDTO = runSelectCommand(table, new String[]{"*"}, cmp, false);
                    if (recordsDTO.num_row > 0) {
                        System.out.println("Duplicate key: " + columnName[i]);
                        System.out.println();
                        return;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        // Check if the new row id is unique
        int newRowId = Integer.parseInt(values[0]);
        int page = searchPageKey(file, newRowId);
        if (page != 0) if (BPlusTreeImpl.isKeyPresent(file, page, newRowId)) {
            System.out.println("Constraint Violated - Uniqueness constraint");
            System.out.println("values:");
            for (int k = 0; k < values.length; k++)
                System.out.println(values[k]);
            return;
        }
        if (page == 0) page = 1;

        // Prepare the byte array to hold the typeCode of each column
        byte[] typeCode = new byte[dateType.length - 1];
        // Calculate payload size for the new record
        short payloadSize = (short) calculatePayloadSize(table, values, typeCode);
        // Calculate the total size of the new cell
        int cellSize = payloadSize + 6;
        // Check if there's enough space in the leaf node to insert the new cell
        int offset = BPlusTreeImpl.getChildSpace(file, page, cellSize);

        // If there is enough space, insert the cell. Else, divide the leaf and recall the method
        if (offset != -1) {
            BPlusTreeImpl.insertChildCell(file, page, offset, payloadSize, newRowId, typeCode, values);
        } else {
            BPlusTreeImpl.splitChild(file, page);
            RunInsertIntoCommand(file, table, values);
        }

        // If the table is not a system table, update its rowId in the davisbase_tables.tbl
        if (!Constants.DAVISBASE_TABLE_NAME.equals(table) && !Constants.DAVISBASE_COLUMN_NAME.equals(table)) {
            runUpdateCommand(Constants.DAVISBASE_TABLE_NAME, new String[]{"table_name", "=", table}, new String[]{"cur_row_id", "=", String.valueOf(values[0])}, Constants.DIRECTORY_CATALOG);
        }
    }

    public static int calculatePayloadSize(String table, String[] vals, byte[] typeCode) {

        // Retrieve data types corresponding to 'table'
        String[] dataType = getDataType(table);

        // Initialize size with the number of data types (fields)
        int size = dataType.length;

        // For each data type except at 0th index
        for (int i = 1; i < dataType.length; i++) {

            // Get type code for each data type and value pair
            typeCode[i - 1] = getTypeCode(vals[i], dataType[i]);

            // Add the length of field to the total size
            size = size + getLengthOfField(typeCode[i - 1]);
        }

        return size;
    }


    public static byte getTypeCode(String value, String dataType) {
        if (value.equals("null")) {

            // switch block to cover various dataTypes
            switch (dataType) {

                // For each data type case, return the dedicated null type code
                case "TINYINT":
                    return Constants.NULL_VALUE;
                case "SMALLINT":
                    return Constants.SHORT_NULL;
                case "INT":
                    return Constants.INTEGER_NULL;
                case "BIGINT":
                    return Constants.LONG_NULL;
                case "REAL":
                    return Constants.INTEGER_NULL;
                case "DOUBLE":
                    return Constants.LONG_NULL;
                case "DATETIME":
                    return Constants.LONG_NULL;
                case "DATE":
                    return Constants.LONG_NULL;
                case "TEXT":
                    return Constants.LONG_NULL;

                default:
                    return Constants.NULL_VALUE;
            }
        } else {
            // switch block to cover various dataTypes
            switch (dataType) {

                // For each data type, return the matching type code
                case "TINYINT":
                    return Constants.TINY_INT_VALUE;
                case "SMALLINT":
                    return Constants.SHORT_INT_VALUE;
                case "INT":
                    return Constants.INTEGER_VALUE;
                case "BIGINT":
                    return Constants.LONG_VALUE;
                case "REAL":
                    return Constants.FLOAT_VALUE;
                case "DOUBLE":
                    return Constants.DOUBLE_VALUE;
                case "DATETIME":
                    return Constants.DATETIME_VALUE;
                case "DATE":
                    return Constants.DATE_VALUE;
                case "TEXT":
                    return (byte) (value.length() + Constants.TEXT_VALUE);

                default:
                    return Constants.NULL_VALUE;
            }
        }
    }


    public static short getLengthOfField(byte code) {
        switch (code) {
            case Constants.NULL_VALUE:
                return 1;
            case Constants.SHORT_NULL:
                return 2;
            case Constants.INTEGER_NULL:
                return 4;
            case Constants.LONG_NULL:
                return 8;
            case Constants.TINY_INT_VALUE:
                return 1;
            case Constants.SHORT_INT_VALUE:
                return 2;
            case Constants.INTEGER_VALUE:
                return 4;
            case Constants.LONG_VALUE:
                return 8;
            case Constants.FLOAT_VALUE:
                return 4;
            case Constants.DOUBLE_VALUE:
                return 8;
            case Constants.DATETIME_VALUE:
                return 8;
            case Constants.DATE_VALUE:
                return 8;
            default:
                return (short) (code - Constants.TEXT_VALUE);
        }
    }

    public static int searchPageKey(RandomAccessFile fileObj, int keyName) {
        try {
            // Fetch the total number of pages in the file
            int numPages = getTotalPageCount(fileObj);

            // Iterate through the pages
            for (int currPage = 1; currPage <= numPages; currPage++) {
                // Seek to the start of the current page
                fileObj.seek((long) (currPage - 1) * Constants.PAGE_SIZE);

                // Read the type of the page
                byte pageType = fileObj.readByte();

                // If this is a record page
                if (pageType == Constants.PAGE_RECORDS) {
                    // Fetch all the keys in the current page
                    int[] keys = BPlusTreeImpl.getArrayOfKey(fileObj, currPage);

                    // If there are no keys in this page, return 0
                    if (keys.length == 0) return 0;

                    // Get the rightmost key of this page
                    int rm = BPlusTreeImpl.getRightMostKey(fileObj, currPage);

                    // If the key we are looking for is in this page, return the current page number
                    if (keys[0] <= keyName && keyName <= keys[keys.length - 1]) {
                        return currPage;
                    }
                    // If there is no page on right, check if the last key of this page is smaller than the target key.
                    // If it is, return the current page number (used for insertions).
                    else if (rm == 0 && keys[keys.length - 1] < keyName) {
                        return currPage;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // If the key isn't found in any page, return 1
        return 1;
    }

    public static String[] getDataType(String tableName) {
        return getDavisbaseColumnsCols(3, tableName);
    }

    public static String[] getColumnName(String tableName) {
        return getDavisbaseColumnsCols(2, tableName);
    }

    public static String[] isTableNullable(String tableName) {
        return getDavisbaseColumnsCols(5, tableName);
    }

    public static String[] isTableUnique(String table) {
        return getDavisbaseColumnsCols(6, table);
    }

    public static String[] getDavisbaseColumnsCols(int index, String tableName) {
        try {
            //fetch the data from davisbase_columns
            RandomAccessFile file = new RandomAccessFile(Constants.DIRECTORY_CATALOG + "davisbase_columns.tbl", "rw");
            RecordsDTO recordsDTO = new RecordsDTO();
            String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable", "is_unique"};
            String[] cmp = {"table_name", "=", tableName};
            runFilterCommand(file, cmp, columnName, new String[]{}, recordsDTO);

            //save the result
            HashMap<Integer, String[]> content = recordsDTO.content;

            //add all to the result array
            ArrayList<String> array = new ArrayList<String>();
            for (String[] x : content.values()) {
                array.add(x[index]);
            }

            return array.toArray(new String[array.size()]);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return new String[0];
    }

    public static RecordsDTO runSelectCommand(String tableName, String[] columns, String[] compareArray, boolean shouldDisplay) {
        try {
            // Define the path
            String path = Constants.USER_DIRECTORY;

            // Check if the tableName is related to the catalog data
            if (tableName.equalsIgnoreCase(Constants.DAVISBASE_TABLE_NAME) || tableName.equalsIgnoreCase(Constants.DAVISBASE_COLUMN_NAME))
                path = Constants.DIRECTORY_CATALOG;

            // Initialize the random access file reader
            RandomAccessFile file = new RandomAccessFile(path + tableName + Constants.TABLE_FILE_EXTENSION, "rw");

            // Retrieve column names and data types from the tableName
            String[] columnName = getColumnName(tableName);
            String[] dataType = getDataType(tableName);

            // Initialize Records object to store the results
            RecordsDTO recordsDTO = new RecordsDTO();

            // Check  if the command involves a comparison to null
            if (compareArray.length > 0 && compareArray[1].equals("=") && compareArray[2].equalsIgnoreCase("null")) {
                System.out.println("Empty Set");
                file.close();
                return null;
            }

            // Prepare compareArray for non-null comparison
            if (compareArray.length > 0 && compareArray[1].equals("!=") && compareArray[2].equalsIgnoreCase("null")) {
                compareArray = new String[0];
            }

            // Run the filter command with the provided parameters
            runFilterCommand(file, compareArray, columnName, dataType, recordsDTO);
            // If shouldDisplay is true, display the columns
            if (shouldDisplay) recordsDTO.display(columns);

            file.close();
            return recordsDTO;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static void runFilterCommand(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, RecordsDTO recordsDTO) {
        try {
            // Find the total number of pages
            int numOfPages = getTotalPageCount(file);

            // Processing every page
            for (int page = 1; page <= numOfPages; page++) {

                // Set file reader at the beginning of the page
                file.seek((long) (page - 1) * Constants.PAGE_SIZE);

                // Read the type of the page
                byte pageType = file.readByte();

                // If the page is a records page, process it
                if (pageType == Constants.PAGE_RECORDS) {

                    // Get the total number of cells in the page
                    byte numOfCells = BPlusTreeImpl.getCellId(file, page);

                    // Process each cell
                    for (int cellNum = 0; cellNum < numOfCells; cellNum++) {

                        // Find the location of each cell
                        long loc = BPlusTreeImpl.getCellPos(file, page, cellNum);

                        // Retrieve values from the location
                        String[] vals = getValues(file, loc);

                        // Extract row id from the vals
                        int rowid = Integer.parseInt(vals[0]);

                        // Prepare values for comparison if their type is DATE or DATETIME
                        for (int j = 0; j < type.length; j++)
                            if (type[j].equals("DATE") || type[j].equals("DATETIME")) vals[j] = "'" + vals[j] + "'";

                        // Compare values with the provided comparison parameters
                        boolean check = compareOperators(vals, rowid, cmp, columnName);

                        // Remove preparation for DATE and DATETIME types
                        for (int j = 0; j < type.length; j++)
                            if (type[j].equals("DATE") || type[j].equals("DATETIME"))
                                vals[j] = vals[j].substring(1, vals[j].length() - 1);

                        // If values passed the comparison check, add them to records
                        if (check) recordsDTO.add(rowid, vals);
                    }
                } else continue;
            }

            // Setting column names and initializing format array for records
            recordsDTO.columnName = columnName;
            recordsDTO.format = new int[columnName.length];
        } catch (Exception e) {
            // Handling potential exceptions
            e.printStackTrace();
        }
    }

    public static boolean compareOperators(String[] valuesArray, int idOfRow, String[] compareArray, String[] columnNames) {
        boolean isCorrect = false;  // Initialize comparison result flag
        if (compareArray.length == 0) {  // If no comparison parameters are given, return true
            isCorrect = true;
        } else {
            int colPos = 1;  // Initialize column position
            // Try to find the column name in columnNames that matches the first element of compareArray
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i].equals(compareArray[0])) {
                    colPos = i + 1;
                    break;
                }
            }
            // If the column is "rowid", apply the corresponding comparison operation
            if (colPos == 1) {
                int val = Integer.parseInt(compareArray[2]);
                String operator = compareArray[1];
                switch (operator) {  // Analyzing the operator
                    case Constants.LOGICAL_EQUALS:
                        return idOfRow == val;
                    case Constants.LOGICAL_GREATER_THAN:
                        return idOfRow > val;
                    case Constants.LOGICAL_GREATER_THAN_EQUAL:
                        return idOfRow >= val;
                    case Constants.LOGICAL_LESS_THAN:
                        return idOfRow < val;
                    case Constants.LOGICAL_LESS_THAN_EQUAL:
                        return idOfRow <= val;
                    case Constants.LOGICAL_NOT_EQUAL:
                        return idOfRow != val;
                }
            } else if (compareArray[0] != "table_name") {  // If comparison field is not the "table_name"
                try {
                    int val = Integer.parseInt(compareArray[2]);
                    String operator = compareArray[1];
                    int cmpVal = Integer.parseInt(valuesArray[colPos - 1]);
                    // Apply the corresponding comparison operation similar to the operations for "rowid"
                    switch (operator) {
                        case Constants.LOGICAL_EQUALS:
                            return cmpVal == val;
                        case Constants.LOGICAL_GREATER_THAN:
                            return cmpVal > val;
                        case Constants.LOGICAL_GREATER_THAN_EQUAL:
                            return cmpVal >= val;
                        case Constants.LOGICAL_LESS_THAN:
                            return cmpVal < val;
                        case Constants.LOGICAL_LESS_THAN_EQUAL:
                            return cmpVal <= val;
                        case Constants.LOGICAL_NOT_EQUAL:
                            return cmpVal != val;
                    }
                } catch (Exception e) {
                    // Handle potential exception
                }
            }
            // Compare the string value in compareArray[2] with string value in valuesArray[at colPos]
            return compareArray[2].equals(valuesArray[colPos - 1]);
        }
        return isCorrect;
    }

    public static void runCreateIndexCommand(String tableName, String[] columns) {
        try {
            // Define the path where the table is located
            String path = Constants.USER_DIRECTORY;

            // Open/create a random access file in read-write mode
            RandomAccessFile file = new RandomAccessFile(path + tableName + Constants.TABLE_FILE_EXTENSION, "rw");

            // Get column names from the specified table
            String[] columnName = getColumnName(tableName);

            // Create a new BTree which will be used for indexing
            BTreeImpl b = new BTreeImpl(new RandomAccessFile(path + tableName + Constants.INDEX_FILE_EXTENSION, "rw"));

            // Variable used to track column index
            int control = 0;

            // Derive index of matching column names from the input array
            for (int j = 0; j < columns.length; j++)
                for (int i = 0; i < columnName.length; i++)
                    if (columns[j].equals(columnName[i])) control = i;

            try {
                // Get total number of pages in the file
                int numOfPages = getTotalPageCount(file);

                // Loop through each page in file
                for (int page = 1; page <= numOfPages; page++) {

                    // Move file pointer to the start of the current page
                    file.seek((long) (page - 1) * Constants.PAGE_SIZE);

                    // Read page type byte
                    byte pageType = file.readByte();

                    // Process the page if it is a leaf page (0x0D) and ignore others
                    if (pageType == 0x0D) {

                        // Get number of cells in the current page
                        byte numOfCells = BPlusTreeImpl.getCellId(file, page);

                        // Loop through each cell within this page
                        for (int i = 0; i < numOfCells; i++) {

                            // Get the location of the current cell
                            long loc = BPlusTreeImpl.getCellPos(file, page, i);

                            // Retrieve values from the current cell
                            String[] vals = getValues(file, loc);

                            // Parse the row ID (assuming it's the first value)
                            int rowid = Integer.parseInt(vals[0]);

                            // Add cell values to BTree
                            b.add(String.valueOf(vals[control]), String.format("%04x", loc));
                        }
                    } else continue;
                }
            } catch (Exception e) {
                // Handle any exception during index creation and print stack trace
                e.printStackTrace();
            }

            // Close the file once we're done processing it
            file.close();
        } catch (Exception e) {
            // Handle any exception during file processing
            e.printStackTrace();
        }
    }
}


