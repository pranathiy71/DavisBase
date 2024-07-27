import Constants.Constants;
import Core.TableImpl;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;


public class DavisBase {


    static boolean exit = false;

    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String[] args) {
        init();
        openingScreen();

        String userCommand = "";

        while (!exit) {
            System.out.print(Constants.INPUT_PROMPT_NAME);
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
            parseCommand(userCommand);
        }
        System.out.println("Quiting...");
        System.out.println("Have a great day!");


    }

    public static void openingScreen() {
        System.out.println(getLine("*", 100));
        System.out.println("Welcome to DavisBase - SQL Mg " + Constants.VERSION_NUMBER);
        System.out.println("\nUse the \"help;\" command to display supported commands.");
        System.out.println(getLine("*", 100));
    }


    public static String getLine(String s, int num) {
        String a = "";
        for (int i = 0; i < num; i++) {
            a += s;
        }
        return a;
    }


    public static void help() {
        System.out.println(getLine("*", 80));
        System.out.println("SUPPORTED COMMANDS");
        System.out.println("All commands below are case insensitive");
        System.out.println();
        System.out.println("\tSHOW TABLES;                                               Display all the tables in the database.");
        System.out.println("\tCREATE TABLE table_name (<column_name datatype> <NOT NULL/UNIQUE>);   Create a new table in the database. First record should be primary key of type Int.");
        System.out.println("\tCREATE INDEX ON table_name (<column_name>);       	     Create a new index for the table in the database.");
        System.out.println("\tINSERT INTO table_name VALUES (value1,value2,..);          Insert a new record into the table. First Column is primary key which has inbuilt auto increment function.");
        System.out.println("\tDELETE FROM TABLE table_name WHERE row_id = key_value;     Delete a record from the table whose rowid is <key_value>.");
        System.out.println("\tUPDATE table_name SET column_name = value WHERE condition; Modifies the records in the table.");
        System.out.println("\tSELECT * FROM table_name;                                  Display all records in the table.");
        System.out.println("\tSELECT * FROM table_name WHERE column_name operator value; Display records in the table where the given condition is satisfied.");
        System.out.println("\tDROP TABLE table_name;                                     Remove table data and its schema.");
        System.out.println("\tVERSION;                                                   Show the program version.");
        System.out.println("\tHELP;                                                      Show this help information.");
        System.out.println("\tEXIT;                                                      Exit the program.");
        System.out.println();
        System.out.println();
        System.out.println(getLine("*", 80));
    }


    public static boolean doesTableExist(String nameOfTable) {
        nameOfTable = nameOfTable + ".tbl";

        try {
            File data_dir = new File(Constants.USER_DIRECTORY);
            if (nameOfTable.equalsIgnoreCase(Constants.DAVISBASE_TABLE_NAME + Constants.TABLE_FILE_EXTENSION) || nameOfTable.equalsIgnoreCase(Constants.DAVISBASE_COLUMN_NAME + Constants.TABLE_FILE_EXTENSION))
                data_dir = new File(Constants.DIRECTORY_CATALOG);

            String[] existingTables = data_dir.list();
            for (int i = 0; i < existingTables.length; i++) {
                if (existingTables[i].equals(nameOfTable)) return true;
            }
        } catch (Exception e) {
            System.out.println("Table directory could not be created!");
            e.printStackTrace();
        }

        return false;
    }

    public static void init() {
        try {
            File data_dir = new File("data");
            if (data_dir.mkdir()) {
                System.out.println("Initializing...");
                runInitialize();
            } else {
                data_dir = new File(Constants.DIRECTORY_CATALOG);
                String[] existingTables = data_dir.list();
                boolean tableExists = false;
                boolean colExists = false;
                for (int i = 0; i < existingTables.length; i++) {
                    if (existingTables[i].equals(Constants.DAVISBASE_TABLE_NAME + Constants.TABLE_FILE_EXTENSION))
                        tableExists = true;
                    if (existingTables[i].equals(Constants.DAVISBASE_COLUMN_NAME + Constants.TABLE_FILE_EXTENSION))
                        colExists = true;
                }

                if (!tableExists) {
                    System.out.println("System tables do not exist, initializing...");
                    System.out.println();
                    runInitialize();
                }

                if (!colExists) {
                    System.out.println("System columns do not exist, initializing...");
                    System.out.println();
                    runInitialize();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void runInitialize() {
        try {
            File data_dir = new File(Constants.USER_DIRECTORY);
            data_dir.mkdir();
            data_dir = new File(Constants.DIRECTORY_CATALOG);
            data_dir.mkdir();
            String[] oldTables;
            oldTables = data_dir.list();
            for (int i = 0; i < oldTables.length; i++) {
                File oldFile = new File(data_dir, oldTables[i]);
                oldFile.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        createDavisbaseTable();
        createDavisbaseColumns();
    }


    public static String[] parseOperators(String condition) {
        String[] parsedOperator = new String[3];
        String[] operators = {
                Constants.LOGICAL_NOT_EQUAL,
                Constants.LOGICAL_LESS_THAN_EQUAL,
                Constants.LOGICAL_GREATER_THAN_EQUAL,
                Constants.LOGICAL_EQUALS,
                Constants.LOGICAL_LESS_THAN,
                Constants.LOGICAL_GREATER_THAN
        };

        for (String operator : operators) {
            if (condition.contains(operator)) {
                String[] temp = condition.split(operator);
                parsedOperator[0] = temp[0].trim();
                parsedOperator[1] = operator;
                parsedOperator[2] = temp[1].trim();
                // Exit the loop once a match is found. No need to continue further.
                break;
            }
        }
        return parsedOperator;
    }

    public static void showTablesCommand() {
        String table = Constants.DAVISBASE_TABLE_NAME;
        String[] cols = {Constants.HEADER_NAME_TABLE};
        String[] condition = new String[0];
        TableImpl.runSelectCommand(table, cols, condition, true);
    }

    public static void parseCreateCommand(String createCommandText) {
        String tableName = createCommandText.split(" ")[2];
        String cols = createCommandText.split(tableName)[1].trim();
        String[] create_cols = cols.substring(1, cols.length() - 1).split(",");

        for (int i = 0; i < create_cols.length; i++)
            create_cols[i] = create_cols[i].trim();

        if (doesTableExist(tableName)) {
            System.out.println("Table " + tableName + " already exists.");
        } else {
            TableImpl.runCreateTableCommand(tableName, create_cols);
        }
    }

    public static void parseInsertCommand(String insertCommand) {
        try {
            String table = insertCommand.split(" ")[2];
            String rawCols = insertCommand.split("values")[1].trim();
            String[] insert_vals_init = rawCols.substring(1, rawCols.length() - 1).split(",");
            String[] insert_vals = new String[insert_vals_init.length + 1];
            for (int i = 1; i <= insert_vals_init.length; i++)
                insert_vals[i] = insert_vals_init[i - 1].trim();

            if (doesTableExist(table)) {
                TableImpl.RunInsertIntoCommand(table, insert_vals, Constants.USER_DIRECTORY + "/");
            } else {
                System.out.println("Table " + table + " does not exist.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void parseDeleteCommand(String deleteCommand) {
        String table = deleteCommand.split(" ")[2];
        String[] splitArray = deleteCommand.split("where");
        String OperatorSubString = splitArray.length > 1 ? splitArray[1] : "";
        String[] parsedOperator = splitArray.length > 1 ? parseOperators(OperatorSubString) : new String[0];
        if (doesTableExist(table)) {
            TableImpl.runDeleteCommand(table, parsedOperator, Constants.USER_DIRECTORY);
        } else {
            System.out.println(table + " not found.");
        }
    }

    public static void parseUpdateCommand(String updateCommand) {
        String table = updateCommand.split(" ")[1];
        String whereSubstring = updateCommand.split("set")[1].split("where")[1];
        String setSubstring = updateCommand.split("set")[1].split("where")[0];
        String[] parsedOperator = parseOperators(whereSubstring);
        String[] parsedSetOperator = parseOperators(setSubstring);
        if (!doesTableExist(table)) {
            System.out.println(table + " not found.");
        } else {
            TableImpl.runUpdateCommand(table, parsedOperator, parsedSetOperator, Constants.USER_DIRECTORY);
        }
    }

    public static void parseQueryCommand(String queryCommand) {
        String[] parsedOperator;
        String[] columnsInCommand;
        String[] getWhereCondition = queryCommand.split("where");
        if (getWhereCondition.length > 1) {
            parsedOperator = parseOperators(getWhereCondition[1].trim());
        } else {
            parsedOperator = new String[0];
        }
        String[] select = getWhereCondition[0].split("from");
        String tableName = select[1].trim();
        String colsSubstring = select[0].replace("runSelectCommand", "").trim();
        if (colsSubstring.contains("*")) {
            columnsInCommand = new String[1];
            columnsInCommand[0] = "*";
        } else {
            columnsInCommand = colsSubstring.split(",");
            for (int i = 0; i < columnsInCommand.length; i++)
                columnsInCommand[i] = columnsInCommand[i].trim();
        }

        if (!doesTableExist(tableName)) {
            System.out.println(tableName + " not found.");
        } else {
            TableImpl.runSelectCommand(tableName, columnsInCommand, parsedOperator, true);
        }
    }

    public static void dropTableCommand(String dropTableCommand) {
        String[] splitCommand = dropTableCommand.split(" ");
        String tableSubstring = splitCommand[2];
        if (doesTableExist(tableSubstring)) {
            TableImpl.runDropCommand(tableSubstring);
        } else {
            System.out.println("Table " + tableSubstring + " does not exist.");
        }
    }

    public static void parseIndexCommand(String createIndexCommand) {
        String[] splitCommand = createIndexCommand.split(" ");
        String tableSubstring = splitCommand[2];
        String[] temp = createIndexCommand.split(tableSubstring);
        String columnsSubstring = temp[1].trim();
        String[] createColumnsSubstring = columnsSubstring.substring(1, columnsSubstring.length() - 1).split(",");

        for (int i = 0; i < createColumnsSubstring.length; i++)
            createColumnsSubstring[i] = createColumnsSubstring[i].trim();

        TableImpl.runCreateIndexCommand(tableSubstring, createColumnsSubstring);
    }

    public static void parseCommand(String userCommand) {

        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

        switch (commandTokens.get(0)) {

            case "show":
                showTablesCommand();
                break;

            case "create":
                switch (commandTokens.get(1)) {
                    case "table":
                        parseCreateCommand(userCommand);
                        break;

                    case "index":
                        parseIndexCommand(userCommand);
                        break;

                    default:
                        System.out.println("I didn't understand the command: \"" + userCommand + "\"");
                        System.out.println();
                        break;
                }
                break;

            case "insert":
                parseInsertCommand(userCommand);
                break;

            case "delete":
                parseDeleteCommand(userCommand);
                break;

            case "update":
                parseUpdateCommand(userCommand);
                break;

            case "select":
                parseQueryCommand(userCommand);
                break;

            case "drop":
                dropTableCommand(userCommand);
                break;

            case "help":
                help();
                break;

            case "version":
                System.out.println("DavisBase Version " + Constants.VERSION_NUMBER);
                break;

            case "exit", "quit":
                exit = true;
                break;

            case "cls", "clear":
                System.out.print("\033\143");
                break;

            default:
                System.out.println("I didn't understand the command: \"" + userCommand + "\"");
                System.out.println();
                break;
        }
    }

    private static void createDavisbaseColumns() {
        try {
            RandomAccessFile davisbaseColumn = new RandomAccessFile(Constants.DIRECTORY_CATALOG + "/davisbase_columns.tbl", "rw");
            davisbaseColumn.setLength(Constants.PAGE_SIZE);
            davisbaseColumn.seek(0);
            davisbaseColumn.writeByte(0x0D);
            davisbaseColumn.writeByte(0x09); //no of records


            int[] offset = new int[9];
            offset[0] = Constants.PAGE_SIZE - 45;
            offset[1] = offset[0] - 49;
            offset[2] = offset[1] - 46;
            offset[3] = offset[2] - 50;
            offset[4] = offset[3] - 51;
            offset[5] = offset[4] - 49;
            offset[6] = offset[5] - 59;
            offset[7] = offset[6] - 51;
            offset[8] = offset[7] - 49;

            davisbaseColumn.writeShort(offset[8]);
            davisbaseColumn.writeInt(0);
            davisbaseColumn.writeInt(0);

            for (int i = 0; i < offset.length; i++)
                davisbaseColumn.writeShort(offset[i]);


            //creating davisbase_columns
            davisbaseColumn.seek(offset[0]);
            davisbaseColumn.writeShort(36);
            davisbaseColumn.writeInt(1); //key
            davisbaseColumn.writeByte(6); //no of columns
            davisbaseColumn.writeByte(28); //16+12next file lines indicate the code for datatype/length of the 5 columns
            davisbaseColumn.writeByte(17); //5+12
            davisbaseColumn.writeByte(15); //3+12
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_TABLE_NAME);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_ROWID);
            davisbaseColumn.writeBytes("INT");
            davisbaseColumn.writeByte(1);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);

            davisbaseColumn.seek(offset[1]);
            davisbaseColumn.writeShort(42);
            davisbaseColumn.writeInt(2);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeByte(28);
            davisbaseColumn.writeByte(22);
            davisbaseColumn.writeByte(16);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_TABLE_NAME);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_TABLE);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_TEXT);
            davisbaseColumn.writeByte(2);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);

            davisbaseColumn.seek(offset[2]);
            davisbaseColumn.writeShort(37);
            davisbaseColumn.writeInt(3);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeByte(29);
            davisbaseColumn.writeByte(17);
            davisbaseColumn.writeByte(15);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_COLUMN_NAME);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_ROWID);
            davisbaseColumn.writeBytes("INT");
            davisbaseColumn.writeByte(1);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);

            davisbaseColumn.seek(offset[3]);
            davisbaseColumn.writeShort(43);
            davisbaseColumn.writeInt(4);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeByte(29);
            davisbaseColumn.writeByte(22);
            davisbaseColumn.writeByte(16);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_COLUMN_NAME);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_TABLE);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_TEXT);
            davisbaseColumn.writeByte(2);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);

            davisbaseColumn.seek(offset[4]);
            davisbaseColumn.writeShort(44);
            davisbaseColumn.writeInt(5);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeByte(29);
            davisbaseColumn.writeByte(23);
            davisbaseColumn.writeByte(16);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_COLUMN_NAME);
            davisbaseColumn.writeBytes("column_name");
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_TEXT);
            davisbaseColumn.writeByte(3);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);

            davisbaseColumn.seek(offset[5]);
            davisbaseColumn.writeShort(42);
            davisbaseColumn.writeInt(6);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeByte(29);
            davisbaseColumn.writeByte(21);
            davisbaseColumn.writeByte(16);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_COLUMN_NAME);
            davisbaseColumn.writeBytes("data_type");
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_TEXT);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);

            davisbaseColumn.seek(offset[6]);
            davisbaseColumn.writeShort(52);
            davisbaseColumn.writeInt(7);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeByte(29);
            davisbaseColumn.writeByte(28);
            davisbaseColumn.writeByte(19);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_COLUMN_NAME);
            davisbaseColumn.writeBytes("ordinal_position");
            davisbaseColumn.writeBytes("TINYINT");
            davisbaseColumn.writeByte(5);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);

            davisbaseColumn.seek(offset[7]);
            davisbaseColumn.writeShort(44);
            davisbaseColumn.writeInt(8);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeByte(29);
            davisbaseColumn.writeByte(23);
            davisbaseColumn.writeByte(16);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_COLUMN_NAME);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_IS_NULLABLE);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_TEXT);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);


            davisbaseColumn.seek(offset[8]);
            davisbaseColumn.writeShort(42);
            davisbaseColumn.writeInt(9);
            davisbaseColumn.writeByte(6);
            davisbaseColumn.writeByte(29);
            davisbaseColumn.writeByte(21);
            davisbaseColumn.writeByte(16);
            davisbaseColumn.writeByte(4);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeByte(14);
            davisbaseColumn.writeBytes(Constants.DAVISBASE_COLUMN_NAME);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_IS_UNIQUE);
            davisbaseColumn.writeBytes(Constants.HEADER_NAME_TEXT);
            davisbaseColumn.writeByte(7);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);
            davisbaseColumn.writeBytes(Constants.NO_VALUE);

            davisbaseColumn.close();

            String[] cur_row_id_value = {"10", Constants.DAVISBASE_TABLE_NAME, "cur_row_id", "INT", "3", Constants.NO_VALUE, Constants.NO_VALUE};
            TableImpl.RunInsertIntoCommand(Constants.DAVISBASE_COLUMN_NAME, cur_row_id_value, Constants.DIRECTORY_CATALOG);            //add current row_id column to davisbase_columns
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createDavisbaseTable() {
        try {
            RandomAccessFile davisbaseTable = new RandomAccessFile(Constants.DIRECTORY_CATALOG + "/davisbase_tables.tbl", "rw");
            davisbaseTable.setLength(Constants.PAGE_SIZE);
            davisbaseTable.seek(0);
            davisbaseTable.write(0x0D);
            davisbaseTable.writeByte(0x02);

            //creating davisbase_tables
            davisbaseTable.writeShort(Constants.OFFSET_FOR_COLUMN);
            davisbaseTable.writeInt(0);
            davisbaseTable.writeInt(0);
            davisbaseTable.writeShort(Constants.OFFSET_IN_TABLE);
            davisbaseTable.writeShort(Constants.OFFSET_FOR_COLUMN);

            davisbaseTable.seek(Constants.OFFSET_IN_TABLE);
            davisbaseTable.writeShort(20);
            davisbaseTable.writeInt(1);
            davisbaseTable.writeByte(1);
            davisbaseTable.writeByte(28);
            davisbaseTable.writeBytes(Constants.DAVISBASE_TABLE_NAME);

            davisbaseTable.seek(Constants.OFFSET_FOR_COLUMN);
            davisbaseTable.writeShort(21);
            davisbaseTable.writeInt(2);
            davisbaseTable.writeByte(1);
            davisbaseTable.writeByte(29);
            davisbaseTable.writeBytes(Constants.DAVISBASE_COLUMN_NAME);

            davisbaseTable.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
