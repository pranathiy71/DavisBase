package Constants;

public final class Constants {

    public static final String HEADER_NAME_ROWID = "rowid";
    public static final String HEADER_NAME_TABLE = "table_name";
    public static final String HEADER_NAME_TEXT = "TEXT";
    public static final String HEADER_NAME_IS_UNIQUE = "is_unique";
    public static final String HEADER_NAME_IS_NULLABLE = "is_nullable";
    public static final String DAVISBASE_TABLE_NAME = "davisbase_tables";
    public static final String DAVISBASE_COLUMN_NAME = "davisbase_columns";
    public static final int PAGE_RECORDS = 0x0D;
    public static final byte NULL_VALUE = 0x00;
    public static final byte SHORT_NULL = 0x01;
    public static final byte INTEGER_NULL = 0x02;
    public static final byte LONG_NULL = 0x03;
    public static final byte TINY_INT_VALUE = 0x04;
    public static final byte SHORT_INT_VALUE = 0x05;
    public static final byte INTEGER_VALUE = 0x06;
    public static final byte LONG_VALUE = 0x07;
    public static final byte FLOAT_VALUE = 0x08;
    public static final byte DOUBLE_VALUE = 0x09;
    public static final byte TEXT_VALUE = 0x0C;
    public static final byte DATETIME_VALUE = 0x0A;
    public static final byte DATE_VALUE = 0x0B;
    public static final String INPUT_PROMPT_NAME = "SqlMg> ";
    public static final String VERSION_NUMBER = "1.1";
    public static final String TABLE_FILE_EXTENSION = ".tbl";
    public static final String INDEX_FILE_EXTENSION = ".ndx";
    public static final String TIMESTAMP_PATTERN = "yyyy-MM-dd_HH:mm:ss";
    public static final String DIRECTORY_CATALOG = "data/catalog/";
    public static final String USER_DIRECTORY = "data/user_data/";
    public static final int PAGE_SIZE = 512;
    public static final int OFFSET_IN_TABLE = PAGE_SIZE - 24;
    public static final int OFFSET_FOR_COLUMN = OFFSET_IN_TABLE - 25;
    public static final String LOGICAL_EQUALS = "=";
    public static final String LOGICAL_LESS_THAN = "<";
    public static final String LOGICAL_GREATER_THAN = ">";
    public static final String LOGICAL_LESS_THAN_EQUAL = "<=";
    public static final String LOGICAL_GREATER_THAN_EQUAL = ">=";
    public static final String LOGICAL_NOT_EQUAL = "!=";
    public static final String NO_VALUE = "NO";

    private Constants() {
        throw new AssertionError();
    }
}
