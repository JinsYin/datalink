package cn.guruguru.datalink.ddl.table;

/**
 * Case strategy for catalog name, database name, table name, column name and keyword
 */
public enum CaseStrategy {
    // keep the same name
    SAME_NAME,
    // convert to uppercase
    UPPERCASE,
    // convert to lowercase
    LOWERCASE,
}
