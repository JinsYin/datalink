package cn.guruguru.datalink.converter.table;

/**
 * Case strategy for catalog name, database name, table name, column name and keyword
 */
public enum CaseStrategy {
    // keep the same name
    SAME_NAME,
    // convert to uppercase
    TO_UPPERCASE,
    // convert to lowercase
    TO_LOWERCASE,
}
