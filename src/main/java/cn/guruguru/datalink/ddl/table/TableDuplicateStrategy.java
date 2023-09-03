package cn.guruguru.datalink.ddl.table;

/**
 * Duplicate name strategy for table
 */
public enum TableDuplicateStrategy {
    // skip table creation
    IGNORE,
    // replace existing table
    REPLACE,
}
