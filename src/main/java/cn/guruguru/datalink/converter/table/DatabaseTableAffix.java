package cn.guruguru.datalink.converter.table;

import lombok.Data;

/**
 * Prefix or suffix of database name and table name. If it is null, it means there is no prefix or
 * suffix.
 */
@Data
public class DatabaseTableAffix {
    // prefix or suffix
    private AffixStrategy affixStrategy;
    // add prefix or suffix value
    private String affixContent;
}
