package cn.guruguru.datalink.ddl.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Prefix or suffix of database name and table name. If it is null or the affixStrategy is {@link AffixStrategy#NONE},
 * it means there is no prefix or suffix.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Affix {
    // prefix or suffix
    private AffixStrategy affixStrategy;
    // add prefix or suffix value
    private String affixContent;
}
