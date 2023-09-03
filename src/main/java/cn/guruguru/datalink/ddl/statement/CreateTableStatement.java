package cn.guruguru.datalink.ddl.statement;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CreateTableStatement {
    /**
     * unique table identifier, e.g. {@code `my_catalog`.`my_db`.`my_table`}
     */
    private String tableIdentifier;

    /**
     * single CREATE-TABLE statement
     */
    private String ddl;
}
