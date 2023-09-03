package cn.guruguru.datalink.ddl.statement;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CreateDatabaseStatement {
    /**
     * unique database identifier, e.g. {@code `my_catalog`.`my_db`}
     */
    private String databaseIdentifier;

    /**
     * single CREATE-DATABASE statement
     */
    private String ddl;
}
