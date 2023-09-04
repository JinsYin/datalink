package cn.guruguru.datalink.ddl.table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
public class TableSchema {
    @JsonProperty("catalog")
    private String catalog;

    @JsonProperty("database")
    private String database;

    @JsonProperty("tableName")
    private String tableName;

    @JsonProperty("tableComment")
    @Nullable
    private String tableComment;

    @JsonProperty("fields")
    private List<TableField> fields;
}
