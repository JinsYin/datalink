package cn.guruguru.datalink.converter.table;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@Data
@Builder
public class TableSchema {
    @JsonProperty("tableIdentifier")
    private String tableIdentifier;
    @JsonProperty("tableComment")
    private String tableComment;
    @JsonProperty("columns")
    private List<TableField> columns;
}
