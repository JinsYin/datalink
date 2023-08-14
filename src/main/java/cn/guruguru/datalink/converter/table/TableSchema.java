package cn.guruguru.datalink.converter.table;

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
    @JsonProperty("tableIdentifier")
    private String tableIdentifier;

    @JsonProperty("tableComment")
    @Nullable
    private String tableComment;

    @JsonProperty("fields")
    private List<TableField> fields;
}
