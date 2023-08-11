package cn.guruguru.datalink.converter.table;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class TableSchema {
    private String tableIdentifier;
    private String tableComment;
    private List<TableColumn> columns;
}
