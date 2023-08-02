package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.ScanExtractNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * MySQL Scan Node
 *
 * @see org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("mysql-scan")
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class MySqlScanNode extends ScanExtractNode implements Serializable {
    @Override
    public Map<String, String> tableOptions() {
        return super.tableOptions();
    }

    @Override
    public String genTableName() {
        return null;
    }

    @Override
    public String getPrimaryKey() {
        return super.getPrimaryKey();
    }

    @Override
    public List<DataField> getPartitionFields() {
        return super.getPartitionFields();
    }
}
