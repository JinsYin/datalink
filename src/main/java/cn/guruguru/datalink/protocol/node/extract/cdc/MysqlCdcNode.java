package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * MySQL CDC Node
 *
 * @see org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("mysql-cdc")
public class MysqlCdcNode extends CdcExtractNode {
    public MysqlCdcNode(String id, String name, List<DataField> fields, @Nullable Map<String, String> properties, @Nullable String filter, @Nullable WatermarkField watermarkField) {
        super(id, name, fields, properties, filter, watermarkField);
    }

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
