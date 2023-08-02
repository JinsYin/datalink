package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.protocol.Metadata;
import cn.guruguru.datalink.protocol.enums.MetaKey;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import cn.guruguru.datalink.protocol.field.WatermarkField;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OracleCdcNode extends CdcExtractNode implements Metadata, Serializable {

    public OracleCdcNode(String id, String name, List<DataField> fields, @Nullable Map<String, String> properties, @Nullable WatermarkField watermarkField) {
        super(id, name, fields, properties, watermarkField);
    }

    @Override
    public boolean isVirtual(MetaKey metaKey) {
        return false;
    }

    @Override
    public Set<MetaKey> supportedMetaFields() {
        return null;
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
