package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.protocol.node.extract.ScanExtractNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.common.enums.MetaField;
import org.apache.inlong.sort.protocol.Metadata;

import java.io.Serializable;
import java.util.Set;

/**
 * Mongodb extract node
 *
 * @see org.apache.inlong.sort.protocol.node.extract.MongoExtractNode
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("mongo-scan")
@Data
public class MongoScanNode extends ScanExtractNode implements Metadata, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public String genTableName() {
        return null;
    }

    @Override
    public boolean isVirtual(MetaField metaField) {
        return false;
    }

    @Override
    public Set<MetaField> supportedMetaFields() {
        return null;
    }
}
