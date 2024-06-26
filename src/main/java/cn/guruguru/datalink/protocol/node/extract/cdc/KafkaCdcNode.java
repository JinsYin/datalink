package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.protocol.Metadata;
import cn.guruguru.datalink.protocol.enums.KafkaScanStartupMode;
import cn.guruguru.datalink.protocol.enums.MetaKey;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.enums.DataFormat;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka Binlog
 *
 * @see org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/kafka/#cdc-changelog-source">CDC Changelog Source</a>
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName(KafkaCdcNode.TYPE)
@NodeDataSource(DataSourceType.KAFKA_2X)
@NoArgsConstructor(force = true)
@Data
public class KafkaCdcNode extends CdcExtractNode implements Metadata, Serializable {
    private static final long serialVersionUID = 919715241203178816L;
    public static final String TYPE = "KafkaCdc";

    @Nonnull
    @JsonProperty("topic")
    private String topic;
    @Nonnull
    @JsonProperty("bootstrapServers")
    private String bootstrapServers;

    @Nonnull
    @JsonProperty("format")
    private DataFormat format; // InLong Sort: Format

    @JsonProperty("scanStartupMode")
    private KafkaScanStartupMode kafkaScanStartupMode;

    @JsonProperty("primaryKey")
    private String primaryKey;

    @JsonProperty("groupId")
    private String groupId;

    @JsonProperty("scanSpecificOffsets")
    private String scanSpecificOffsets;

    @JsonProperty("scanTimestampMillis")
    private String scanTimestampMillis;

    public KafkaCdcNode(String id, String name, List<DataField> fields, @Nullable Map<String, String> properties, @Nullable WatermarkField watermarkField) {
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

    public Map<String, String> tableOptions(Parser parser) {
        return super.tableOptions(parser.getEngineType());
    }

    public String genTableName() {
        return null;
    }

    public String getPrimaryKey() {
        return super.getPrimaryKey();
    }

    public List<DataField> getPartitionFields() {
        return super.getPartitionFields();
    }

    @Override
    public String getNodeType() {
        return TYPE;
    }
}
