package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.protocol.DataField;
import cn.guruguru.datalink.protocol.enums.DataFormat;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import cn.guruguru.datalink.protocol.transformation.WatermarkField;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.protocol.enums.KafkaScanStartupMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Kafka Binlog
 *
 * @see org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode
 * @see https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/kafka/#cdc-changelog-source
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("kafka-cdc")
public class KafkaCdcNode extends CdcExtractNode {
    private static final long serialVersionUID = 1L;

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

    public KafkaCdcNode(String id, String name, List<DataField> fields, @Nullable Map<String, String> properties, @Nullable String filter, @Nullable WatermarkField watermarkField) {
        super(id, name, fields, properties, filter, watermarkField);
    }

    public Map<String, String> tableOptions() {
        return super.tableOptions();
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
}
