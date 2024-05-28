package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.parser.EngineType;
import cn.guruguru.datalink.protocol.Metadata;
import cn.guruguru.datalink.protocol.enums.KafkaScanStartupMode;
import cn.guruguru.datalink.protocol.enums.MetaKey;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka Streaming Node
 *
 * @see org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/kafka/">Kafka SQL Connector</a>
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/upsert-kafka/">Upsert Kafka SQL Connector</a>
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/kafka/#bounded-ending-position">Bounded Ending Position</a>
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName(KafkaNode.TYPE)
@NodeDataSource(DataSourceType.KAFKA_2X)
@JsonInclude(JsonInclude.Include.NON_NULL)
@NoArgsConstructor
@Data
public class KafkaNode extends CdcExtractNode implements Metadata, Serializable {
    private static final long serialVersionUID = -6074500799289652678L;
    public static final String TYPE = "Kafka";

    @JsonProperty("topic")
    private String topic;
    @JsonProperty("bootstrapServers")
    private String bootstrapServers;
    @JsonProperty("groupId")
    private String groupId;
    /**
     * data format
     *
     * <p>To be improved, different formats have different properties
     * @see org.apache.inlong.sort.protocol.node.format.Format
     */
    @JsonProperty("format")
    private String format;
    @JsonProperty("scanStartupMode")
    private KafkaScanStartupMode scanStartupMode;
    @JsonProperty("scanStartupSpecificOffsets")
    private String scanStartupSpecificOffsets;
    @JsonProperty("scanStartupTimestampMillis")
    private String scanStartupTimestampMillis;
    @JsonProperty("primaryKey")
    private String primaryKey;

    @JsonCreator
    public KafkaNode(@JsonProperty("id") String id,
                            @JsonProperty("name") String name,
                            @JsonProperty("fields") List<DataField> fields,
                            @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField,
                            @JsonProperty("properties") Map<String, String> properties,
                            @Nonnull @JsonProperty("topic") String topic,
                            @Nonnull @JsonProperty("bootstrapServers") String bootstrapServers,
                            @Nonnull @JsonProperty("format") String format,
                            @JsonProperty("scanStartupMode") KafkaScanStartupMode scanStartupMode,
                            @JsonProperty("primaryKey") String primaryKey,
                            @JsonProperty("groupId") String groupId,
                            @JsonProperty("scanStartupSpecificOffsets") String scanStartupSpecificOffsets,
                            @JsonProperty("scanStartupTimestampMillis") String scanStartupTimestampMillis) {
        super(id, name, fields, properties, watermarkField);
        this.topic = Preconditions.checkNotNull(topic, "kafka topic is empty");
        this.bootstrapServers = Preconditions.checkNotNull(bootstrapServers, "kafka bootstrapServers is empty");
        this.format = Preconditions.checkNotNull(format, "kafka format is empty");
        this.scanStartupMode = Preconditions.checkNotNull(scanStartupMode, "kafka scanStartupMode is empty");
        this.primaryKey = primaryKey;
        this.groupId = groupId;
        if (scanStartupMode == KafkaScanStartupMode.SPECIFIC_OFFSETS) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(scanStartupSpecificOffsets),
                    "scanStartupSpecificOffsets is empty");
            this.scanStartupSpecificOffsets = scanStartupSpecificOffsets;
        }
        if (scanStartupMode == KafkaScanStartupMode.TIMESTAMP_MILLIS) {
            Preconditions.checkArgument(StringUtils.isNotBlank(scanStartupTimestampMillis),
                    "scanStartupTimestampMillis is empty");
            this.scanStartupTimestampMillis = scanStartupTimestampMillis;
        }
    }

    @Override
    public Map<String, String> tableOptions(EngineType engineType) {
        if (engineType != EngineType.FLINK_SQL) {
            throw new UnsupportedEngineException("Unsupported computing engine");
        }
        Map<String, String> options = super.tableOptions(engineType);
        options.put("connector", "kafka");
        options.put("topic", topic);
        options.put("format", format);
        options.put("properties.bootstrap.servers", bootstrapServers);
        options.put("scan.startup.mode", scanStartupMode.getValue());
        if (StringUtils.isNotEmpty(scanStartupSpecificOffsets)) {
            options.put("scan.startup.specific-offsets", scanStartupSpecificOffsets);
        }
        if (StringUtils.isNotBlank(scanStartupTimestampMillis)) {
            options.put("scan.startup.timestamp-millis", scanStartupTimestampMillis);
        }
        if (StringUtils.isNotEmpty(groupId)) {
            options.put("properties.group.id", groupId);
        }
        return options;
    }

    @Override
    public String genTableName() {
        return quoteIdentifier(topic);
    }

    @Override
    public String getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public List<DataField> getPartitionFields() {
        return super.getPartitionFields();
    }

    @Override
    public boolean isVirtual(MetaKey metaKey) {
        switch (metaKey) {
            case KEY:
            case VALUE:
            case HEADERS:
            case HEADERS_TO_JSON_STR:
            case PARTITION:
            case OFFSET:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    @Override
    public Set<MetaKey> supportedMetaFields() {
        return EnumSet.of(MetaKey.PROCESS_TIME, MetaKey.TABLE_NAME, MetaKey.OP_TYPE, MetaKey.DATABASE_NAME,
                MetaKey.SQL_TYPE, MetaKey.PK_NAMES, MetaKey.TS, MetaKey.OP_TS, MetaKey.IS_DDL,
                MetaKey.MYSQL_TYPE, MetaKey.BATCH_ID, MetaKey.UPDATE_BEFORE,
                MetaKey.KEY, MetaKey.VALUE, MetaKey.PARTITION, MetaKey.HEADERS,
                MetaKey.HEADERS_TO_JSON_STR, MetaKey.OFFSET, MetaKey.TIMESTAMP);
    }
}
