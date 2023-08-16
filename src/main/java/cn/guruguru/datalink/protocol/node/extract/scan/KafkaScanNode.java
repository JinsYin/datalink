package cn.guruguru.datalink.protocol.node.extract.scan;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.KafkaScanStartupMode;
import org.apache.inlong.sort.protocol.node.extract.KafkaExtractNode;
import org.apache.inlong.sort.protocol.node.format.Format;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Kafka Scan
 *
 * @see https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/kafka/#bounded-ending-position
 */
@JsonTypeName(KafkaScanNode.TYPE)
@NodeDataSource(DataSourceType.KAFKA_2X)
public class KafkaScanNode extends KafkaExtractNode {
    public static final String TYPE = "KafkaScan";

    public KafkaScanNode(String id, String name, List<FieldInfo> fields, @Nullable WatermarkField watermarkField, Map<String, String> properties, @Nonnull String topic, @Nonnull String bootstrapServers, @Nonnull Format format, KafkaScanStartupMode kafkaScanStartupMode, String primaryKey, String groupId) {
        super(id, name, fields, watermarkField, properties, topic, bootstrapServers, format, kafkaScanStartupMode, primaryKey, groupId);
    }

    public KafkaScanNode(String id, String name, List<FieldInfo> fields, @Nullable WatermarkField watermarkField, Map<String, String> properties, @Nonnull String topic, @Nonnull String bootstrapServers, @Nonnull Format format, KafkaScanStartupMode kafkaScanStartupMode, String primaryKey, String groupId, String scanSpecificOffsets, String scanTimestampMillis) {
        super(id, name, fields, watermarkField, properties, topic, bootstrapServers, format, kafkaScanStartupMode, primaryKey, groupId, scanSpecificOffsets, scanTimestampMillis);
    }
}
