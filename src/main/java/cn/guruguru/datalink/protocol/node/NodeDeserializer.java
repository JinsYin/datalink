package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MysqlCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.JdbcScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.load.LakehouseLoadNode;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class NodeDeserializer extends JsonDeserializer<Node> {

    @Override
    public Node deserialize(JsonParser parser, DeserializationContext deserializationContext)
            throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        ObjectMapper objectMapper = new ObjectMapper();
        String type = node.get("type").asText();
        switch (type) {
            // ----- 离线抽取节点 -----
            case "DmScanNode": // InLong 和 Flink 均不支持 DM
                return objectMapper.readValue(node.toString(), JdbcScanNode.class);
            case MySqlScanNode.TYPE:
                return objectMapper.readValue(node.toString(), MySqlScanNode.class);
            // ----- 实时抽取节点 -----
            case MysqlCdcNode.TYPE:
                return objectMapper.readValue(node.toString(), MysqlCdcNode.class);
            case KafkaCdcNode.TYPE:
                return objectMapper.readValue(node.toString(), KafkaCdcNode.class);
            // ----- 目标写入节点 -----
            case LakehouseLoadNode.TYPE:
                return objectMapper.readValue(node.toString(), LakehouseLoadNode.class);
        }
        throw new UnsupportedOperationException("Unsupported node type: " + type);
    }
}
