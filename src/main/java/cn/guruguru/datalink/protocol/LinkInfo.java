package cn.guruguru.datalink.protocol;


import cn.guruguru.datalink.protocol.enums.SyncType;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.transformation.relation.Relation;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Data model for synchronization
 *
 * @see org.apache.inlong.sort.protocol.StreamInfo
 * @see org.apache.inlong.sort.protocol.GroupInfo
 */
@Data
@AllArgsConstructor
public class LinkInfo implements Serializable {
    // 开发模式（FORM/JSON/SQL/CANVAS）
    // private LinkMode linkMode;

    @JsonProperty("syncType")
    private SyncType syncType;

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("nodes")
    private List<Node> nodes;

    @JsonProperty("relation")
    private Relation relation;

    // 引擎配置属性，最终将转成 SET 语句
    @JsonProperty("properties")
    private Map<String, String> properties;
}

