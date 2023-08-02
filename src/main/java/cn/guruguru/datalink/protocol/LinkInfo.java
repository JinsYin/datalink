package cn.guruguru.datalink.protocol;


import cn.guruguru.datalink.protocol.enums.RuntimeMode;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.relation.Relation;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
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
    // private DevelopmentMode devMode;

    @JsonProperty("runtimeMode")
    private RuntimeMode runtimeMode;

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @Nullable
    @JsonProperty("description")
    private String description;

    @JsonProperty("nodes")
    private List<Node> nodes;

    @JsonProperty("relation")
    private Relation relation;

    // 引擎配置属性，最终将转成 SET 语句
    @JsonProperty("properties")
    private Map<String, String> properties;

    /**
     * LinkInfo Constructor
     *
     * @param id Uniquely identifies of LinkInfo
     * @param nodes The node list that LinkInfo contains
     * @param relation The relation that LinkInfo contains
     */
    @JsonCreator
    public LinkInfo(@JsonProperty("id") String id,
                    @JsonProperty("name") String name,
                    @Nullable @JsonProperty("description") String description,
                    @JsonProperty("nodes") List<Node> nodes,
                    @JsonProperty("relation") Relation relation) {
        this.id = Preconditions.checkNotNull(id, "id is null");
        this.name = Preconditions.checkNotNull(id, "name is null");
        this.description = description;
        this.nodes = Preconditions.checkNotNull(nodes, "nodes is null");
        Preconditions.checkState(!nodes.isEmpty(), "nodes is empty");
        this.relation = Preconditions.checkNotNull(relation, "relations is null");
    }

    // ~ utilities --------------------------------------------------

    /**
     * Converts to LinkInfo from json string
     *
     * @param json json string
     */
    public static LinkInfo serialize(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(json, LinkInfo.class);
    }
}

