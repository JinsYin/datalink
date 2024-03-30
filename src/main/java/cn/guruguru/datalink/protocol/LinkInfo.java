package cn.guruguru.datalink.protocol;


import cn.guruguru.datalink.protocol.enums.RuntimeMode;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.relation.Relation;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
@NoArgsConstructor
public class LinkInfo implements Serializable {
    private static final long serialVersionUID = 5805129166290430783L;

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
    // for deserializing non-collection objects, the 'using' parameter needs to be utilized
    // @JsonDeserialize(contentUsing = NodeDeserializer.class) // Use the @JsonSubTypes instead of
    private List<Node> nodes;

    @JsonProperty("relation")
    private Relation relation;

    /**
     * Configuration properties for the processing engine that will be translated into SET statements
     */
    @JsonProperty("properties")
    private Map<String, String> properties;

    /**
     * Constructor for the {@link LinkInfo}
     *
     * @param id Uniquely identifies of LinkInfo
     * @param name The node name
     * @param description The node description
     * @param nodes The node list that LinkInfo contains
     * @param relation The relation that LinkInfo contains
     * @param properties flink configuration properties
     */
    @JsonCreator
    public LinkInfo(@JsonProperty("id") String id,
                    @JsonProperty("name") String name,
                    @Nullable @JsonProperty("description") String description,
                    @JsonProperty("nodes") List<Node> nodes,
                    @JsonProperty("relation") Relation relation,
                    @Nullable @JsonProperty("properties") Map<String, String> properties) {
        this.id = Preconditions.checkNotNull(id, "id is null");
        this.name = Preconditions.checkNotNull(id, "name is null");
        this.description = description;
        this.nodes = Preconditions.checkNotNull(nodes, "nodes is null");
        Preconditions.checkState(!nodes.isEmpty(), "nodes is empty");
        this.relation = Preconditions.checkNotNull(relation, "relations is null");
        this.properties = properties;
    }

    // ~ utilities --------------------------------------------------

    /**
     * Converts to a {@link LinkInfo} from a json string
     *
     * @param json json string
     * @return LinkInfo
     * @throws JsonProcessingException a JsonProcessingException
     */
    public static LinkInfo deserialize(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(json, LinkInfo.class);
    }

    /**
     * Serialize a {@link LinkInfo} to a json string
     *
     * @param linkInfo {@link LinkInfo}
     * @return json string
     * @throws JsonProcessingException a {@link JsonProcessingException}
     */
    public static String serialize(LinkInfo linkInfo) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(linkInfo);
    }
}

