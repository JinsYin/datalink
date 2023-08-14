package cn.guruguru.datalink.protocol.relation;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.io.Serializable;

@JsonTypeName(NodeLocation.TYPE)
@Data
@NoArgsConstructor
public class NodeLocation implements Serializable {
    public static final String TYPE = "NodeLocation";

    @JsonProperty("节点 ID")
    private String id;
    @JsonProperty("节点 X 轴")
    private String xAxis;
    @JsonProperty("节点 Y 轴")
    private String yAxis;
}
