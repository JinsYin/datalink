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

    @JsonProperty("Node ID")
    private String id;
    @JsonProperty("X Axis")
    private String xAxis;
    @JsonProperty("Y Axis")
    private String yAxis;
}
