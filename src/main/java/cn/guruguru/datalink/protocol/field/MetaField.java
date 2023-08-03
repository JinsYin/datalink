package cn.guruguru.datalink.protocol.field;

import cn.guruguru.datalink.protocol.enums.MetaKey;
import lombok.Getter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata field
 *
 * @see org.apache.inlong.sort.protocol.MetaFieldInfo
 */
@Getter
public class MetaField extends DataField {
    public static final String TYPE = "MetaField";

    private static final long serialVersionUID = -3436204467879205139L;

    @JsonProperty("metaKey")
    private final MetaKey metaKey;

    @JsonCreator
    public MetaField(
            @JsonProperty("name") String name,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("metaKey") MetaKey metaKey) {
        super(name, nodeId, null);
        this.metaKey = metaKey;
    }

    public MetaField(
            @JsonProperty("name") String name,
            @JsonProperty("metaKey") MetaKey metaKey) {
        super(name);
        this.metaKey = metaKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MetaField that = (MetaField) o;
        return metaKey == that.metaKey
                && super.equals(that);
    }
}
