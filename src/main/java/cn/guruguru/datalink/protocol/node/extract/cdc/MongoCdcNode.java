package cn.guruguru.datalink.protocol.node.extract.cdc;

import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Mongodb extract node
 *
 * @see org.apache.inlong.sort.protocol.node.extract.MongoExtractNode
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName(MongoCdcNode.TYPE)
@Data
public class MongoCdcNode extends CdcExtractNode implements Serializable {
    public static final String TYPE = "MongoCdc";

    private static final long serialVersionUID = 1L;

    /**
     * the primary key must be "_id"
     *
     * @see <a href="https://ververica.github.io/flink-cdc-connectors/release-2.1/content/connectors/mongodb-cdc.html#">MongoDB CDC Connector</a>
     */
    private static final String ID = "_id";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("primaryKey")
    private String primaryKey;
    @JsonProperty("hostname")
    private String hosts;
    @JsonProperty("username")
    private String username;
    @JsonProperty("password")
    private String password;
    @JsonProperty("database")
    private String database;
    @JsonProperty("collection")
    private String collection;

    @JsonCreator
    public MongoCdcNode(@JsonProperty("id") String id,
                        @JsonProperty("name") String name,
                        @JsonProperty("fields") List<DataField> fields,
                        @JsonProperty("properties") Map<String, String> properties,
                        @Nullable @JsonProperty("watermarkField") WatermarkField waterMarkField,
                        @JsonProperty("collection") @Nonnull String collection,
                        @JsonProperty("hostname") String hostname,
                        @JsonProperty("username") String username,
                        @JsonProperty("password") String password,
                        @JsonProperty("database") String database) {
        super(id, name, fields, properties, waterMarkField);
        if (fields.stream().noneMatch(m -> m.getName().equals(ID))) {
            List<DataField> allFields = new ArrayList<>(fields);
            allFields.add(new DataField(ID, new FieldFormat())); // StringFormatInfo
            this.setFields(allFields);
        }
        this.collection = Preconditions.checkNotNull(collection, "collection is null");
        this.hosts = Preconditions.checkNotNull(hostname, "hostname is null");
        this.username = Preconditions.checkNotNull(username, "username is null");
        this.password = Preconditions.checkNotNull(password, "password is null");
        this.database = Preconditions.checkNotNull(database, "database is null");
        this.primaryKey = ID;
    }

    @Override
    public String genTableName() {
        return null;
    }
}
