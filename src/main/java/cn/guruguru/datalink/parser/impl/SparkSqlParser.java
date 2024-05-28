package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.parser.EngineType;
import cn.guruguru.datalink.parser.result.ParseResult;
import cn.guruguru.datalink.parser.result.SparkSqlParseResult;
import cn.guruguru.datalink.protocol.Pipeline;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.DataType;
import cn.guruguru.datalink.protocol.field.Field;
import cn.guruguru.datalink.protocol.node.ExtractNode;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.node.NodePropDescriptor;
import cn.guruguru.datalink.protocol.node.transform.TransformNode;
import cn.guruguru.datalink.protocol.relation.FieldRelation;
import cn.guruguru.datalink.type.converter.DataTypeConverter;
import cn.guruguru.datalink.type.converter.SparkDataTypeConverter;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Spark sql parser
 */
@Slf4j
public class SparkSqlParser extends AbstractSqlParser {

    @Override
    public EngineType getEngineType() {
        return EngineType.SPARK_SQL;
    }

    @Override
    public DataTypeConverter getTypeConverter() {
        return new SparkDataTypeConverter();
    }

    @Override
    protected ParseResult getParseResult(Pipeline pipeline) {
        // Parse the configuration of the computing engine
        List<String> setSqls = parseConfiguration(pipeline);
        List<String> createTableSqls = new ArrayList<>(extractTableSqls);
        createTableSqls.addAll(transformTableSqls);
        createTableSqls.addAll(loadTableSqls);
        return new SparkSqlParseResult(setSqls, Collections.emptyList(), createTableSqls, insertSqls);
    }

    /**
     * Parse field relation
     *
     * @param fields The fields defined in node
     * @param primaryKey The primary key
     * @param fieldRelationMap The field relation map
     * @param sb Container for storing sql
     */
    @Override
    protected void parseFieldRelations(String nodeType,
                                     String primaryKey,
                                     List<DataField> fields,
                                     Map<String, FieldRelation> fieldRelationMap,
                                     StringBuilder sb) {
        for (DataField field : fields) {
            FieldRelation fieldRelation = fieldRelationMap.get(field.getName());
            DataType dataType = field.getDataType();
            if (fieldRelation == null) {
                castFiled(sb, nodeType, dataType, "NULL", field.format());
                continue;
            }
            boolean complexType = "STRUCT".equals(dataType.getType())
                                  || "ARRAY".equals(dataType.getType())
                                  || "MAP".equals(dataType.getType());
            Field inputField = fieldRelation.getInputField();
            if (inputField instanceof DataField) {
                DataField dataField = (DataField) inputField;
                DataType formatInfo = dataField.getDataType();
                DataField outputField = fieldRelation.getOutputField();
                boolean sameType = formatInfo != null
                                   && outputField != null
                                   && outputField.getDataType() != null
                                   && outputField.getDataType().getType().equals(formatInfo.getType());
                if (complexType || sameType || dataType == null) {
                    if (isPrimaryKey(primaryKey, inputField.format())) {
                        castAndCoalesceField(sb, nodeType, inputField, field);
                    } else {
                        sb.append("\n    ")
                                .append(inputField.format())
                                .append(" AS ")
                                .append(field.format())
                                .append(",");
                    }
                } else {
                    // https://github.com/apache/iceberg/issues/2456
                    // https://developer.aliyun.com/ask/550123
                    // Spark will perform a nullable check on the target to be written,
                    // And the primary key of the target cannot be null.
                    // Solution: COALESCE(CAST(`ID` as DECIMAL(10,0)), "0") AS `ID`
                    if (isPrimaryKey(primaryKey, inputField.format())) {
                        castAndCoalesceField(sb, nodeType, inputField, field);
                    } else {
                        castFiled(sb, nodeType, dataType, inputField.format(), field.format());
                    }
                }
            } else {
                castFiled(sb, nodeType, dataType, inputField.format(), field.format());
            }
        }
        sb.deleteCharAt(sb.length() - 1);
    }

    /**
     * Check if the input field is a primary key
     *
     * @param primaryKeys comma separated primary key string
     * @param fieldFormat input field format
     * @return true or false
     */
    private boolean isPrimaryKey(String primaryKeys, String fieldFormat) {
        return !Strings.isNullOrEmpty(primaryKeys)
                && formatFields(primaryKeys.split(",")).contains(fieldFormat);
    }

    /**
     * Generate create sql
     *
     * @param node The abstract of extract, transform, load
     * @return The creation sql pf table
     */
    @Override
    protected String genCreateSql(Node node) {
        if (node instanceof ExtractNode) {
            return genCreateExtractSql(node);
        }
        if (node instanceof TransformNode) {
            return genCreateTransformSql(node);
        }
        return genGenericCreateSql(node);
    }

    /**
     * Generate create extract sql
     *
     * @param node The extract node
     * @return The creation sql of extract node
     */
    private String genCreateExtractSql(Node node) {
        // Spark view does not support schema
        return "CREATE OR REPLACE TEMPORARY VIEW " + node.genTableName() + "\n"
               + parseOptions(node.getPropDescriptor(getEngineType()),
                node.tableOptions(getEngineType()));
    }

    /**
     * Parse options to generate with options
     *
     * @param options The options defined in node
     * @return The with option string
     */
    @Override
    protected String parseOptions(NodePropDescriptor propDescriptor, Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        if (options != null && !options.isEmpty()) {
            // process the USING clause for Spark SQL
            if (options.get("USING") != null) {
                sb.append(" USING ").append(options.get("USING"));
                options.remove("USING");
            }
            if (!options.isEmpty()) {
                sb.append(" ").append(propDescriptor.name()).append(" (");
                for (Map.Entry<String, String> kv : options.entrySet()) {
                    sb.append("\n    ").append(kv.getKey())
                            .append(" '").append(kv.getValue()).append("'")
                            .append(",");
                }
                if (sb.length() > 0) {
                    sb.delete(sb.lastIndexOf(","), sb.length());
                }
                sb.append("\n)");
            }
        }
        return sb.toString();
    }

    /**
     * Generate primary key format in sql
     *
     * @param primaryKey The primary key of table
     * @param filterPrimaryKey filter PrimaryKey, use for mongo
     * @return Primary key format in sql
     */
    @Override
    protected String genPrimaryKey(String primaryKey, String filterPrimaryKey) {
        boolean checkPrimaryKeyFlag = StringUtils.isNotBlank(primaryKey)
                                      && (StringUtils.isBlank(filterPrimaryKey) || !primaryKey.equals(filterPrimaryKey));
        if (checkPrimaryKeyFlag) {
            primaryKey = String.format(",\n    PRIMARY KEY (%s)",
                    StringUtils.join(formatFields(primaryKey.split(",")), ","));
        } else {
            primaryKey = "";
        }
        return primaryKey;
    }
}
