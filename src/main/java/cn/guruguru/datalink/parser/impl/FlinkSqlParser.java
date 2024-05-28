package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.parser.EngineType;
import cn.guruguru.datalink.protocol.field.DataType;
import cn.guruguru.datalink.parser.result.ParseResult;
import cn.guruguru.datalink.parser.result.FlinkSqlParseResult;
import cn.guruguru.datalink.protocol.Pipeline;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.Field;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.node.extract.cdc.MongoCdcNode;
import cn.guruguru.datalink.protocol.node.transform.TransformNode;
import cn.guruguru.datalink.protocol.relation.FieldRelation;
import cn.guruguru.datalink.type.converter.DataTypeConverter;
import cn.guruguru.datalink.type.converter.FlinkDataTypeConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Flink sql parser
 *
 * @see org.apache.inlong.sort.parser.impl.FlinkSqlParser
 * @see org.apache.inlong.sort.parser.impl.NativeFlinkSqlParser
 */
@Slf4j
public class FlinkSqlParser extends AbstractSqlParser {

    @Override
    public EngineType getEngineType() {
        return EngineType.FLINK_SQL;
    }

    @Override
    protected DataTypeConverter getTypeConverter() {
        return new FlinkDataTypeConverter();
    }

    @Override
    protected ParseResult getParseResult(Pipeline pipeline) {
        // Parse Flink configuration
        List<String> setSqls = parseConfiguration(pipeline);
        List<String> createTableSqls = new ArrayList<>(extractTableSqls);
        createTableSqls.addAll(transformTableSqls);
        createTableSqls.addAll(loadTableSqls);
        return new FlinkSqlParseResult(setSqls, createTableSqls, insertSqls);
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
            boolean complexType = "ROW".equals(dataType.getType())
                    || "ARRAY".equals(dataType.getType())
                    || "MAP".equals(dataType.getType());
            Field inputField = fieldRelation.getInputField();
            if (inputField instanceof DataField) {
                DataField DataField = (DataField) inputField;
                DataType formatInfo = DataField.getDataType();
                DataField outputField = fieldRelation.getOutputField();
                boolean sameType = formatInfo != null
                        && outputField != null
                        && outputField.getDataType() != null
                        && outputField.getDataType().getType().equals(formatInfo.getType());
                if (complexType || sameType || dataType == null) {
                    sb.append("\n    ").append(inputField.format()).append(" AS ").append(field.format()).append(",");
                } else {
                    castFiled(sb, nodeType, dataType, inputField.format(), field.format());
                }
            } else {
                castFiled(sb, nodeType, dataType, inputField.format(), field.format());
            }
        }
        sb.deleteCharAt(sb.length() - 1);
    }

    /**
     * Generate create sql
     *
     * @param node The abstract of extract, transform, load
     * @return The creation sql pf table
     */
    @Override
    protected String genCreateSql(Node node) {
        if (node instanceof TransformNode) {
            return genCreateTransformSql(node);
        }
        return genGenericCreateSql(node);
    }

    /**
     * Get filter PrimaryKey for Mongo when multi-sink mode
     */
    @Override
    protected String getFilterPrimaryKey(Node node) {
        if (node instanceof MongoCdcNode) { // MongoCdcNode ?
            if (null != node.getProperties().get(SOURCE_MULTIPLE_ENABLE_KEY)
                    && node.getProperties().get(SOURCE_MULTIPLE_ENABLE_KEY).equals("true")) {
                return node.getPrimaryKey();
            }
        }
        return null;
    }

    /**
     * Parse options to generate with options
     *
     * @param options The options defined in node
     * @return The with option string
     */
    @Override
    protected String parseOptions(Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        if (options != null && !options.isEmpty()) {
            sb.append(" WITH (");
            for (Map.Entry<String, String> kv : options.entrySet()) {
                sb.append("\n    '").append(kv.getKey()).append("' = '").append(kv.getValue()).append("'").append(",");
            }
            if (sb.length() > 0) {
                sb.delete(sb.lastIndexOf(","), sb.length());
            }
            sb.append("\n)");
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
            primaryKey = String.format(",\n    PRIMARY KEY (%s) NOT ENFORCED",
                    StringUtils.join(formatFields(primaryKey.split(",")), ","));
        } else {
            primaryKey = "";
        }
        return primaryKey;
    }
}
