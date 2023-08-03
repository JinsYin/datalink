package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.parser.ParseResult;
import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.parser.result.FlinkSqlParseResult;
import cn.guruguru.datalink.protocol.LinkInfo;
import cn.guruguru.datalink.protocol.Metadata;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.Field;
import cn.guruguru.datalink.protocol.field.MetaField;
import cn.guruguru.datalink.protocol.node.ExtractNode;
import cn.guruguru.datalink.protocol.node.LoadNode;
import cn.guruguru.datalink.protocol.node.Node;
import cn.guruguru.datalink.protocol.node.extract.CdcExtractNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MongoCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.transform.TransformNode;
import cn.guruguru.datalink.protocol.relation.FieldRelation;
import cn.guruguru.datalink.protocol.relation.NodeRelation;
import cn.guruguru.datalink.converter.type.FlinkTypeConverter;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Flink sql parser
 *
 * @see org.apache.inlong.sort.parser.impl.FlinkSqlParser
 * @see org.apache.inlong.sort.parser.impl.NativeFlinkSqlParser
 */
@Slf4j
public class FlinkSqlParser implements Parser {

    private final LinkInfo linkInfo;

    public static final String SOURCE_MULTIPLE_ENABLE_KEY = "source.multiple.enable";
    private final Set<String> hasParsedSet = new HashSet<>();
    private final List<String> setSqls = new ArrayList<>();
    private final List<String> extractTableSqls = new ArrayList<>();
    private final List<String> transformTableSqls = new ArrayList<>();
    private final List<String> loadTableSqls = new ArrayList<>();
    private final List<String> insertSqls = new ArrayList<>();

    private static final FlinkTypeConverter typeMapper = new FlinkTypeConverter();

    public FlinkSqlParser(LinkInfo linkInfo) {
        this.linkInfo = linkInfo;
    }

    /**
     * Get an instance of FlinkSqlParser
     *
     * @param linkInfo data model abstraction of task execution
     * @return FlinkSqlParser The flink sql parse handler
     */
    public static FlinkSqlParser getInstance(LinkInfo linkInfo) {
        return new FlinkSqlParser(linkInfo);
    }

    /**
     * Parser a {@link LinkInfo}
     *
     * @see org.apache.inlong.sort.parser.impl.FlinkSqlParser#parse()
     */
    @Override
    public ParseResult parse() {
        Preconditions.checkNotNull(linkInfo, "link info is null");
        Preconditions.checkNotNull(linkInfo.getId(), "id is null");
        Preconditions.checkNotNull(linkInfo.getNodes(), "nodes is null");
        Preconditions.checkState(!linkInfo.getNodes().isEmpty(), "nodes is empty");
        Preconditions.checkNotNull(linkInfo.getRelation(), "relation is null");
        Preconditions.checkNotNull(linkInfo.getRelation().getNodeRelations(), "node relations is null");
        Preconditions.checkState(!linkInfo.getRelation().getNodeRelations().isEmpty(), "node relations is empty");
        Preconditions.checkNotNull(linkInfo.getRelation().getFieldRelations(), "field relations is null");
//        Preconditions.checkState(!linkInfo.getRelation().getFieldRelations().isEmpty(), "field relations is empty");
        log.info("start parse LinkInfo, id:{}", linkInfo.getId());
        // Parse nodes
        Map<String, Node> nodeMap = new HashMap<>(linkInfo.getNodes().size());
        linkInfo.getNodes().forEach(s -> {
            Preconditions.checkNotNull(s.getId(), "node id is null");
            nodeMap.put(s.getId(), s);
        });
        // Parse node relations
        Map<String, NodeRelation> nodeRelationMap = new HashMap<>();
        linkInfo.getRelation().getNodeRelations().forEach(r -> {
            for (String output : r.getOutputs()) {
                nodeRelationMap.put(output, r);
            }
        });
        linkInfo.getRelation().getNodeRelations().forEach(r -> {
            parseNodeRelation(r, nodeMap, nodeRelationMap);
        });
        // Parse field relations

        // Parse Flink configuration
        parseFlinkConfiguration(linkInfo);
        log.info("parse LinkInfo success, id:{}", linkInfo.getId());
        // Parse Result
        List<String> createTableSqls = new ArrayList<>(extractTableSqls);
        createTableSqls.addAll(transformTableSqls);
        createTableSqls.addAll(loadTableSqls);
        return new FlinkSqlParseResult(setSqls, createTableSqls, insertSqls);
    }

    /**
     * Parse Flink configuration
     */
    private void parseFlinkConfiguration(LinkInfo linkInfo) {
        if (linkInfo.getProperties() != null) {
            for (Map.Entry<String, String> entry : linkInfo.getProperties().entrySet()) {
                // TODO: check if it is valid
                String key = entry.getKey().trim();
                String value = entry.getValue().trim();
                String statement = String.format("SET %s=%s", key, value);
                setSqls.add(statement);
            }
        }
    }

    /**
     * parse node relation
     * Here we only parse the output node in the relation,
     * and the input node parsing is achieved by parsing the dependent node parsing of the output node.
     *
     * @param relation Define relations between nodes, it also shows the data flow
     * @param nodeMap Store the mapping relation between node id and node
     * @param relationMap Store the mapping relation between node id and relation
     */
    private void parseNodeRelation(NodeRelation relation, Map<String, Node> nodeMap,
                                   Map<String, NodeRelation> relationMap) {
        log.info("start parse node relation, relation:{}", relation);
        Preconditions.checkNotNull(relation, "relation is null");
        Preconditions.checkState(relation.getInputs().size() > 0,
                "relation must have at least one input node");
        Preconditions.checkState(relation.getOutputs().size() > 0,
                "relation must have at least one output node");
        relation.getOutputs().forEach(s -> {
            Preconditions.checkNotNull(s, "node id in outputs is null");
            Node outputNode = nodeMap.get(s);
            Preconditions.checkNotNull(outputNode, "can not find any node by node id " + s);
            parseInputNodes(relation, nodeMap, relationMap);
            parseSingleNode(outputNode, relation, nodeMap);
            // for Load node we need to generate insert sql
            if (outputNode instanceof LoadNode) {
                insertSqls.add(genLoadNodeInsertSql((LoadNode) outputNode, relation, nodeMap));
            }
        });
        log.info("parse node relation success, relation:{}", relation);
    }

    /**
     * parse the input nodes corresponding to the output node
     * @param relation Define relations between nodes, it also shows the data flow
     * @param nodeMap Store the mapping relation between node id and node
     * @param relationMap Store the mapping relation between node id and relation
     */
    private void parseInputNodes(NodeRelation relation, Map<String, Node> nodeMap,
                                 Map<String, NodeRelation> relationMap) {
        for (String upstreamNodeId : relation.getInputs()) {
            if (!hasParsedSet.contains(upstreamNodeId)) {
                Node upstreamNode = nodeMap.get(upstreamNodeId);
                Preconditions.checkNotNull(upstreamNode,
                        "can not find any node by node id " + upstreamNodeId);
                parseSingleNode(upstreamNode, relationMap.get(upstreamNodeId), nodeMap);
            }
        }
    }

    private void registerTableSql(Node node, String sql) {
        if (node instanceof ExtractNode) {
            extractTableSqls.add(sql);
        } else if (node instanceof TransformNode) {
            transformTableSqls.add(sql);
        } else if (node instanceof LoadNode) {
            loadTableSqls.add(sql);
        } else {
            throw new UnsupportedOperationException("Only support [ExtractNode|TransformNode|LoadNode]");
        }
    }

    /**
     * Parse a single node and generate the corresponding sql
     *
     * @param node The abstract of extract, transform, load
     * @param relation Define relations between nodes, it also shows the data flow
     * @param nodeMap store the mapping relation between node id and node
     */
    private void parseSingleNode(Node node, NodeRelation relation, Map<String, Node> nodeMap) {
        if (hasParsedSet.contains(node.getId())) {
            log.warn("the node has already been parsed, node id:{}", node.getId());
            return;
        }
        if (node instanceof ExtractNode) {
            log.info("start parse node, node id:{}", node.getId());
            String sql = genCreateSql(node);
            log.info("node id:{}, create table sql:\n{}", node.getId(), sql);
            registerTableSql(node, sql);
            hasParsedSet.add(node.getId());
        } else {
            Preconditions.checkNotNull(relation, "relation is null");
            if (node instanceof LoadNode) {
                String createSql = genCreateSql(node);
                log.info("node id:{}, create table sql:\n{}", node.getId(), createSql);
                registerTableSql(node, createSql);
                hasParsedSet.add(node.getId());
            } else if (node instanceof TransformNode) {
                TransformNode transformNode = (TransformNode) node;
                Preconditions.checkNotNull(transformNode.getFieldRelations(),
                        "field relations is null");
                Preconditions.checkState(!transformNode.getFieldRelations().isEmpty(),
                        "field relations is empty");
                String createSql = genCreateSql(node);
                log.info("node id:{}, create table sql:\n{}", node.getId(), createSql);
                String selectSql = genTransformSelectSql(transformNode, relation, nodeMap);
                log.info("node id:{}, transform sql:\n{}", node.getId(), selectSql);
                registerTableSql(node, createSql + " AS\n" + selectSql);
                hasParsedSet.add(node.getId());
            }
        }
        log.info("parse node success, node id:{}", node.getId());
    }


    private String genTransformSelectSql(TransformNode transformNode, NodeRelation relation,
                                         Map<String, Node> nodeMap) {
        // parse base relation that one to one and generate the transform sql
        Preconditions.checkState(relation.getInputs().size() == 1,
                "simple transform only support one input node");
        Preconditions.checkState(relation.getOutputs().size() == 1,
                "join node only support one output node");
        return genSimpleSelectSql(transformNode, transformNode.getFieldRelations(), relation,
                transformNode.getFilterClause(), nodeMap);
    }

    /**
     * Generate the most basic conversion sql one-to-one
     *
     * @param node The load node
     * @param fieldRelations The relation between fields
     * @param relation Define relations between nodes, it also shows the data flow
     * @param filterClause The filter clause
     * @param nodeMap Store the mapping relation between node id and node
     * @return Select sql for this node logic
     */
    private String genSimpleSelectSql(Node node, List<FieldRelation> fieldRelations,
                                      NodeRelation relation, String filterClause,
                                      Map<String, Node> nodeMap) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        Map<String, FieldRelation> fieldRelationMap = new HashMap<>(fieldRelations.size());
        fieldRelations.forEach(s -> {
            fieldRelationMap.put(s.getOutputField().getName(), s);
        });
        parseFieldRelations(node.getFields(), fieldRelationMap, sb);
        String tableName = nodeMap.get(relation.getInputs().get(0)).genTableName();
        sb.append("\n    FROM `").append(tableName).append("` ");
        parseFilterFields(filterClause, sb);
        return sb.toString();
    }

    /**
     * Parse filter fields to generate filter sql like 'where 1=1...'
     *
     * @param filterClause The filter clause
     * @param sb Container for storing sql
     */
    private void parseFilterFields(String filterClause, StringBuilder sb) {
        if (filterClause != null ) {
            sb.append("\n ");
            sb.append(filterClause);
        }
    }

    /**
     * Parse field relation
     *
     * @param fields The fields defined in node
     * @param fieldRelationMap The field relation map
     * @param sb Container for storing sql
     */
    private void parseFieldRelations(List<DataField> fields,
                                     Map<String, FieldRelation> fieldRelationMap, StringBuilder sb) {
        for (DataField field : fields) {
            FieldRelation fieldRelation = fieldRelationMap.get(field.getName());
            FieldFormat fieldFormat = field.getFieldFormat();
            if (fieldRelation == null) {
                String targetType = typeMapper.deriveEngineType(new MySqlScanNode(), fieldFormat).getType(); // TODO
                sb.append("\n    CAST(NULL as ").append(targetType).append(") AS ").append(field.format()).append(",");
                continue;
            }
            boolean complexType = "ROW".equals(fieldFormat.getType())
                    || "ARRAY".equals(fieldFormat.getType())
                    || "MAP".equals(fieldFormat.getType());
            Field inputField = fieldRelation.getInputField();
            if (inputField instanceof DataField) {
                DataField DataField = (DataField) inputField;
                FieldFormat formatInfo = DataField.getFieldFormat();
                DataField outputField = fieldRelation.getOutputField();
                boolean sameType = formatInfo != null
                        && outputField != null
                        && outputField.getFieldFormat() != null
                        && outputField.getFieldFormat().getType().equals(formatInfo.getType());
                if (complexType || sameType || fieldFormat == null) {
                    sb.append("\n    ").append(inputField.format()).append(" AS ").append(field.format()).append(",");
                } else {
                    String targetType = typeMapper.deriveEngineType(new MySqlScanNode(), fieldFormat).getType();
                    sb.append("\n    CAST(").append(inputField.format()).append(" as ")
                            .append(targetType).append(") AS ").append(field.format()).append(",");
                }
            } else {
                String targetType = typeMapper.deriveEngineType(new MySqlScanNode(), field.getFieldFormat()).getType();
                sb.append("\n    CAST(").append(inputField.format()).append(" as ")
                        .append(targetType).append(") AS ").append(field.format()).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
    }

    /**
     * Generate load node insert sql
     *
     * @param loadNode The real data write node
     * @param relation The relation between nods
     * @param nodeMap The node map
     * @return Insert sql
     */
    private String genLoadNodeInsertSql(LoadNode loadNode, NodeRelation relation, Map<String, Node> nodeMap) {
        Preconditions.checkNotNull(loadNode.getFieldRelations(), "field relations is null");
        Preconditions.checkState(!loadNode.getFieldRelations().isEmpty(),
                "field relations is empty");
        String selectSql = genLoadSelectSql(loadNode, relation, nodeMap);
        return "INSERT INTO `" + loadNode.genTableName() + "`\n    " + selectSql;
    }

    private String genLoadSelectSql(LoadNode loadNode, NodeRelation relation,
                                    Map<String, Node> nodeMap) {
        // parse base relation that one to one and generate the select sql
        Preconditions.checkState(relation.getInputs().size() == 1,
                "simple transform only support one input node");
        Preconditions.checkState(relation.getOutputs().size() == 1,
                "join node only support one output node");
        return genSimpleSelectSql(loadNode, loadNode.getFieldRelations(), relation,
                    loadNode.getFilterClause(), nodeMap);
    }

    /**
     * Generate create sql
     *
     * @param node The abstract of extract, transform, load
     * @return The creation sql pf table
     */
    private String genCreateSql(Node node) {
        if (node instanceof TransformNode) {
            return genCreateTransformSql(node);
        }
        StringBuilder sb = new StringBuilder("CREATE TABLE `");
        sb.append(node.genTableName()).append("`(\n");
        String filterPrimaryKey = getFilterPrimaryKey(node);
        sb.append(genPrimaryKey(node.getPrimaryKey(), filterPrimaryKey));
        sb.append(parseFields(node.getFields(), node, filterPrimaryKey));
        if (node instanceof CdcExtractNode) {
            CdcExtractNode extractNode = (CdcExtractNode) node;
            if (extractNode.getWatermarkField() != null) {
                sb.append(",\n     ").append(extractNode.getWatermarkField().format());
            }
        }
        sb.append(")");
        if (node.getPartitionFields() != null && !node.getPartitionFields().isEmpty()) {
            sb.append(String.format("\nPARTITIONED BY (%s)",
                    StringUtils.join(formatFields(node.getPartitionFields()), ",")));
        }
        sb.append(parseOptions(node.tableOptions()));
        return sb.toString();
    }

    /**
     * Get filter PrimaryKey for Mongo when multi-sink mode
     */
    private String getFilterPrimaryKey(Node node) {
        if (node instanceof MongoCdcNode) { // MongoCdcNode ?
            if (null != node.getProperties().get(SOURCE_MULTIPLE_ENABLE_KEY)
                    && node.getProperties().get(SOURCE_MULTIPLE_ENABLE_KEY).equals("true")) {
                return node.getPrimaryKey();
            }
        }
        return null;
    }

    /**
     * Generate create transform sql
     *
     * @param node The transform node
     * @return The creation sql of transform node
     */
    private String genCreateTransformSql(Node node) {
        return String.format("CREATE VIEW `%s` (%s)",
                node.genTableName(), parseTransformNodeFields(node.getFields()));
    }

    /**
     * Parse options to generate with options
     *
     * @param options The options defined in node
     * @return The with option string
     */
    private String parseOptions(Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        if (options != null && !options.isEmpty()) {
            sb.append("\n    WITH (");
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
     * Parse transform node fields
     *
     * @param fields The fields defined in node
     * @return Field formats in select sql
     */
    private String parseTransformNodeFields(List<DataField> fields) {
        StringBuilder sb = new StringBuilder();
        for (DataField field : fields) {
            sb.append("\n    `").append(field.getName()).append("`,");
        }
        if (sb.length() > 0) {
            sb.delete(sb.lastIndexOf(","), sb.length());
        }
        return sb.toString();
    }

    /**
     * Parse fields
     *
     * @param fields The fields defined in node
     * @param node The abstract of extract, transform, load
     * @param filterPrimaryKey filter PrimaryKey, use for mongo
     * @return Field formats in select sql
     */
    private String parseFields(List<DataField> fields, Node node, String filterPrimaryKey) {
        StringBuilder sb = new StringBuilder();
        for (DataField field : fields) {
            if (StringUtils.isNotBlank(filterPrimaryKey) && field.getName().equals(filterPrimaryKey)) {
                continue;
            }
            sb.append("    `").append(field.getName()).append("` ");
            if (field instanceof MetaField) {
                if (!(node instanceof Metadata)) {
                    throw new IllegalArgumentException(String.format("Node: %s is not instance of Metadata",
                            node.getClass().getSimpleName()));
                }
                MetaField metaFieldInfo = (MetaField) field;
                Metadata metadataNode = (Metadata) node;
                if (!metadataNode.supportedMetaFields().contains(metaFieldInfo.getMetaKey())) {
                    throw new UnsupportedOperationException(String.format("Unsupported meta field for %s: %s",
                            metadataNode.getClass().getSimpleName(), metaFieldInfo.getMetaKey()));
                }
                sb.append(metadataNode.format(metaFieldInfo.getMetaKey()));
            } else {
                sb.append(typeMapper.deriveEngineType(new MySqlScanNode(), field.getFieldFormat()).getType());
            }
            sb.append(",\n");
        }
        if (sb.length() > 0) {
            sb.delete(sb.lastIndexOf(","), sb.length());
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
    private String genPrimaryKey(String primaryKey, String filterPrimaryKey) {
        boolean checkPrimaryKeyFlag = StringUtils.isNotBlank(primaryKey)
                && (StringUtils.isBlank(filterPrimaryKey) || !primaryKey.equals(filterPrimaryKey));
        if (checkPrimaryKeyFlag) {
            primaryKey = String.format("    PRIMARY KEY (%s) NOT ENFORCED,\n",
                    StringUtils.join(formatFields(primaryKey.split(",")), ","));
        } else {
            primaryKey = "";
        }
        return primaryKey;
    }

    /**
     * Format fields with '`'
     *
     * @param fields The fields that need format
     * @return list of field after format
     */
    private List<String> formatFields(String... fields) {
        List<String> formatFields = new ArrayList<>(fields.length);
        for (String field : fields) {
            if (!field.contains("`")) {
                formatFields.add(String.format("`%s`", field.trim()));
            } else {
                formatFields.add(field);
            }
        }
        return formatFields;
    }

    /**
     * Format fields with '`'
     */
    private List<String> formatFields(List<DataField> fields) {
        List<String> formatFields = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            formatFields.add(field.format());
        }
        return formatFields;
    }
}
