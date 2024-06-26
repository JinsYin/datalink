package cn.guruguru.datalink.parser.result;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser result for flink sql
 *
 * @see org.apache.inlong.sort.parser.result.FlinkSqlParseResult
 */
@Data
@Slf4j
public class FlinkSqlParseResult implements ParseResult {
    private static final long serialVersionUID = -28762188896227462L;

    private final List<String> setSqls;
    private final List<String> createTableSqls;
    private final List<String> insertSqls;
    // private final List<String> addJarSqls;
    // In Flink 1.15, the CREATE FUNCTION syntax does not support the USING JAR clause
    // private final List<String> createFunctionSqls;

    public FlinkSqlParseResult(List<String> setSqls,
                               List<String> createTableSqls,
                               List<String> insertSqls) {
        this.setSqls = setSqls;
        this.createTableSqls = Preconditions.checkNotNull(createTableSqls, "createTableSqls is null");
        Preconditions.checkState(!createTableSqls.isEmpty(), "createTableSqls is empty");
        this.insertSqls = Preconditions.checkNotNull(insertSqls, "insertSqls is null");
        Preconditions.checkState(!insertSqls.isEmpty(), "insertSqls is empty");
    }

    @Override
    public List<String> getSqlStatements() {
        List<String> sqls = new ArrayList<>(setSqls); // deep copy a list
        sqls.addAll(createTableSqls);
        sqls.addAll(insertSqls);
        return sqls;
    }
}
