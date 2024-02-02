package cn.guruguru.datalink.parser.result;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser result for spark sql
 */
@Data
@Slf4j
public class SparkSqlParseResult implements ParseResult {
    private static final long serialVersionUID = -8711127045960117303L;

    private final List<String> setSqls;
    private final List<String> createFunctionSqls;
    private final List<String> createTableSqls;
    private final List<String> insertSqls;

    public SparkSqlParseResult(List<String> setSqls,
                               List<String> createFunctionSqls,
                               List<String> createTableSqls,
                               List<String> insertSqls) {
        this.setSqls = setSqls;
        this.createFunctionSqls = createFunctionSqls;
        this.createTableSqls = Preconditions.checkNotNull(createTableSqls, "createTableSqls is null");
        Preconditions.checkState(!createTableSqls.isEmpty(), "createTableSqls is empty");
        this.insertSqls = Preconditions.checkNotNull(insertSqls, "insertSqls is null");
        Preconditions.checkState(!insertSqls.isEmpty(), "insertSqls is empty");
    }

    @Override
    public List<String> getSqlStatements() {
        List<String> sqls = new ArrayList<>();
        sqls.addAll(setSqls);
        sqls.addAll(createFunctionSqls);
        sqls.addAll(createTableSqls);
        sqls.addAll(insertSqls);
        return sqls;
    }
}
