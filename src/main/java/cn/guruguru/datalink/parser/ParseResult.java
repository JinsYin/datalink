package cn.guruguru.datalink.parser;

/**
 * Parser result
 *
 * @see org.apache.inlong.sort.parser.result.ParseResult
 */
public interface ParseResult {
    /**
     * Execute the parse result without waiting
     *
     * @throws Exception The exception may throws when executing
     */
    void execute() throws Exception;
}
