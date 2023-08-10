package cn.guruguru.datalink.exception;

/**
 * SQL Syntax Exception
 */
public class SQLSyntaxException extends RuntimeException {

    public SQLSyntaxException(String var1) {
        super(var1);
    }

    public SQLSyntaxException(String var1, Throwable var2) {
        super(var1, var2);
    }

    public SQLSyntaxException(Throwable var1) {
        super(var1);
    }
}
