package cn.guruguru.datalink.exception;

/**
 * Unsupported data source exception
 */
public class UnsupportedDataSourceException extends RuntimeException {
    public UnsupportedDataSourceException(String var1) {
        super(var1);
    }

    public UnsupportedDataSourceException(String var1, Throwable var2) {
        super(var1, var2);
    }

    public UnsupportedDataSourceException(Throwable var1) {
        super(var1);
    }
}
