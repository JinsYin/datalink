package cn.guruguru.datalink.exception;

/**
 * Unsupported data type exception
 */
public class UnsupportedDataTypeException extends RuntimeException {

    public UnsupportedDataTypeException(String var1) {
        super(var1);
    }

    public UnsupportedDataTypeException(String var1, Throwable var2) {
        super(var1, var2);
    }

    public UnsupportedDataTypeException(Throwable var1) {
        super(var1);
    }

}
