package cn.guruguru.datalink.exception;

/**
 * Illegal DDL Statement Exception
 */
public class IllegalDDLException extends RuntimeException {

    public IllegalDDLException(String message) {
        super(message);
    }

    public IllegalDDLException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalDDLException(Throwable cause) {
        super(cause);
    }
}
