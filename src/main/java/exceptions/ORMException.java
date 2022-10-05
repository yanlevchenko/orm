package exceptions;

public class ORMException extends RuntimeException {
    public ORMException(String message, Throwable cause) {
        super(message, cause);
    }
}
