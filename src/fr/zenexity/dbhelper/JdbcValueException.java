package fr.zenexity.dbhelper;

public class JdbcValueException extends RuntimeException {

    public JdbcValueException(Throwable cause) {
        super(cause);
    }

    public JdbcValueException(String message, Throwable cause) {
        super(message, cause);
    }

}
