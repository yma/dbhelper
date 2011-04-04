package fr.zenexity.dbhelper;

public class JdbcException extends RuntimeException {

    public JdbcException(Throwable cause) {
        super(cause);
    }

    public JdbcException(String message) {
        super(message);
    }

}
