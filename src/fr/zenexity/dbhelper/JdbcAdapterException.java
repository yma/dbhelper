package fr.zenexity.dbhelper;

public class JdbcAdapterException extends RuntimeException {

    public JdbcAdapterException(Throwable cause) {
        super(cause);
    }

    public JdbcAdapterException(String message, Throwable cause) {
        super(message, cause);
    }

}
