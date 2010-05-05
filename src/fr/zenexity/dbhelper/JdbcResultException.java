package fr.zenexity.dbhelper;

public class JdbcResultException extends Exception {

    public JdbcResultException(Throwable cause) {
        super(cause);
    }

    public JdbcResultException(String message, Throwable cause) {
        super(message, cause);
    }

}
