package fr.zenexity.dbhelper;

public class SqlException extends RuntimeException {

    public SqlException(Throwable cause) {
        super(cause);
    }

    public SqlException(String message) {
        super(message);
    }

}
