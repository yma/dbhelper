package fr.zenexity.dbhelper;

public class JdbcIteratorException extends JdbcException {

    public JdbcIteratorException(Throwable cause) {
        super(cause);
    }

    public JdbcIteratorException(String message) {
        super(message);
    }

}
