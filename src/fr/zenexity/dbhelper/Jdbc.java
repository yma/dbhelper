package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class Jdbc {

    public static <T> JdbcIterator<T> iterator(ResultSet result, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        return new JdbcIterator<T>(null, result, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass) throws JdbcIteratorException {
        return new JdbcIterator<T>(null, result, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass, String... fields) throws JdbcIteratorException {
        return new JdbcIterator<T>(null, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass, List<String> fields) throws JdbcIteratorException {
        return new JdbcIterator<T>(null, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(null, result, offset, size, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(null, result, offset, size, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass, String... fields) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(null, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass, List<String> fields) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(null, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }


    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        return new JdbcIterator<T>(statement, result, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass) throws JdbcIteratorException {
        return new JdbcIterator<T>(statement, result, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass, String... fields) throws JdbcIteratorException {
        return new JdbcIterator<T>(statement, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass, List<String> fields) throws JdbcIteratorException {
        return new JdbcIterator<T>(statement, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(statement, result, offset, size, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(statement, result, offset, size, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass, String... fields) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(statement, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass, List<String> fields) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(statement, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }


    // Instance

    public final Connection connection;

    public Jdbc(Connection connection) {
        this.connection = connection;
    }

    public void close() throws JdbcException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public JdbcStatement newStatement(String query, Object... params) throws JdbcStatementException {
        JdbcStatement statement =  new JdbcStatement(connection, query);
        statement.params(params);
        return statement;
    }

    public JdbcStatement newStatement(Sql.Query query) throws JdbcStatementException {
        return new JdbcStatement(connection, query);
    }

    public JdbcStatement newStatement(Sql.UpdateQuery query) throws JdbcStatementException {
        return new JdbcStatement(connection, query);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, JdbcResult.Factory<T> resultFactory) throws JdbcException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), resultFactory);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass) throws JdbcException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass, String... fields) throws JdbcException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass, List<String> fields) throws JdbcException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, resultFactory);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass) throws JdbcException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass, String... fields) throws JdbcException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass, List<String> fields) throws JdbcException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public int executeUpdate(String query, Object... params) throws JdbcStatementException {
        return JdbcStatement.executeUpdate(connection, query, params);
    }

    public int execute(Sql.UpdateQuery query) throws JdbcStatementException {
        JdbcStatement qs = newStatement(query);
        final int result;
        try {
            result = qs.executeUpdate();
        } finally {
            qs.close();
        }
        return result;
    }

    public void execute(SqlScript script) throws JdbcStatementException {
        for (Sql.FinalUpdateQuery query : script) execute(query);
    }

}
