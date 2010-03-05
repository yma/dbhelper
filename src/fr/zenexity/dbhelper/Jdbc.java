package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class Jdbc {

    public static <T> JdbcIterator<T> iterator(ResultSet result, JdbcResult.Factory<T> resultFactory) {
        return new JdbcIterator<T>(null, result, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass) {
        return new JdbcIterator<T>(null, result, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass, String... fields) {
        return new JdbcIterator<T>(null, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass, List<String> fields) {
        return new JdbcIterator<T>(null, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) {
        return new JdbcIterator.Window<T>(null, result, offset, size, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass) {
        return new JdbcIterator.Window<T>(null, result, offset, size, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass, String... fields) {
        return new JdbcIterator.Window<T>(null, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass, List<String> fields) {
        return new JdbcIterator.Window<T>(null, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }


    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, JdbcResult.Factory<T> resultFactory) {
        return new JdbcIterator<T>(statement, result, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass) {
        return new JdbcIterator<T>(statement, result, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass, String... fields) {
        return new JdbcIterator<T>(statement, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass, List<String> fields) {
        return new JdbcIterator<T>(statement, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) {
        return new JdbcIterator.Window<T>(statement, result, offset, size, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass) {
        return new JdbcIterator.Window<T>(statement, result, offset, size, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass, String... fields) {
        return new JdbcIterator.Window<T>(statement, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass, List<String> fields) {
        return new JdbcIterator.Window<T>(statement, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }


    // Instance

    public final Connection connection;

    public Jdbc(Connection connection) {
        this.connection = connection;
    }

    public void close() throws SQLException {
        connection.close();
    }

    public JdbcStatement newStatement(String query, Object... params) throws SQLException {
        JdbcStatement statement =  new JdbcStatement(connection, query);
        statement.params(params);
        return statement;
    }

    public JdbcStatement newStatement(Sql.Query query) throws SQLException {
        return new JdbcStatement(connection, query);
    }

    public JdbcStatement newStatement(Sql.UpdateQuery query) throws SQLException {
        return new JdbcStatement(connection, query);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, JdbcResult.Factory<T> resultFactory) throws SQLException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), resultFactory);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass) throws SQLException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass, String... fields) throws SQLException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass, List<String> fields) throws SQLException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, JdbcResult.Factory<T> resultFactory) throws SQLException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, resultFactory);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass) throws SQLException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass, String... fields) throws SQLException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass, List<String> fields) throws SQLException {
        JdbcStatement qs = new JdbcStatement(connection, query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public int executeUpdate(String query, Object... params) throws SQLException {
        return JdbcStatement.executeUpdate(connection, query, params);
    }

    public int execute(Sql.UpdateQuery query) throws SQLException {
        JdbcStatement qs = newStatement(query);
        final int result;
        try {
            result = qs.executeUpdate();
        } finally {
            qs.close();
        }
        return result;
    }

}
