package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class Jdbc {

    public final Connection connection;
    public final JdbcAdapter adapter;

    private int defaultResultSetType = JdbcStatement.defaultResultSetType;
    private int defaultResultSetConcurrency = JdbcStatement.defaultResultSetConcurrency;

    public Jdbc(Connection connection, JdbcAdapter adapter) {
        this.connection = connection;
        this.adapter = adapter;
    }

    public Jdbc(Connection connection) {
        this(connection, JdbcAdapter.defaultAdapter);
    }

    public int defaultResultSetType() {
        return defaultResultSetType;
    }

    public Jdbc defaultResultSetType(int resultSetType) {
        defaultResultSetType = resultSetType;
        return this;
    }

    public int defaultResultSetConcurrency() {
        return defaultResultSetConcurrency;
    }

    public Jdbc defaultResultSetConcurrency(int resultSetConcurrency) {
        defaultResultSetConcurrency = resultSetConcurrency;
        return this;
    }

    public void close() throws JdbcException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }


    public JdbcStatement newStatement(Sql.Query query) throws JdbcStatementException {
        return JdbcStatement.prepare(connection, adapter, query, defaultResultSetType, defaultResultSetConcurrency);
    }

    public JdbcStatement newStatement(Sql.Query query, int resultSetType, int resultSetConcurrency) throws JdbcStatementException {
        return JdbcStatement.prepare(connection, adapter, query, resultSetType, resultSetConcurrency);
    }

    public JdbcStatement newStatement(Sql.UpdateQuery query) throws JdbcStatementException {
        return JdbcStatement.prepare(connection, adapter, query);
    }

    public JdbcStatement newStatement(Sql.UpdateQuery query, boolean returnGeneratedKeys) throws JdbcStatementException {
        return JdbcStatement.prepare(connection, adapter, query, returnGeneratedKeys);
    }


    public <T> JdbcIterator<T> execute(Sql.Query query, JdbcResult.Factory<T> resultFactory) throws JdbcException {
        JdbcStatement qs = newStatement(query);
        return iterator(qs.statement, qs.executeQuery(), resultFactory);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass) throws JdbcException {
        JdbcStatement qs = newStatement(query);
        return iterator(qs.statement, qs.executeQuery(), resultClass);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass, String... fields) throws JdbcException {
        JdbcStatement qs = newStatement(query);
        return iterator(qs.statement, qs.executeQuery(), resultClass, fields);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, Class<T> resultClass, List<String> fields) throws JdbcException {
        JdbcStatement qs = newStatement(query);
        return iterator(qs.statement, qs.executeQuery(), resultClass, fields);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcException {
        JdbcStatement qs = newStatement(query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, resultFactory);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass) throws JdbcException {
        JdbcStatement qs = newStatement(query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, resultClass);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass, String... fields) throws JdbcException {
        JdbcStatement qs = newStatement(query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, resultClass, fields);
    }

    public <T> JdbcIterator<T> execute(Sql.Query query, int offset, int size, Class<T> resultClass, List<String> fields) throws JdbcException {
        JdbcStatement qs = newStatement(query);
        return iterator(qs.statement, qs.executeQuery(), offset, size, resultClass, fields);
    }


    public <T> JdbcIterator<T> iterator(ResultSet result, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        return iterator(null, result, resultFactory);
    }

    public <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass) throws JdbcIteratorException {
        return iterator(null, result, resultClass);
    }

    public <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass, String... fields) throws JdbcIteratorException {
        return iterator(null, result, resultClass, fields);
    }

    public <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass, List<String> fields) throws JdbcIteratorException {
        return iterator(null, result, resultClass, fields);
    }

    public <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        return iterator(null, result, offset, size, resultFactory);
    }

    public <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass) throws JdbcIteratorException {
        return iterator(null, result, offset, size, resultClass);
    }

    public <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass, String... fields) throws JdbcIteratorException {
        return iterator(null, result, offset, size, resultClass, fields);
    }

    public <T> JdbcIterator<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass, List<String> fields) throws JdbcIteratorException {
        return iterator(null, result, offset, size, resultClass, fields);
    }


    public <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        return new JdbcIterator<T>(statement, result, adapter, resultFactory);
    }

    public <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass) throws JdbcIteratorException {
        return iterator(statement, result, JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass, String... fields) throws JdbcIteratorException {
        return iterator(statement, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, Class<T> resultClass, List<String> fields) throws JdbcIteratorException {
        return iterator(statement, result, JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        return new JdbcIterator.Window<T>(statement, result, offset, size, adapter, resultFactory);
    }

    public <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass) throws JdbcIteratorException {
        return iterator(statement, result, offset, size, JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass, String... fields) throws JdbcIteratorException {
        return iterator(statement, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> iterator(Statement statement, ResultSet result, int offset, int size, Class<T> resultClass, List<String> fields) throws JdbcIteratorException {
        return iterator(statement, result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }


    public int executeUpdate(String query, Object... params) throws JdbcStatementException {
        return JdbcStatement.executeUpdate(connection, adapter, query, params);
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
