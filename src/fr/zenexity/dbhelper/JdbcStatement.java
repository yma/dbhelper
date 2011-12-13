package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JdbcStatement {

    public final PreparedStatement statement;
    public final JdbcAdapter adapter;
    private int index;

    public static PreparedStatement prepare(Connection cnx, String sql) throws JdbcStatementException {
        try {
            return cnx.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public static int executeUpdate(Connection cnx, JdbcAdapter adapter, String query, Object... params) throws JdbcStatementException {
        JdbcStatement qs = new JdbcStatement(cnx, adapter, query, params);
        final int result;
        try {
            result = qs.executeUpdate();
        } finally {
            qs.close();
        }
        return result;
    }

    public static int executeUpdate(Connection cnx, String query, Object... params) throws JdbcStatementException {
        return executeUpdate(cnx, JdbcAdapter.defaultAdapter, query, params);
    }

    public JdbcStatement(PreparedStatement statement, JdbcAdapter adapter) {
        this(statement, 0, adapter);
    }

    public JdbcStatement(PreparedStatement statement, int index, JdbcAdapter adapter) {
        this.statement = statement;
        this.index = index;
        this.adapter = adapter;
    }

    public JdbcStatement(PreparedStatement statement, JdbcAdapter adapter, Object... params) throws JdbcStatementException {
        this(statement, adapter);
        params(params);
    }

    public JdbcStatement(PreparedStatement statement, JdbcAdapter adapter, Iterable<Object> params) throws JdbcStatementException {
        this(statement, adapter);
        paramsList(params);
    }

    public JdbcStatement(Connection cnx, JdbcAdapter adapter, String query) throws JdbcStatementException {
        this(prepare(cnx, query), adapter);
    }

    public JdbcStatement(Connection cnx, JdbcAdapter adapter, String query, Object... params) throws JdbcStatementException {
        this(prepare(cnx, query), adapter, params);
    }

    public JdbcStatement(Connection cnx, JdbcAdapter adapter, String query, Iterable<Object> params) throws JdbcStatementException {
        this(prepare(cnx, query), adapter, params);
    }

    public JdbcStatement(Connection cnx, JdbcAdapter adapter, Sql.Query query) throws JdbcStatementException {
        this(cnx, adapter, query.toString(), query.params());
    }

    public JdbcStatement(Connection cnx, JdbcAdapter adapter, Sql.UpdateQuery query) throws JdbcStatementException {
        this(cnx, adapter, query.toString(), query.params());
    }

    public void reset() throws JdbcStatementException {
        try {
            statement.clearParameters();
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
        index = 0;
    }

    public void params(Object... params) throws JdbcStatementException {
        try {
            for (Object param : params)
                statement.setObject(++index, adapter.normalizeValueForSql(param));
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public void paramsList(Iterable<Object> params) throws JdbcStatementException {
        try {
            for (Object param : params)
                statement.setObject(++index, adapter.normalizeValueForSql(param));
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public ResultSet executeQuery() throws JdbcStatementException {
        try {
            return statement.executeQuery();
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public <T> JdbcIterator<T> execute(JdbcResult.Factory<T> resultFactory) throws JdbcException {
        return new JdbcIterator<T>(null, executeQuery(), adapter, resultFactory);
    }

    public <T> JdbcIterator<T> execute(Class<T> resultClass) throws JdbcException {
        return execute(JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> execute(Class<T> resultClass, String... fields) throws JdbcException {
        return execute(JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(Class<T> resultClass, List<String> fields) throws JdbcException {
        return execute(JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcException {
        return new JdbcIterator.Window<T>(null, executeQuery(), offset, size, adapter, resultFactory);
    }

    public <T> JdbcIterator<T> execute(int offset, int size, Class<T> resultClass) throws JdbcException {
        return execute(offset, size, JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> execute(int offset, int size, Class<T> resultClass, String... fields) throws JdbcException {
        return execute(offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> execute(int offset, int size, Class<T> resultClass, List<String> fields) throws JdbcException {
        return execute(offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public int executeUpdate() throws JdbcStatementException {
        try {
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public int executeUpdate(Object... params) throws JdbcStatementException {
        reset();
        params(params);
        return executeUpdate();
    }

    public void close() throws JdbcStatementException {
        try {
            statement.close();
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

}
