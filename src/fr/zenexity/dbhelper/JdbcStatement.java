package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class JdbcStatement {

    public static final int defaultResultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
    public static final int defaultResultSetConcurrency = ResultSet.CONCUR_READ_ONLY;

    public static int executeUpdate(Connection cnx, JdbcAdapter adapter, String query, Object... params) throws JdbcStatementException {
        JdbcStatement qs = prepareUpdate(cnx, adapter, query).params(params);
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


    public static PreparedStatement prepareQuery(Connection cnx, String sql, int resultSetType, int resultSetConcurrency) throws JdbcStatementException {
        try {
            return cnx.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public static PreparedStatement prepareQuery(Connection cnx, String sql) throws JdbcStatementException {
        return prepareQuery(cnx, sql, defaultResultSetType, defaultResultSetConcurrency);
    }

    public static PreparedStatement prepareUpdate(Connection cnx, String sql) throws JdbcStatementException {
        try {
            return cnx.prepareStatement(sql);
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public static PreparedStatement prepareUpdate(Connection cnx, String sql, boolean returnGeneratedKeys) throws JdbcStatementException {
        try {
            return cnx.prepareStatement(sql,
                    returnGeneratedKeys ?
                            Statement.RETURN_GENERATED_KEYS :
                            Statement.NO_GENERATED_KEYS);
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }


    public static JdbcStatement prepareQuery(Connection cnx, JdbcAdapter adapter, String query) throws JdbcStatementException {
        return new JdbcStatement(prepareQuery(cnx, query), adapter);
    }

    public static JdbcStatement prepareQuery(Connection cnx, JdbcAdapter adapter, String query, int resultSetType, int resultSetConcurrency) throws JdbcStatementException {
        return new JdbcStatement(prepareQuery(cnx, query, resultSetType, resultSetConcurrency), adapter);
    }

    public static JdbcStatement prepareUpdate(Connection cnx, JdbcAdapter adapter, String query) throws JdbcStatementException {
        return new JdbcStatement(prepareUpdate(cnx, query), adapter);
    }

    public static JdbcStatement prepareUpdate(Connection cnx, JdbcAdapter adapter, String query, boolean returnGeneratedKeys) throws JdbcStatementException {
        return new JdbcStatement(prepareUpdate(cnx, query, returnGeneratedKeys), adapter);
    }


    public static JdbcStatement prepareQuery(Jdbc jdbc, String query) throws JdbcStatementException {
        return prepareQuery(jdbc.connection, jdbc.adapter, query);
    }

    public static JdbcStatement prepareQuery(Jdbc jdbc, String query, int resultSetType, int resultSetConcurrency) throws JdbcStatementException {
        return prepareQuery(jdbc.connection, jdbc.adapter, query, resultSetType, resultSetConcurrency);
    }

    public static JdbcStatement prepareUpdate(Jdbc jdbc, String query) throws JdbcStatementException {
        return prepareUpdate(jdbc.connection, jdbc.adapter, query);
    }

    public static JdbcStatement prepareUpdate(Jdbc jdbc, String query, boolean returnGeneratedKeys) throws JdbcStatementException {
        return prepareUpdate(jdbc.connection, jdbc.adapter, query, returnGeneratedKeys);
    }


    public static JdbcStatement prepare(Connection cnx, JdbcAdapter adapter, Sql.Query query) throws JdbcStatementException {
        return prepareQuery(cnx, adapter, query.toString()).paramsList(query.params());
    }

    public static JdbcStatement prepare(Connection cnx, JdbcAdapter adapter, Sql.Query query, int resultSetType, int resultSetConcurrency) throws JdbcStatementException {
        return prepareQuery(cnx, adapter, query.toString(), resultSetType, resultSetConcurrency).paramsList(query.params());
    }

    public static JdbcStatement prepare(Connection cnx, JdbcAdapter adapter, Sql.UpdateQuery query) throws JdbcStatementException {
        return prepareUpdate(cnx, adapter, query.toString()).paramsList(query.params());
    }

    public static JdbcStatement prepare(Connection cnx, JdbcAdapter adapter, Sql.UpdateQuery query, boolean returnGeneratedKeys) throws JdbcStatementException {
        return prepareUpdate(cnx, adapter, query.toString(), returnGeneratedKeys).paramsList(query.params());
    }


    public final PreparedStatement statement;
    public final JdbcAdapter adapter;
    private int index;

    public JdbcStatement(PreparedStatement statement, JdbcAdapter adapter) {
        this(statement, 0, adapter);
    }

    public JdbcStatement(PreparedStatement statement, int index, JdbcAdapter adapter) {
        this.statement = statement;
        this.index = index;
        this.adapter = adapter;
    }

    public void reset() throws JdbcStatementException {
        try {
            statement.clearParameters();
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
        index = 0;
    }

    public JdbcStatement params(Object... params) throws JdbcStatementException {
        try {
            for (Object param : params)
                statement.setObject(++index, adapter.encodeSqlValue(param));
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
        return this;
    }

    public JdbcStatement paramsList(Iterable<Object> params) throws JdbcStatementException {
        try {
            for (Object param : params)
                statement.setObject(++index, adapter.encodeSqlValue(param));
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
        return this;
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

    public ResultSet getGeneratedKeys() throws JdbcStatementException {
        try {
            return statement.getGeneratedKeys();
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public <T> JdbcIterator<T> getGeneratedKeys(JdbcResult.Factory<T> resultFactory) throws JdbcException {
        return new JdbcIterator<T>(null, getGeneratedKeys(), adapter, resultFactory);
    }

    public <T> JdbcIterator<T> getGeneratedKeys(Class<T> resultClass) throws JdbcException {
        return getGeneratedKeys(JdbcResult.buildFactory(resultClass));
    }

    public void close() throws JdbcStatementException {
        try {
            statement.close();
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

}
