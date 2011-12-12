package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcStatement {

    public final PreparedStatement statement;
    private int index;
    private final JdbcValue jdbcValue;

    public static PreparedStatement prepare(Connection cnx, String sql) throws JdbcStatementException {
        try {
            return cnx.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public static int executeUpdate(Connection cnx, JdbcValue jdbcValue, String query, Object... params) throws JdbcStatementException {
        JdbcStatement qs = new JdbcStatement(cnx, jdbcValue, query, params);
        final int result;
        try {
            result = qs.executeUpdate();
        } finally {
            qs.close();
        }
        return result;
    }

    public static int executeUpdate(Connection cnx, String query, Object... params) throws JdbcStatementException {
        return executeUpdate(cnx, JdbcValue.defaultAdapters, query, params);
    }

    public JdbcStatement(PreparedStatement statement, JdbcValue jdbcValue) {
        this(statement, 0, jdbcValue);
    }

    public JdbcStatement(PreparedStatement statement, int index, JdbcValue jdbcValue) {
        this.statement = statement;
        this.index = index;
        this.jdbcValue = jdbcValue;
    }

    public JdbcStatement(PreparedStatement statement, JdbcValue jdbcValue, Object... params) throws JdbcStatementException {
        this(statement, jdbcValue);
        params(params);
    }

    public JdbcStatement(PreparedStatement statement, JdbcValue jdbcValue, Iterable<Object> params) throws JdbcStatementException {
        this(statement, jdbcValue);
        paramsList(params);
    }

    public JdbcStatement(Connection cnx, JdbcValue jdbcValue, String query) throws JdbcStatementException {
        this(prepare(cnx, query), jdbcValue);
    }

    public JdbcStatement(Connection cnx, JdbcValue jdbcValue, String query, Object... params) throws JdbcStatementException {
        this(prepare(cnx, query), jdbcValue, params);
    }

    public JdbcStatement(Connection cnx, JdbcValue jdbcValue, String query, Iterable<Object> params) throws JdbcStatementException {
        this(prepare(cnx, query), jdbcValue, params);
    }

    public JdbcStatement(Connection cnx, JdbcValue jdbcValue, Sql.Query query) throws JdbcStatementException {
        this(cnx, jdbcValue, query.toString(), query.params());
    }

    public JdbcStatement(Connection cnx, JdbcValue jdbcValue, Sql.UpdateQuery query) throws JdbcStatementException {
        this(cnx, jdbcValue, query.toString(), query.params());
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
                statement.setObject(++index, jdbcValue.normalizeValueForSql(param));
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public void paramsList(Iterable<Object> params) throws JdbcStatementException {
        try {
            for (Object param : params)
                statement.setObject(++index, jdbcValue.normalizeValueForSql(param));
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
