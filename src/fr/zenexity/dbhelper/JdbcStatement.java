package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcStatement {

    public final PreparedStatement statement;
    private int index;

    public static PreparedStatement prepare(Connection cnx, String sql) throws JdbcStatementException {
        try {
            return cnx.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public static int executeUpdate(Connection cnx, String query, Object... params) throws JdbcStatementException {
        JdbcStatement qs = new JdbcStatement(cnx, query);
        qs.params(params);
        final int result;
        try {
            result = qs.executeUpdate();
        } finally {
            qs.close();
        }
        return result;
    }

    public JdbcStatement(PreparedStatement statement) {
        this(statement, 0);
    }

    public JdbcStatement(PreparedStatement statement, int index) {
        this.statement = statement;
        this.index = index;
    }

    public JdbcStatement(PreparedStatement statement, Object... params) throws JdbcStatementException {
        this(statement, 0);
        params(params);
    }

    public JdbcStatement(PreparedStatement statement, Iterable<Object> params) throws JdbcStatementException {
        this(statement, 0);
        paramsList(params);
    }

    public JdbcStatement(Connection cnx, String query) throws JdbcStatementException {
        this(prepare(cnx, query));
    }

    public JdbcStatement(Connection cnx, String query, Object... params) throws JdbcStatementException {
        this(prepare(cnx, query), params);
    }

    public JdbcStatement(Connection cnx, String query, Iterable<Object> params) throws JdbcStatementException {
        this(prepare(cnx, query), params);
    }

    public JdbcStatement(Connection cnx, Sql.Query query) throws JdbcStatementException {
        this(cnx, query.toString(), query.params());
    }

    public JdbcStatement(Connection cnx, Sql.UpdateQuery query) throws JdbcStatementException {
        this(cnx, query.toString(), query.params());
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
                statement.setObject(++index, JdbcResult.jdbcValue.normalizeValueForSql(param));
        } catch (SQLException e) {
            throw new JdbcStatementException(e);
        }
    }

    public void paramsList(Iterable<Object> params) throws JdbcStatementException {
        try {
            for (Object param : params)
                statement.setObject(++index, JdbcResult.jdbcValue.normalizeValueForSql(param));
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
