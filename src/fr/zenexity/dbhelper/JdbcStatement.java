package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcStatement {

    public final PreparedStatement statement;
    private int index;

    public static PreparedStatement prepare(Connection cnx, String sql) throws SQLException {
        PreparedStatement pst = cnx.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        return pst;
    }

    public static int executeUpdate(Connection cnx, String query, Object... params) throws SQLException {
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

    public JdbcStatement(PreparedStatement statement, Object... params) throws SQLException {
        this(statement, 0);
        params(params);
    }

    public JdbcStatement(PreparedStatement statement, Iterable<Object> params) throws SQLException {
        this(statement, 0);
        paramsList(params);
    }

    public JdbcStatement(Connection cnx, String query) throws SQLException {
        this(prepare(cnx, query));
    }

    public JdbcStatement(Connection cnx, String query, Object... params) throws SQLException {
        this(prepare(cnx, query), params);
    }

    public JdbcStatement(Connection cnx, String query, Iterable<Object> params) throws SQLException {
        this(prepare(cnx, query), params);
    }

    public JdbcStatement(Connection cnx, Sql.Query query) throws SQLException {
        this(cnx, query.toString(), query.params());
    }

    public JdbcStatement(Connection cnx, Sql.UpdateQuery query) throws SQLException {
        this(cnx, query.toString(), query.params());
    }

    public void reset() throws SQLException {
        statement.clearParameters();
        index = 0;
    }

    public void params(Object... params) throws SQLException {
        for (Object param : params)
            statement.setObject(++index, param);
    }

    public void paramsList(Iterable<Object> params) throws SQLException {
        for (Object param : params)
            statement.setObject(++index, param);
    }

    public ResultSet executeQuery() throws SQLException {
        return statement.executeQuery();
    }

    public int executeUpdate() throws SQLException {
        return statement.executeUpdate();
    }

    public void close() throws SQLException {
        statement.close();
    }

}
