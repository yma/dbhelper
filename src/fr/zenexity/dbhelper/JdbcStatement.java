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

    public JdbcStatement(PreparedStatement statement, int index) {
        this.statement = statement;
        this.index = index;
    }

    public JdbcStatement(Connection cnx, String query) throws SQLException {
        this(prepare(cnx, query), 0);
    }

    public JdbcStatement(Connection cnx, Sql.Query query) throws SQLException {
        this(cnx, query.toString());
        paramsList(query.params());
    }

    public JdbcStatement(Connection cnx, Sql.UpdateQuery query) throws SQLException {
        this(cnx, query.toString());
        paramsList(query.params());
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
