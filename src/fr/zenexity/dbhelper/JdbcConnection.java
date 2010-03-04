package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcConnection {

    public static PreparedStatement prepareStatement(Connection cnx, String sql) throws SQLException {
        PreparedStatement pst = cnx.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        return pst;
    }

    public static void params(PreparedStatement pst, Object... params) throws SQLException {
        int index = 0;
        for (Object param : params)
            pst.setObject(++index, param);
    }

    public static void paramsList(PreparedStatement pst, Iterable<Object> params) throws SQLException {
        int index = 0;
        for (Object param : params)
            pst.setObject(++index, param);
    }


    public static ResultSet query(Connection cnx, String sql, Object... params) throws SQLException {
        PreparedStatement pst = prepareStatement(cnx, sql);
        params(pst, params);
        try {
            return pst.executeQuery();
        } catch (SQLException e) {
            pst.close();
            throw e;
        }
    }

    public static ResultSet queryList(Connection cnx, String sql, Iterable<Object> params) throws SQLException {
        PreparedStatement pst = prepareStatement(cnx, sql);
        paramsList(pst, params);
        try {
            return pst.executeQuery();
        } catch (SQLException e) {
            pst.close();
            throw e;
        }
    }

    public static int update(Connection cnx, String sql, Object... params) throws SQLException {
        PreparedStatement pst = prepareStatement(cnx, sql);
        params(pst, params);
        final int result;
        try {
            result = pst.executeUpdate();
        } finally {
            pst.close();
        }
        return result;
    }

    public static int updateList(Connection cnx, String sql, Iterable<Object> params) throws SQLException {
        PreparedStatement pst = prepareStatement(cnx, sql);
        paramsList(pst, params);
        final int result;
        try {
            result = pst.executeUpdate();
        } finally {
            pst.close();
        }
        return result;
    }


    public static ResultSet execute(Connection cnx, Sql.Query query) throws SQLException {
        return queryList(cnx, query.toString(), query.params());
    }

    public static int execute(Connection cnx, Sql.UpdateQuery query) throws SQLException {
        return updateList(cnx, query.toString(), query.params());
    }


    private final Connection cnx;

    public JdbcConnection(Connection cnx) {
        this.cnx = cnx;
    }

    public ResultSet query(String sql, Object... params) throws SQLException {
        return query(cnx, sql, params);
    }

    public ResultSet queryList(String sql, Iterable<Object> params) throws SQLException {
        return queryList(cnx, sql, params);
    }

    public int update(String sql, Object... params) throws SQLException {
        return update(cnx, sql, params);
    }

    public int updateList(String sql, Iterable<Object> params) throws SQLException {
        return updateList(cnx, sql, params);
    }

    public ResultSet execute(Sql.Query query) throws SQLException {
        return execute(cnx, query);
    }

    public int execute(Sql.UpdateQuery query) throws SQLException {
        return execute(cnx, query);
    }

    public void close() throws SQLException {
        cnx.close();
    }

}
