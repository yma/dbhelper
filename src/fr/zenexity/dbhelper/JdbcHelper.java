package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JdbcHelper {

    public static PreparedStatement prepareStatement(Connection cnx, String sql) throws SQLException {
        PreparedStatement pst = cnx.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        return pst;
    }

    public static void params(PreparedStatement pst, Object... params) throws SQLException {
        int index = 0;
        for (Object param : params)
            pst.setObject(++index, param);
    }

    public static void paramsList(PreparedStatement pst, List<Object> params) throws SQLException {
        int index = 0;
        for (Object param : params)
            pst.setObject(++index, param);
    }


    public static ResultSet query(Connection cnx, String sql, Object... params) throws SQLException {
        PreparedStatement pst = prepareStatement(cnx, sql);
        params(pst, params);
        return pst.executeQuery();
    }

    public static ResultSet queryList(Connection cnx, String sql, List<Object> params) throws SQLException {
        PreparedStatement pst = prepareStatement(cnx, sql);
        paramsList(pst, params);
        return pst.executeQuery();
    }

    public static int update(Connection cnx, String sql, Object... params) throws SQLException {
        PreparedStatement pst = prepareStatement(cnx, sql);
        params(pst, params);
        return pst.executeUpdate();
    }

    public static int updateList(Connection cnx, String sql, List<Object> params) throws SQLException {
        PreparedStatement pst = prepareStatement(cnx, sql);
        paramsList(pst, params);
        return pst.executeUpdate();
    }


    public static ResultSet execute(Connection cnx, Sql.Query query) throws SQLException {
        return queryList(cnx, query.toString(), query.params());
    }

    public static int execute(Connection cnx, Sql.Update query) throws SQLException {
        return updateList(cnx, query.toString(), query.params());
    }


    private final Connection cnx;

    public JdbcHelper(Connection cnx) {
        this.cnx = cnx;
    }

    public ResultSet query(String sql, Object... params) throws SQLException {
        return query(cnx, sql, params);
    }

    public ResultSet queryList(String sql, List<Object> params) throws SQLException {
        return queryList(cnx, sql, params);
    }

    public int update(String sql, Object... params) throws SQLException {
        return update(cnx, sql, params);
    }

    public int updateList(String sql, List<Object> params) throws SQLException {
        return updateList(cnx, sql, params);
    }

    public ResultSet execute(Sql.Query query) throws SQLException {
        return execute(cnx, query);
    }

    public int execute(Sql.Update query) throws SQLException {
        return execute(cnx, query);
    }

}
