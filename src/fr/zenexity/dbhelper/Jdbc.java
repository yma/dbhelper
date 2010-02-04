package fr.zenexity.dbhelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Jdbc {

    public static <T> JdbcIterator<T> iterator(ResultSet result, JdbcResult.Factory<T> resultFactory) throws SQLException {
        return new JdbcIterator<T>(result, resultFactory);
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass) throws SQLException {
        return new JdbcIterator<T>(result, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass, String... fields) throws SQLException {
        return new JdbcIterator<T>(result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterator(ResultSet result, Class<T> resultClass, List<String> fields) throws SQLException {
        return new JdbcIterator<T>(result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator.Window<T> iterator(ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws SQLException {
        return new JdbcIterator.Window<T>(result, offset, size, resultFactory);
    }

    public static <T> JdbcIterator.Window<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass) throws SQLException {
        return new JdbcIterator.Window<T>(result, offset, size, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator.Window<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass, String... fields) throws SQLException {
        return new JdbcIterator.Window<T>(result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator.Window<T> iterator(ResultSet result, int offset, int size, Class<T> resultClass, List<String> fields) throws SQLException {
        return new JdbcIterator.Window<T>(result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

}
