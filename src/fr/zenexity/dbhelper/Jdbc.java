package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Jdbc {

    public static <T> JdbcIterator<T> iterate(ResultSet result, JdbcResult.Factory<T> resultFactory) throws SQLException {
        return new JdbcIterator<T>(result, resultFactory);
    }

    public static <T> JdbcIterator<T> iterate(ResultSet result, Class<T> resultClass) throws SQLException {
        return new JdbcIterator<T>(result, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterate(ResultSet result, Class<T> resultClass, String... fields) throws SQLException {
        return new JdbcIterator<T>(result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterate(ResultSet result, Class<T> resultClass, List<String> fields) throws SQLException {
        return new JdbcIterator<T>(result, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterate(ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws SQLException {
        return new JdbcIterator.Window<T>(result, offset, size, resultFactory);
    }

    public static <T> JdbcIterator<T> iterate(ResultSet result, int offset, int size, Class<T> resultClass) throws SQLException {
        return new JdbcIterator.Window<T>(result, offset, size, JdbcResult.buildFactory(resultClass));
    }

    public static <T> JdbcIterator<T> iterate(ResultSet result, int offset, int size, Class<T> resultClass, String... fields) throws SQLException {
        return new JdbcIterator.Window<T>(result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public static <T> JdbcIterator<T> iterate(ResultSet result, int offset, int size, Class<T> resultClass, List<String> fields) throws SQLException {
        return new JdbcIterator.Window<T>(result, offset, size, JdbcResult.buildFactory(resultClass, fields));
    }


    // Instance

    public final JdbcConnection connection;

    public Jdbc(Connection connection) {
        this(new JdbcConnection(connection));
    }

    public Jdbc(JdbcConnection connection) {
        this.connection = connection;
    }

    public <T> JdbcIterator<T> iterate(Sql.Query query, JdbcResult.Factory<T> resultFactory) throws SQLException {
        return new JdbcIterator<T>(connection.execute(query), resultFactory);
    }

    public <T> JdbcIterator<T> iterate(Sql.Query query, Class<T> resultClass) throws SQLException {
        return new JdbcIterator<T>(connection.execute(query), JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> iterate(Sql.Query query, Class<T> resultClass, String... fields) throws SQLException {
        return new JdbcIterator<T>(connection.execute(query), JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> iterate(Sql.Query query, Class<T> resultClass, List<String> fields) throws SQLException {
        return new JdbcIterator<T>(connection.execute(query), JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> iterate(Sql.Query query, int offset, int size, JdbcResult.Factory<T> resultFactory) throws SQLException {
        return new JdbcIterator.Window<T>(connection.execute(query), offset, size, resultFactory);
    }

    public <T> JdbcIterator<T> iterate(Sql.Query query, int offset, int size, Class<T> resultClass) throws SQLException {
        return new JdbcIterator.Window<T>(connection.execute(query), offset, size, JdbcResult.buildFactory(resultClass));
    }

    public <T> JdbcIterator<T> iterate(Sql.Query query, int offset, int size, Class<T> resultClass, String... fields) throws SQLException {
        return new JdbcIterator.Window<T>(connection.execute(query), offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public <T> JdbcIterator<T> iterate(Sql.Query query, int offset, int size, Class<T> resultClass, List<String> fields) throws SQLException {
        return new JdbcIterator.Window<T>(connection.execute(query), offset, size, JdbcResult.buildFactory(resultClass, fields));
    }

    public int execute(Sql.UpdateQuery query) throws SQLException {
        return connection.execute(query);
    }

}
