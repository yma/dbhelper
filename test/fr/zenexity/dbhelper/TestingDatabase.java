package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;

public class TestingDatabase {

    protected Jdbc jdbc;

    public static class Entry {
        public String distName;
        public String version;
    }

    public static Connection initdb_HSQLMEM() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:dbhelper", "sa", "");
            JdbcStatement.executeUpdate(connection, "DROP TABLE IF EXISTS Entry");
            JdbcStatement.executeUpdate(connection, "CREATE TABLE Entry (distName VARCHAR(255) DEFAULT NULL, version VARCHAR(255))");
            return connection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void loadData() throws SQLException {
        jdbc = new Jdbc(initdb_HSQLMEM());
        String insertEntry = "INSERT INTO Entry (distName, version) VALUES (?, ?)";
        jdbc.executeUpdate(insertEntry, "Debian", "5.0");
        jdbc.executeUpdate(insertEntry, "Ubuntu", "9.10");
        jdbc.executeUpdate(insertEntry, "Fedora", "12");
        jdbc.executeUpdate(insertEntry, "Mandriva", "2010");
        jdbc.executeUpdate(insertEntry, "Slackware", "13.0");
    }

    @After
    public void closeConnection() throws SQLException {
        jdbc.connection.close();
    }

}
