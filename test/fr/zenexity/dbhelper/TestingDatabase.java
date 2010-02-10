package fr.zenexity.dbhelper;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;

public class TestingDatabase {

    protected final Jdbc jdbc;

    public static class Entry {
        public String distName;
        public String version;
    }

    public static JdbcConnection initdb_HSQLMEM() throws ClassNotFoundException, SQLException {
        Class.forName("org.hsqldb.jdbcDriver");
        JdbcConnection jdbcConnection = new JdbcConnection(DriverManager.getConnection("jdbc:hsqldb:mem:aname", "sa", ""));
        jdbcConnection.update("DROP TABLE IF EXISTS Entry");
        jdbcConnection.update("CREATE TABLE Entry (distName VARCHAR(255) DEFAULT NULL, version VARCHAR(255))");
        return jdbcConnection;
    }

    public TestingDatabase() {
        try {
            jdbc = new Jdbc(initdb_HSQLMEM());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void loadData() throws SQLException {
        jdbc.connection.update("DELETE FROM Entry");
        Sql.Insert entry = Sql.insert().into(Entry.class).columns("distName", "version");

        jdbc.execute(Sql.clone(entry).values("Debian", "5.0"));
        jdbc.execute(Sql.clone(entry).values("Ubuntu", "9.10"));
        jdbc.execute(Sql.clone(entry).values("Fedora", "12"));
        jdbc.execute(Sql.clone(entry).values("Mandriva", "2010"));
        jdbc.execute(Sql.clone(entry).values("Slackware", "13.0"));
    }

    @After
    public void closeConnection() throws SQLException {
        jdbc.connection.close();
    }

}
