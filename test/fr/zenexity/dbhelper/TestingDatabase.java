package fr.zenexity.dbhelper;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Before;

public class TestingDatabase {

    protected final JdbcHelper jdbc;

    public static class Entry {
        public String distName;
        public String version;
    }

    public static JdbcHelper initdb_HSQLMEM() throws ClassNotFoundException, SQLException {
        Class.forName("org.hsqldb.jdbcDriver");
        JdbcHelper jdbc = new JdbcHelper(DriverManager.getConnection("jdbc:hsqldb:mem:aname", "sa", ""));
        jdbc.update("DROP TABLE IF EXISTS Entry");
        jdbc.update("CREATE TABLE Entry (distName VARCHAR(255) DEFAULT NULL, version VARCHAR(255))");
        return jdbc;
    }

    public TestingDatabase() {
        try {
            jdbc = initdb_HSQLMEM();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void loadData() throws SQLException {
        jdbc.update("DELETE FROM Entry");
        Sql.Insert entry = Sql.insert().into(Entry.class).columns("distName", "version");

        jdbc.execute(new Sql.Insert(entry).value("?", "Debian").value("?", "5.0"));
        jdbc.execute(new Sql.Insert(entry).value("?", "Ubuntu").value("?", "9.10"));
        jdbc.execute(new Sql.Insert(entry).value("?", "Fedora").value("?", "12"));
        jdbc.execute(new Sql.Insert(entry).value("?", "Mandriva").value("?", "2010"));
        jdbc.execute(new Sql.Insert(entry).value("?", "Slackware").value("?", "13.0"));
    }

}
