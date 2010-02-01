package fr.zenexity.dbhelper;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class JdbcIteratorTest {

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

    public JdbcIteratorTest() {
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

    @Test
    public void iterator() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (Entry entry : new JdbcIterator<Entry>(jdbc.execute(query), Entry.class, "distName", "version")) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void list() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (Entry entry : new JdbcIterator<Entry>(jdbc.execute(query), Entry.class, "distName", "version").list()) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void iteratorAutoMapping() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("distName", "version").from(Entry.class);

        for (Entry entry : new JdbcIterator<Entry>(jdbc.execute(query), Entry.class)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void windowIterator() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class).orderBy("version");

        for (Entry entry : new JdbcWindowIterator<Entry>(jdbc.execute(query), 1, 3, Entry.class, "distName", "version")) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void primitiveIteratorDistName() throws SQLException {
        Set<String> distNames = new HashSet<String>();
        distNames.add("Debian");
        distNames.add("Ubuntu");
        distNames.add("Fedora");
        distNames.add("Mandriva");
        distNames.add("Slackware");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (String distName : new JdbcIterator<String>(jdbc.execute(query), String.class, "distName")) {
            assertTrue(distNames.contains(distName));
            distNames.remove(distName);
        }

        assertEquals(0, distNames.size());
    }

    @Test
    public void primitiveIteratorVersion() throws SQLException {
        Set<String> versions = new HashSet<String>();
        versions.add("5.0");
        versions.add("9.10");
        versions.add("12");
        versions.add("2010");
        versions.add("13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (String version : new JdbcIterator<String>(jdbc.execute(query), String.class, "version")) {
            assertTrue(versions.contains(version));
            versions.remove(version);
        }

        assertEquals(0, versions.size());
    }

}
