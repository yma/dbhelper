package fr.zenexity.dbhelper;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class JdbcIteratorTest extends TestingDatabase {

    @Test
    public void iterator() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (Entry entry : new JdbcIterator<Entry>(jdbc.connection.execute(query), JdbcResult.classFactory(Entry.class, "distName", "version"))) {
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

        for (Entry entry : new JdbcIterator<Entry>(jdbc.connection.execute(query), JdbcResult.classFactory(Entry.class, "distName", "version")).list()) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void first() throws SQLException {
        Sql.Select query = Sql.selectAll().from(Entry.class).orderBy("distName");
        Entry entry = new JdbcIterator<Entry>(jdbc.connection.execute(query), JdbcResult.classFactory(Entry.class, "distName", "version")).first();
        assertEquals("Debian", entry.distName);
        assertEquals("5.0", entry.version);
    }

    @Test
    public void firstNotFound() throws SQLException {
        Sql.Select query = Sql.selectAll().from(Entry.class).where("distName=?","yop").orderBy("distName");
        Entry entry = new JdbcIterator<Entry>(jdbc.connection.execute(query), JdbcResult.classFactory(Entry.class, "distName", "version")).first();
        assertNull(entry);
    }

    @Test
    public void windowIterator() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class).orderBy("version");

        for (Entry entry : new JdbcIterator.Window<Entry>(jdbc.connection.execute(query), 1, 3, JdbcResult.classFactory(Entry.class, "distName", "version"))) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

}
