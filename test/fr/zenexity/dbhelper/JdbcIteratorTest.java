package fr.zenexity.dbhelper;

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
    public void iterator() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.selectAll().from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"))) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void list() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.selectAll().from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).list()) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void first() {
        JdbcStatement qs = jdbc.newStatement(Sql.selectAll().from(Entry.class).orderBy("distName"));
        JdbcIterator<Entry> it = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));
        Entry entry = it.first();
        assertEquals("Debian", entry.distName);
        assertEquals("5.0", entry.version);
        assertFalse(it.hasNext());
    }

    @Test
    public void firstNotFound() {
        JdbcStatement qs = jdbc.newStatement(Sql.selectAll().from(Entry.class).where("distName=?","yop").orderBy("distName"));
        JdbcIterator<Entry> it = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertNull(it.first());
        assertFalse(it.hasNext());
    }

    @Test
    public void windowIterator() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.selectAll().from(Entry.class).orderBy("version"));
        for (Entry entry : new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 1, 3, JdbcResult.classFactory(Entry.class, "distName", "version"))) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

}
