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

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"))) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void list() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).list()) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void first() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("distName"));
        JdbcIterator<Entry> it = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));
        Entry entry = it.first();
        assertEquals("Debian", entry.distName);
        assertEquals("5.0", entry.version);
        assertFalse(it.hasNext());
    }

    @Test
    public void firstNotFound() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).where("distName=?","yop").orderBy("distName"));
        JdbcIterator<Entry> it = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertNull(it.first());
        assertFalse(it.hasNext());
    }

    @Test
    public void windowIterator_all() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 0, 5, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertTrue(entry.hasNext());
        assertEquals("Fedora", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Slackware", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Mandriva", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Debian", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Ubuntu", entry.next().distName);
        assertFalse(entry.hasNext());
    }

    @Test
    public void windowIterator_less() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 0, 3, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertTrue(entry.hasNext());
        assertEquals("Fedora", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Slackware", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Mandriva", entry.next().distName);
        assertFalse(entry.hasNext());
    }

    @Test
    public void windowIterator_more() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 0, 100, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertTrue(entry.hasNext());
        assertEquals("Fedora", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Slackware", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Mandriva", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Debian", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Ubuntu", entry.next().distName);
        assertFalse(entry.hasNext());
    }

    @Test
    public void windowIterator_cut() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 2, 2, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertTrue(entry.hasNext());
        assertEquals("Mandriva", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Debian", entry.next().distName);
        assertFalse(entry.hasNext());
    }

    @Test
    public void windowIterator_cutOut() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 4, 2, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertTrue(entry.hasNext());
        assertEquals("Ubuntu", entry.next().distName);
        assertFalse(entry.hasNext());
    }

    @Test
    public void windowIterator_outOfRange() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 5, 2, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertFalse(entry.hasNext());
    }

    @Test
    public void windowIterator_cutZero() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 1, 0, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertFalse(entry.hasNext());
    }

}
