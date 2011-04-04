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
    public void seek() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).seek(2)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void seekFirst() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).seek(0)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void seekLast() {
        Map<String, String> linux = new HashMap<String, String>();

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).seek(-1)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void seekBeforeLast() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).seek(-2)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void seekLastFirst() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).seek(-1).seek(0)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void offset() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).offset(2)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void offsetBeforeFisrt() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).offset(-2)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void offsetAfterLast() {
        Map<String, String> linux = new HashMap<String, String>();

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).offset(7)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void offsetZero() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        for (Entry entry : new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version")).offset(0)) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void position() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));

        assertEquals(0, iterator.position());
        assertTrue(iterator.hasNext());
        iterator.next();
        assertEquals(1, iterator.position());
        assertTrue(iterator.hasNext());
        iterator.next();
        assertEquals(2, iterator.position());
        assertTrue(iterator.hasNext());
        iterator.next();
        assertEquals(3, iterator.position());
        assertTrue(iterator.hasNext());
        iterator.next();
        assertEquals(4, iterator.position());
        assertTrue(iterator.hasNext());
        iterator.next();
        assertEquals(5, iterator.position());
        assertFalse(iterator.hasNext());

        iterator.close();
    }

    @Test
    public void positionSeekAll() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));
        for (int i = 0; i <= 5; i++) {
            int position = iterator.seek(i).position();
            assertEquals(i, position);
        }
        iterator.close();
    }

    @Test
    public void positionSeekAfterLast() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));
        for (int i = 6; i < 10; i++) {
            int position = iterator.seek(i).position();
            assertTrue(position == 0 || position == 6);
        }
        iterator.close();
    }

    @Test
    public void positionSeekBeforeZero() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));
        for (int i = 0; i > -5; i--) {
            int position = iterator.offset(i).position();
            assertEquals(0, position);
        }
        iterator.close();
    }

    @Test
    public void positionSeekEnd() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertEquals(5, iterator.seek(-1).position());
        iterator.close();
    }

    @Test
    public void limit() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"))
                .limit(3);
        for (Entry entry : iterator) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }
        assertFalse(iterator.hasNext());

        iterator.limit(-2);
        assertFalse(iterator.hasNext());

        iterator.close();
        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void limitBeforeZero() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"))
                .offset(-3).limit(3);
        for (Entry entry : iterator) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }
        assertFalse(iterator.hasNext());

        iterator.close();
        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void limitCutted() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"))
                .offset(3).limit(3);
        for (Entry entry : iterator) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }
        assertFalse(iterator.hasNext());

        iterator.close();
        assertEquals(new HashMap<String, String>(), linux);
    }

    @Test
    public void limitZero() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"))
                .limit(0);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void limitKeepOpen() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class));
        JdbcIterator<Entry> iterator = new JdbcIterator<Entry>(qs.statement, qs.executeQuery(), JdbcResult.classFactory(Entry.class, "distName", "version"))
                .keepOpen().limit(3);
        for (Entry entry : iterator) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }
        assertFalse(iterator.hasNext());

        iterator.limit(-1);
        for (Entry entry : iterator) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }
        assertFalse(iterator.hasNext());

        iterator.seek(0).limit(1);
        assertTrue(iterator.hasNext());
        assertEquals("Debian", iterator.next().distName);
        assertFalse(iterator.hasNext());

        iterator.limit(-2);
        assertTrue(iterator.hasNext());

        iterator.close();
        assertEquals(new HashMap<String, String>(), linux);
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

    @Test
    public void windowIterator_beforeZero() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), -2, 5, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertTrue(entry.hasNext());
        assertEquals("Fedora", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Slackware", entry.next().distName);
        assertTrue(entry.hasNext());
        assertEquals("Mandriva", entry.next().distName);
        assertFalse(entry.hasNext());
    }

    @Test
    public void windowIterator_allBeforeZero() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), -5, 5, JdbcResult.classFactory(Entry.class, "distName", "version"));
        assertFalse(entry.hasNext());
    }

    @Test
    public void windowIterator_keepOpen() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 0, 5, JdbcResult.classFactory(Entry.class, "distName", "version"));
        try {
            entry.keepOpen();
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals("Can't use keepOpen with JdbcIterator.Window", e.getMessage());
        }
    }

    @Test
    public void windowIterator_seek() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 0, 5, JdbcResult.classFactory(Entry.class, "distName", "version"));
        try {
            entry.seek(0);
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals("Can't use seek with JdbcIterator.Window", e.getMessage());
        }
    }

    @Test
    public void windowIterator_offset() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 0, 5, JdbcResult.classFactory(Entry.class, "distName", "version"));
        try {
            entry.offset(0);
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals("Can't use offset with JdbcIterator.Window", e.getMessage());
        }
    }

    @Test
    public void windowIterator_limit() {
        JdbcStatement qs = jdbc.newStatement(Sql.select("*").from(Entry.class).orderBy("version"));
        JdbcIterator<Entry> entry = new JdbcIterator.Window<Entry>(qs.statement, qs.executeQuery(), 0, 5, JdbcResult.classFactory(Entry.class, "distName", "version"));
        try {
            entry.limit(0);
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals("Can't use limit with JdbcIterator.Window", e.getMessage());
        }
    }

}
