package fr.zenexity.dbhelper;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class JdbcResultTest extends TestingDatabase {

    @Test
    public void testPrimitiveFactory_distName() throws SQLException {
        Set<String> distNames = new HashSet<String>();
        distNames.add("Debian");
        distNames.add("Ubuntu");
        distNames.add("Fedora");
        distNames.add("Mandriva");
        distNames.add("Slackware");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (String distName : jdbc.execute(query, JdbcResult.primitiveFactory(String.class, "distName"))) {
            assertTrue(distNames.contains(distName));
            distNames.remove(distName);
        }

        assertEquals(0, distNames.size());
    }

    @Test
    public void testPrimitiveFactory_version() throws SQLException {
        Set<String> versions = new HashSet<String>();
        versions.add("5.0");
        versions.add("9.10");
        versions.add("12");
        versions.add("2010");
        versions.add("13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (String version : jdbc.execute(query, JdbcResult.primitiveFactory(String.class, "version"))) {
            assertTrue(versions.contains(version));
            versions.remove(version);
        }

        assertEquals(0, versions.size());
    }

    @Test
    public void testClassFactoryAutoMapping() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("distName", "version").from(Entry.class);

        for (Entry entry : jdbc.execute(query, JdbcResult.classFactory(Entry.class))) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testClassFactorySelectAll() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (Entry entry : jdbc.execute(query, JdbcResult.classFactory(Entry.class))) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testClassFactoryWithFields() throws SQLException {
        Set<String> linux = new HashSet<String>();
        linux.add("Debian");
        linux.add("Ubuntu");
        linux.add("Fedora");
        linux.add("Mandriva");
        linux.add("Slackware");

        Sql.Select query = Sql.select("distName", "version").from(Entry.class);

        for (Entry entry : jdbc.execute(query, JdbcResult.classFactory(Entry.class, "distName"))) {
            assertTrue(linux.contains(entry.distName));
            assertNull(entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testClassFactoryWithUnknownField() throws SQLException {
        Sql.Select query = Sql.select("distName", "version").from(Entry.class);
        JdbcStatement st = jdbc.newStatement(query);
        ResultSet rs = st.executeQuery();
        JdbcResult.Factory<Entry> factory = JdbcResult.classFactory(Entry.class, "distName", "yop");
        try {
            factory.init(rs);
            fail("JdbcException expected");
        } catch (JdbcResultException e) {
            Throwable cause = e.getCause();
            assertEquals(NoSuchFieldException.class, cause.getClass());
            assertEquals("yop", cause.getMessage());
        } finally {
            rs.close();
            st.close();
        }
    }

    @Test
    public void testClassFactoryWithTransientField() throws SQLException {
        Sql.Select query = Sql.selectAll().from(Entry.class);
        JdbcStatement st = jdbc.newStatement(query);
        ResultSet rs = st.executeQuery();
        JdbcResult.Factory<EntryWithTransient> factory = JdbcResult.classFactory(EntryWithTransient.class);
        try {
            factory.init(rs);
            fail("JdbcException expected");
        } catch (JdbcResultException e) {
            Throwable cause = e.getCause();
            assertEquals(IllegalArgumentException.class, cause.getClass());
            assertEquals("version", cause.getMessage());
        } finally {
            rs.close();
            st.close();
        }
    }

    @Test
    public void testClassFactoryWithoutTransientField() throws SQLException {
        Set<String> linux = new HashSet<String>();
        linux.add("Debian");
        linux.add("Ubuntu");
        linux.add("Fedora");
        linux.add("Mandriva");
        linux.add("Slackware");

        Sql.Select query = Sql.select("distName").from(Entry.class);

        for (EntryWithTransient entry : jdbc.execute(query, JdbcResult.classFactory(EntryWithTransient.class))) {
            assertTrue(linux.contains(entry.distName));
            assertNull(entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testMapFactory() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (Map<String, Object> entry : jdbc.execute(query, JdbcResult.mapFactory())) {
            assertEquals(linux.get(entry.get("distName")), entry.get("version"));
            linux.remove(entry.get("distName"));
        }

        assertEquals(0, linux.size());
    }

    public static class EntryWithTransient {
        public String distName;
        public transient String version;
    }

}
