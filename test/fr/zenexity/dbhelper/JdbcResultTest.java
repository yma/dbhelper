package fr.zenexity.dbhelper;

import static org.junit.Assert.*;

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

        for (String distName : jdbc.iterate(query, JdbcResult.primitiveFactory(String.class, "distName"))) {
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

        for (String version : jdbc.iterate(query, JdbcResult.primitiveFactory(String.class, "version"))) {
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

        for (Entry entry : jdbc.iterate(query, JdbcResult.classFactory(Entry.class))) {
            assertEquals(linux.get(entry.distName), entry.version);
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

        for (Map<String, Object> entry : jdbc.iterate(query, JdbcResult.mapFactory())) {
            assertEquals(linux.get(entry.get("distName")), entry.get("version"));
            linux.remove(entry.get("distName"));
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testMapFacoryWithFields() throws SQLException {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.selectAll().from(Entry.class);

        for (Map<String, Object> entry : jdbc.iterate(query, JdbcResult.mapFactory("distName", "version"))) {
            Set<String> keys = entry.keySet();
            assertTrue(keys.contains("distName"));
            assertTrue(keys.contains("version"));
            assertEquals(linux.get(entry.get("distName")), entry.get("version"));
            linux.remove(entry.get("distName"));
        }

        assertEquals(0, linux.size());
    }

}
