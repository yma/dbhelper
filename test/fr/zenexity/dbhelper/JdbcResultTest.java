package fr.zenexity.dbhelper;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class JdbcResultTest extends TestingDatabase {

    @Test
    public void testPrimitive() {
        assertTrue(JdbcResult.primitive(String.class));
        assertTrue(JdbcResult.primitive(Long.class));
        assertTrue(JdbcResult.primitive(Integer.class));
        assertTrue(JdbcResult.primitive(Short.class));
        assertTrue(JdbcResult.primitive(Byte.class));
        assertTrue(JdbcResult.primitive(Double.class));
        assertTrue(JdbcResult.primitive(Float.class));
        assertTrue(JdbcResult.primitive(Character.class));
        assertTrue(JdbcResult.primitive(Boolean.class));

        assertTrue(JdbcResult.primitive(long.class));
        assertTrue(JdbcResult.primitive(int.class));
        assertTrue(JdbcResult.primitive(short.class));
        assertTrue(JdbcResult.primitive(byte.class));
        assertTrue(JdbcResult.primitive(double.class));
        assertTrue(JdbcResult.primitive(float.class));
        assertTrue(JdbcResult.primitive(char.class));
        assertTrue(JdbcResult.primitive(boolean.class));

        assertFalse(JdbcResult.primitive(JdbcResult.class));
    }

    @Test
    public void testPrimitiveFactory_distName() {
        Set<String> distNames = new HashSet<String>();
        distNames.add("Debian");
        distNames.add("Ubuntu");
        distNames.add("Fedora");
        distNames.add("Mandriva");
        distNames.add("Slackware");

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (String distName : jdbc.execute(query, JdbcResult.primitiveFactory(String.class, "distName"))) {
            assertTrue(distNames.contains(distName));
            distNames.remove(distName);
        }

        assertEquals(0, distNames.size());
    }

    @Test
    public void testPrimitiveFactory_version() {
        Set<String> versions = new HashSet<String>();
        versions.add("5.0");
        versions.add("9.10");
        versions.add("12");
        versions.add("2010");
        versions.add("13.0");

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (String version : jdbc.execute(query, JdbcResult.primitiveFactory(String.class, "version"))) {
            assertTrue(versions.contains(version));
            versions.remove(version);
        }

        assertEquals(0, versions.size());
    }

    @Test
    public void testPrimitiveFactory_num() {
        Set<Double> nums = new HashSet<Double>();
        nums.add(5.0);
        nums.add(9.10);
        nums.add(12.0);
        nums.add(2010.0);
        nums.add(13.0);

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (Double num : jdbc.execute(query, JdbcResult.primitiveFactory(Double.class, "num"))) {
            assertTrue(nums.contains(num));
            nums.remove(num);
        }

        assertEquals(0, nums.size());
    }

    @Test
    public void testPrimitiveFactory_withNull() {
        assertEquals(1, jdbc.execute(Sql.insert(Entry.class).set("distName", null)));
        assertEquals(1, jdbc.execute(Sql.insert(Entry.class).set("distName", null)));

        Sql.Select query = Sql.select("*").from(Entry.class).orderBy("distName");
        JdbcIterator<String> entry = jdbc.execute(query, 0, 3, JdbcResult.primitiveFactory(String.class, "distName"));
        assertTrue(entry.hasNext());
        assertEquals(null, entry.next());
        assertTrue(entry.hasNext());
        assertEquals(null, entry.next());
        assertTrue(entry.hasNext());
        assertEquals("Debian", entry.next());
        assertFalse(entry.hasNext());
    }

    @Test
    public void testPrimitivePrimitiveFactory_num() {
        Set<Double> nums = new HashSet<Double>();
        nums.add(5.0);
        nums.add(9.10);
        nums.add(12.0);
        nums.add(2010.0);
        nums.add(13.0);

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (double num : jdbc.execute(query, JdbcResult.primitiveFactory(double.class, "num"))) {
            assertTrue(nums.contains(num));
            nums.remove(num);
        }

        assertEquals(0, nums.size());
    }

    @Test
    public void testPrimitiveFactory_num_badType() {
        try {
            Sql.Select query = Sql.select("*").from(Entry.class);
            jdbc.execute(query, JdbcResult.primitiveFactory(Float.class, "num")).first();
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals(JdbcResultException.class, e.getCause().getClass());
            assertEquals("num[3]", e.getCause().getMessage());
            assertEquals(JdbcValueException.class, e.getCause().getCause().getClass());
            assertEquals("5.0 (java.lang.Double) to java.lang.Float", e.getCause().getCause().getMessage());
            assertEquals(ClassCastException.class, e.getCause().getCause().getCause().getClass());
        }
    }

    @Test
    public void testPrimitivePrimitiveFactory_num_badType() {
        try {
            Sql.Select query = Sql.select("*").from(Entry.class);
            jdbc.execute(query, JdbcResult.primitiveFactory(float.class, "num")).first().getClass();
            fail("ClassCastException expected");
        } catch (ClassCastException e) {
            assertEquals("java.lang.Double cannot be cast to java.lang.Float", e.getMessage());
        }
    }

    @Test
    public void testPrimitiveFactory_EnumByName() {
        Set<Entry.DistType> types = new HashSet<Entry.DistType>();
        types.add(Entry.DistType.DEBIAN);
        types.add(Entry.DistType.UBUNTU);
        types.add(Entry.DistType.FEDORA);
        types.add(Entry.DistType.MANDRIVA);
        types.add(Entry.DistType.SLACKWARE);

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (Entry.DistType type : jdbc.execute(query, JdbcResult.primitiveFactory(Entry.DistType.class, "typeName"))) {
            assertTrue(types.contains(type));
            types.remove(type);
        }

        assertEquals(0, types.size());
    }

    @Test
    public void testPrimitiveFactory_EnumByOrdinal() {
        Set<Entry.DistType> types = new HashSet<Entry.DistType>();
        types.add(Entry.DistType.DEBIAN);
        types.add(Entry.DistType.UBUNTU);
        types.add(Entry.DistType.FEDORA);
        types.add(Entry.DistType.MANDRIVA);
        types.add(Entry.DistType.SLACKWARE);

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (Entry.DistType type : jdbc.execute(query, JdbcResult.primitiveFactory(Entry.DistType.class, "typeOrdinal"))) {
            assertTrue(types.contains(type));
            types.remove(type);
        }

        assertEquals(0, types.size());
    }

    @Test
    public void testClassFactoryAutoMapping() {
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
    public void testClassFactorySelectAll() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (Entry entry : jdbc.execute(query, JdbcResult.classFactory(Entry.class))) {
            assertEquals(linux.get(entry.distName), entry.version);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testClassFactorySelectAllTooManyColumns() {
        try {
            Sql.Select query = Sql.select("*").from(Entry.class);
            jdbc.execute(query, JdbcResult.classFactory(TinyEntry.class));
            fail("JdbcIteratorException expected");
        } catch(JdbcIteratorException e) {
            assertEquals(JdbcResultException.class, e.getCause().getClass());
            assertEquals(NoSuchFieldException.class, e.getCause().getCause().getClass());
        }
    }

    @Test
    public void testClassFactorySelectAllPrimitive() {
        Map<String, Double> linux = new HashMap<String, Double>();
        linux.put("Debian", 5.0);
        linux.put("Ubuntu", 9.10);
        linux.put("Fedora", 12.0);
        linux.put("Mandriva", 2010.0);
        linux.put("Slackware", 13.0);

        Sql.Select query = Sql.select("distName", "num").from(Entry.class);

        for (PrimitiveEntry entry : jdbc.execute(query, JdbcResult.classFactory(PrimitiveEntry.class))) {
            assertEquals(linux.get(entry.distName).doubleValue(), entry.num, 0);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testClassFactorySelectAllEnums() {
        Map<String, Entry.DistType> linux = new HashMap<String, Entry.DistType>();
        linux.put("Debian", Entry.DistType.DEBIAN);
        linux.put("Ubuntu", Entry.DistType.UBUNTU);
        linux.put("Fedora", Entry.DistType.FEDORA);
        linux.put("Mandriva", Entry.DistType.MANDRIVA);
        linux.put("Slackware", Entry.DistType.SLACKWARE);

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (Entry entry : jdbc.execute(query, JdbcResult.classFactory(Entry.class))) {
            assertEquals(linux.get(entry.distName), entry.typeName);
            assertEquals(linux.get(entry.distName), entry.typeOrdinal);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testClassFactorySelectAllWithCastError() {
        try {
            Sql.Select query = Sql.select("num").from(Entry.class);
            jdbc.execute(query, JdbcResult.classFactory(BadCastedEntry.class)).first();
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals(JdbcResultException.class, e.getCause().getClass());
            assertEquals("num[1]", e.getCause().getMessage());
            assertEquals(JdbcValueException.class, e.getCause().getCause().getClass());
            assertEquals("5.0 (java.lang.Double) to java.lang.Float", e.getCause().getCause().getMessage());
            assertEquals(ClassCastException.class, e.getCause().getCause().getCause().getClass());
        }
    }

    @Test
    public void testClassFactoryWithFields() {
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
        Sql.Select query = Sql.select("distName", "version").from(Entry.class);
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
    public void testClassFactoryWithoutTransientField() {
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
    public void testClassFactoryWithExEntry() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("*").from(ExEntry.class);

        for (ExEntry entry : jdbc.execute(query, JdbcResult.classFactory(ExEntry.class))) {
            assertEquals(linux.get(entry.distName), entry.version);
            assertEquals(entry.distName +" "+ entry.version, entry.fullName);
            linux.remove(entry.distName);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testClassFactoryWithTextClob() {
        Set<String> linux = new HashSet<String>();
        linux.add("Debian 5.0");
        linux.add("Ubuntu 9.10");
        linux.add("Fedora 12");
        linux.add("Mandriva 2010");
        linux.add("Slackware 13.0");

        Sql.Select query = Sql.select("textClob").from(EntryClob.class);

        for (EntryClob entry : jdbc.execute(query, JdbcResult.classFactory(EntryClob.class))) {
            assertTrue(linux.contains(entry.textClob));
            linux.remove(entry.textClob);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testMapFactory() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("*").from(Entry.class);

        for (Map<String, Object> entry : jdbc.execute(query, JdbcResult.mapFactory())) {
            assertEquals(linux.get(entry.get("distName")), entry.get("version"));
            linux.remove(entry.get("distName"));
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testListFactory() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("distName", "version").from(Entry.class);

        for (List<Object> entry : jdbc.execute(query, JdbcResult.listFactory())) {
            assertEquals(String.class, entry.get(0).getClass());
            assertEquals(String.class, entry.get(1).getClass());
            assertEquals(linux.get(entry.get(0)), entry.get(1));
            linux.remove(entry.get(0));
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testListFactoryWithFields() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("num", "version", "typeOrdinal", "distName").from(Entry.class);

        for (List<Object> entry : jdbc.execute(query, JdbcResult.listFactory("Version", "distname"))) {
            assertEquals(linux.get(entry.get(1)), entry.get(0));
        }

        for (List<Object> entry : jdbc.execute(query, JdbcResult.listFactory("distname", "Version"))) {
            assertEquals(String.class, entry.get(0).getClass());
            assertEquals(String.class, entry.get(1).getClass());
            assertEquals(linux.get(entry.get(0)), entry.get(1));
            linux.remove(entry.get(0));
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testListFactoryAssertType() {
        Sql.Select query = Sql.select("num", "version", "typeOrdinal", "distName").from(Entry.class);
        for (List<Object> entry : jdbc.execute(query, JdbcResult.listFactory())) {
            assertEquals(Double.class, entry.get(0).getClass());
            assertEquals(String.class, entry.get(1).getClass());
            assertEquals(Integer.class, entry.get(2).getClass());
            assertEquals(String.class, entry.get(3).getClass());
        }
    }

    @Test
    public void testListFactoryTypeCheck() {
        Map<String, Double> linux = new HashMap<String, Double>();
        linux.put("Debian", 5.0);
        linux.put("Ubuntu", 9.10);
        linux.put("Fedora", 12.0);
        linux.put("Mandriva", 2010.0);
        linux.put("Slackware", 13.0);

        Sql.Select query = Sql.select("distName", "num").from(Entry.class);

        for (List<Object> entry : jdbc.execute(query, JdbcResult.listFactory())) {
            assertEquals(String.class, entry.get(0).getClass());
            assertEquals(Double.class, entry.get(1).getClass());
            assertEquals(linux.get(entry.get(0)), entry.get(1));
            linux.remove(entry.get(0));
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testTypedListFactory() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("distName", "version").from(Entry.class);

        for (List<String> entry : jdbc.execute(query, JdbcResult.listFactory(String.class))) {
            assertEquals(String.class, entry.get(0).getClass());
            assertEquals(String.class, entry.get(1).getClass());
            assertEquals(linux.get(entry.get(0)), entry.get(1));
            linux.remove(entry.get(0));
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testTypedListFactoryBadCast() {
        try {
            Sql.Select query = Sql.select("distName", "num").from(Entry.class);
            jdbc.execute(query, JdbcResult.listFactory(String.class)).first();
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals(JdbcResultException.class, e.getCause().getClass());
            assertEquals("List[1]", e.getCause().getMessage());
            assertEquals(JdbcValueException.class, e.getCause().getCause().getClass());
            assertEquals("5.0 (java.lang.Double) to java.lang.String", e.getCause().getCause().getMessage());
            assertEquals(ClassCastException.class, e.getCause().getCause().getCause().getClass());
        }
    }

    @Test
    public void testArrayFactory() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("distName", "version").from(Entry.class);

        for (Object[] entry : jdbc.execute(query, JdbcResult.arrayFactory())) {
            assertEquals(String.class, entry[0].getClass());
            assertEquals(String.class, entry[1].getClass());
            assertEquals(linux.get(entry[0]), entry[1]);
            linux.remove(entry[0]);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testArrayFactoryWithFields() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("num", "version", "typeOrdinal", "distName").from(Entry.class);

        for (Object[] entry : jdbc.execute(query, JdbcResult.arrayFactory("Version", "distname"))) {
            assertEquals(linux.get(entry[1]), entry[0]);
        }

        for (Object[] entry : jdbc.execute(query, JdbcResult.arrayFactory("distname", "Version"))) {
            assertEquals(String.class, entry[0].getClass());
            assertEquals(String.class, entry[1].getClass());
            assertEquals(linux.get(entry[0]), entry[1]);
            linux.remove(entry[0]);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testArrayFactoryAssertType() {
        Sql.Select query = Sql.select("num", "version", "typeOrdinal", "distName").from(Entry.class);
        for (Object[] entry : jdbc.execute(query, JdbcResult.arrayFactory())) {
            assertEquals(Double.class, entry[0].getClass());
            assertEquals(String.class, entry[1].getClass());
            assertEquals(Integer.class, entry[2].getClass());
            assertEquals(String.class, entry[3].getClass());
        }
    }

    @Test
    public void testArrayFactoryTypeCheck() {
        Map<String, Double> linux = new HashMap<String, Double>();
        linux.put("Debian", 5.0);
        linux.put("Ubuntu", 9.10);
        linux.put("Fedora", 12.0);
        linux.put("Mandriva", 2010.0);
        linux.put("Slackware", 13.0);

        Sql.Select query = Sql.select("distName", "num").from(Entry.class);

        for (Object[] entry : jdbc.execute(query, JdbcResult.arrayFactory())) {
            assertEquals(String.class, entry[0].getClass());
            assertEquals(Double.class, entry[1].getClass());
            assertEquals(linux.get(entry[0]), entry[1]);
            linux.remove(entry[0]);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testTypedArrayFactory() {
        Map<String, String> linux = new HashMap<String, String>();
        linux.put("Debian", "5.0");
        linux.put("Ubuntu", "9.10");
        linux.put("Fedora", "12");
        linux.put("Mandriva", "2010");
        linux.put("Slackware", "13.0");

        Sql.Select query = Sql.select("distName", "version").from(Entry.class);

        for (String[] entry : jdbc.execute(query, JdbcResult.arrayFactory(String.class))) {
            assertEquals(String.class, entry[0].getClass());
            assertEquals(String.class, entry[1].getClass());
            assertEquals(linux.get(entry[0]), entry[1]);
            linux.remove(entry[0]);
        }

        assertEquals(0, linux.size());
    }

    @Test
    public void testTypedArrayFactoryBadCast() {
        try {
            Sql.Select query = Sql.select("distName", "num").from(Entry.class);
            jdbc.execute(query, JdbcResult.arrayFactory(String.class)).first();
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals(JdbcResultException.class, e.getCause().getClass());
            assertEquals("Array[1]", e.getCause().getMessage());
            assertEquals(JdbcValueException.class, e.getCause().getCause().getClass());
            assertEquals("5.0 (java.lang.Double) to java.lang.String", e.getCause().getCause().getMessage());
            assertEquals(ClassCastException.class, e.getCause().getCause().getCause().getClass());
        }
    }

    @Test
    public void testCollectionFactoryWithUnknownField() {
        Sql.Select query = Sql.select("num", "version", "typeOrdinal", "distName").from(Entry.class);
        try{
            jdbc.execute(query, JdbcResult.arrayFactory("distname", "Varsion", "yop"));
            fail("JdbcIteratorException expected");
        } catch (JdbcIteratorException e) {
            assertEquals(JdbcResultException.class, e.getCause().getClass());
            assertEquals(NoSuchFieldException.class, e.getCause().getCause().getClass());
            assertEquals("varsion", e.getCause().getCause().getMessage());
        }
    }

    public static class EntryWithTransient {
        public String distName;
        public transient String version;
    }

    public static class PrimitiveEntry {
        public String distName;
        public double num;
    }

    public static class TinyEntry {
        public String distName;
        public String version;
    }

    public static class BadCastedEntry {
        public Float num;
    }

}
