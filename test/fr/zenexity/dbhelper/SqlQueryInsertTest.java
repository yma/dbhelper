package fr.zenexity.dbhelper;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class SqlQueryInsertTest {

    @Test
    public void testInsertInto() {
        assertEquals("", Sql.insert().toString());
        assertEquals("INSERT INTO x", Sql.insert("x").toString());
        assertEquals("INSERT INTO SqlQueryInsertTest", Sql.insert(SqlQueryInsertTest.class).toString());
    }

    @Test
    public void testReplace() {
        assertEquals("", Sql.replace().toString());
        assertEquals("REPLACE INTO x", Sql.replace("x").toString());
        assertEquals("REPLACE INTO SqlQueryInsertTest", Sql.replace(SqlQueryInsertTest.class).toString());
    }

    @Test
    public void testColumns() {
        assertEquals("(y)", Sql.insert().column("y").toString());
        assertEquals("(y, z)", Sql.insert().column("y").column("z").toString());
        assertEquals("(y, z)", Sql.insert().columns("y", "z").toString());
    }

    @Test
    public void testDefaultValues() {
        assertEquals("DEFAULT VALUES", Sql.insert().defaultValues().toString());
    }

    @Test(expected=NullPointerException.class)
    public void testDefaultValuesNPE1() {
        Sql.insert().value(1).defaultValues().toString();
    }

    @Test(expected=NullPointerException.class)
    public void testDefaultValuesNPE2() {
        Sql.insert().defaultValues().value(1).toString();
    }

    @Test
    public void testSelect() {
        SqlQueryTest.assertEquals(Sql.insert().select(Sql.select().where("x", 1)), "x", 1);
    }

    @Test(expected=NullPointerException.class)
    public void testSelectNPE1() {
        Sql.insert().value(1).select(Sql.select()).toString();
    }

    @Test(expected=NullPointerException.class)
    public void testSelectNPE2() {
        Sql.insert().select(Sql.select("x")).value(1).toString();
    }

    @Test
    public void testValueExpr() {
        assertEquals("VALUES (x)", Sql.insert().valueExpr("x").toString());
        assertEquals("VALUES (x, y)", Sql.insert().valueExpr("x").valueExpr("y").toString());
        SqlQueryTest.assertEquals(Sql.insert().valueExpr("x", 1).valueExpr("y", 2), "VALUES (x, y)", 1, 2);
        SqlQueryTest.assertEquals(Sql.insert().valueExpr("x", 1, 2).valueExpr("y", 3, 4), "VALUES (x, y)", 1, 2, 3, 4);
    }

    @Test
    public void testValues() {
        SqlQueryTest.assertEquals(Sql.insert().value("x"), "VALUES (?)", "x");
        SqlQueryTest.assertEquals(Sql.insert().value("x").value("y"), "VALUES (?, ?)", "x", "y");
        SqlQueryTest.assertEquals(Sql.insert().values("x", "y"), "VALUES (?, ?)", "x", "y");
    }

    @Test
    public void testSet() {
        SqlQueryTest.assertEquals(Sql.insert().set("x", 1), "(x) VALUES (?)", 1);
        SqlQueryTest.assertEquals(Sql.insert().set("x", 1).set("y", 2), "(x, y) VALUES (?, ?)", 1, 2);
    }

    @Test
    public void testSetExpr() {
        SqlQueryTest.assertEquals(Sql.insert().setExpr("c1", "x", 1), "(c1) VALUES (x)", 1);
        SqlQueryTest.assertEquals(Sql.insert().setExpr("c1", "x", 1).setExpr("c2", "y", 2), "(c1, c2) VALUES (x, y)", 1, 2);
    }

    @Test
    public void setFrom() {
        SqlQueryTest.assertEquals(Sql.insert().setFrom(new xyz(1,2,3)), "(x, y, z) VALUES (?, ?, ?)", 1, 2, 3);
        SqlQueryTest.assertEquals(Sql.insert().setFrom(new xyz(1,2,3), "y"), "(y) VALUES (?)", 2);
        SqlQueryTest.assertEquals(Sql.insert().setFrom(new xyz(1,2,3), "x", "z"), "(x, z) VALUES (?, ?)", 1, 3);
        SqlQueryTest.assertEquals(Sql.insert().setFrom(new xyz(1,2,3), "x", "y", "z"), "(x, y, z) VALUES (?, ?, ?)", 1, 2, 3);
    }

    @Test
    public void setFromMap() {
        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
        map.put("x", 1);
        map.put("y", 2);
        map.put("z", 3);
        SqlQueryTest.assertEquals(Sql.insert().setFromMap(map), "(x, y, z) VALUES (?, ?, ?)", 1, 2, 3);
        SqlQueryTest.assertEquals(Sql.insert().setFromMap(map, "y"), "(y) VALUES (?)", 2);
        SqlQueryTest.assertEquals(Sql.insert().setFromMap(map, "x", "z"), "(x, z) VALUES (?, ?)", 1, 3);
        SqlQueryTest.assertEquals(Sql.insert().setFromMap(map, "x", "y", "z"), "(x, y, z) VALUES (?, ?, ?)", 1, 2, 3);
    }

    @Test
    public void testToString() {
        assertEquals("INSERT INTO t (c) VALUES (x)", Sql.insert("t").column("c").valueExpr("x").toString());
        assertEquals("INSERT INTO t (c) DEFAULT VALUES", Sql.insert("t").column("c").defaultValues().toString());
        assertEquals("INSERT INTO t (c) SELECT x", Sql.insert("t").column("c").select(Sql.select("x")).toString());
        SqlQueryTest.assertEquals(Sql.insert("t").set("c", "x"), "INSERT INTO t (c) VALUES (?)", "x");
    }

    public static class xyz {
        public int x, y, z;
        public xyz(int x, int y, int z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }
    }

}
