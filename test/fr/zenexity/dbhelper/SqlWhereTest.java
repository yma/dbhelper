package fr.zenexity.dbhelper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class SqlWhereTest {

    public static void assertWhere(Sql.Where where, String sqlString, Object... sqlParams) {
        assertEquals(sqlString, where.toString());
        assertEquals(Arrays.asList(sqlParams), where.params());
    }

    @Test
    public void testNew() {
        assertWhere(new Sql.Where(), "");
    }

    @Test
    public void testNewCopy() {
        assertWhere(new Sql.Where(new Sql.Where().and("x", 1)), "(x)", 1);
    }

    @Test
    public void testAnd() {
        assertWhere(new Sql.Where().and("x"), "(x)");
        assertWhere(new Sql.Where().and("x", 1), "(x)", 1);
        assertWhere(new Sql.Where().and("x", 1).and("y", 2), "(x AND y)", 1, 2);
    }

    @Test
    public void testAndSubWhere() {
        Sql.Where x = new Sql.Where().and("x", 1);
        Sql.Where y = new Sql.Where().and("y", 2);
        assertWhere(new Sql.Where().and(x), "((x))", 1);
        assertWhere(new Sql.Where().and(x).and(y), "((x) AND (y))", 1, 2);
    }

    @Test
    public void testOr() {
        assertWhere(new Sql.Where().or("x"), "(x)");
        assertWhere(new Sql.Where().or("x", 1), "(x)", 1);
        assertWhere(new Sql.Where().or("x", 1).or("y", 2), "(x OR y)", 1, 2);
    }

    @Test
    public void testOrSubWhere() {
        Sql.Where x = new Sql.Where().and("x", 1);
        Sql.Where y = new Sql.Where().and("y", 2);
        assertWhere(new Sql.Where().or(x), "((x))", 1);
        assertWhere(new Sql.Where().or(x).or(y), "((x) OR (y))", 1, 2);
    }

    @Test
    public void testKeyObject() {
        assertWhere(Sql.Where.key(new xyz(1, 2, 3)), "");
        assertWhere(Sql.Where.key(new xyz(1, 2, 3), "y"), "(y=?)", 2);
        assertWhere(Sql.Where.key(new xyz(1, 2, 3), "x", "z"), "(x=? AND z=?)", 1, 3);
        assertWhere(Sql.Where.key(new xyz(1, 2, 3), "x", "y", "z"), "(x=? AND y=? AND z=?)", 1, 2, 3);
    }

    @Test
    public void testKeyMap() {
        Map<String, Integer> xyz = new HashMap<String, Integer>();
        xyz.put("x", 1);
        xyz.put("y", 2);
        xyz.put("z", 3);
        assertWhere(Sql.Where.key(xyz), "");
        assertWhere(Sql.Where.key(xyz, "y"), "(y=?)", 2);
        assertWhere(Sql.Where.key(xyz, "x", "z"), "(x=? AND z=?)", 1, 3);
        assertWhere(Sql.Where.key(xyz, "x", "y", "z"), "(x=? AND y=? AND z=?)", 1, 2, 3);
    }

    @Test
    public void testToString() {
        assertEquals("(x OR y AND z)", new Sql.Where().and("x", 1).or("y", 2).and("z", 3).toString());
    }

    @Test
    public void value() {
        assertEquals("x OR y AND z", new Sql.Where().and("x", 1).or("y", 2).and("z", 3).value());
    }

    @Test
    public void params() {
        assertEquals(
                Arrays.<Object>asList(1, 2, 3, 4, 5, 6),
                new Sql.Where().and("x", 1, 2).or("y", 3, 4).and("z", 5, 6).params());
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
