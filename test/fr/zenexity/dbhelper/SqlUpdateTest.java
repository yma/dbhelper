package fr.zenexity.dbhelper;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class SqlUpdateTest {

    @Test
    public void update() {
        assertEquals("", Sql.update().toString());
        assertEquals("UPDATE x", Sql.update("x").toString());
        assertEquals("UPDATE x, y", Sql.update("x", "y").toString());
        assertEquals("UPDATE Sql", Sql.update(Sql.class).toString());
        assertEquals("UPDATE Sql, Update", Sql.update(Sql.class, Sql.Update.class).toString());
    }

    @Test
    public void set() {
        SqlTest.assertQuery(Sql.update().set("x", 1), "SET x=?", 1);
        SqlTest.assertQuery(Sql.update().set("x", 1).set("y", 2), "SET x=?, y=?", 1, 2);
    }

    @Test
    public void setExpr() {
        SqlTest.assertQuery(Sql.update().setExpr("x", "a", 1), "SET x=a", 1);
        SqlTest.assertQuery(Sql.update().setExpr("x", "a", 1).setExpr("y", "b", 2), "SET x=a, y=b", 1, 2);
    }

    @Test
    public void object() {
        SqlTest.assertQuery(Sql.update().object(new xyz(1,2,3)), "SET x=?, y=?, z=?", 1, 2, 3);
        SqlTest.assertQuery(Sql.update().object(new xyz(1,2,3), "y"), "SET y=?", 2);
        SqlTest.assertQuery(Sql.update().object(new xyz(1,2,3), "x", "z"), "SET x=?, z=?", 1, 3);
        SqlTest.assertQuery(Sql.update().object(new xyz(1,2,3), "x", "y", "z"), "SET x=?, y=?, z=?", 1, 2, 3);
    }

    @Test
    public void map() {
        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
        map.put("x", 1);
        map.put("y", 2);
        map.put("z", 3);
        SqlTest.assertQuery(Sql.update().map(map), "SET x=?, y=?, z=?", 1, 2, 3);
        SqlTest.assertQuery(Sql.update().map(map, "y"), "SET y=?", 2);
        SqlTest.assertQuery(Sql.update().map(map, "x", "z"), "SET x=?, z=?", 1, 3);
        SqlTest.assertQuery(Sql.update().map(map, "x", "y", "z"), "SET x=?, y=?, z=?", 1, 2, 3);
    }

    @Test
    public void where() {
        SqlTest.assertQuery(Sql.update().where("x", 1), "WHERE x", 1);
        SqlTest.assertQuery(Sql.update().where("x", 1).andWhere("y", 2), "WHERE x AND y", 1, 2);
        assertEquals("WHERE x OR y", Sql.update().where("x").orWhere("y").toString());
    }

    @Test
    public void subWhere() {
        assertEquals("WHERE (x)", Sql.update().where(Sql.where("x")).toString());
        assertEquals("WHERE (x) AND (y)", Sql.update().where(Sql.where("x")).andWhere(Sql.where("y")).toString());
        assertEquals("WHERE (x) OR (y)", Sql.update().where(Sql.where("x")).orWhere(Sql.where("y")).toString());
    }

    @Test
    public void orderBy() {
        assertEquals("ORDER BY x", Sql.update().orderBy("x").toString());
        assertEquals("ORDER BY x, y", Sql.update().orderBy("x", "y").toString());
        assertEquals("ORDER BY x, y, 1", Sql.update().orderBy("x", "y", 1).toString());
    }

    @Test
    public void limit() {
        assertEquals("LIMIT 1", Sql.update().limit(1).toString());
    }

    @Test
    public void full() {
        SqlTest.assertQuery(Sql.update("x").set("y", 3).where("z", 4).orderBy(1).limit(2),
                "UPDATE x SET y=? WHERE z ORDER BY 1 LIMIT 2", 3, 4);
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
