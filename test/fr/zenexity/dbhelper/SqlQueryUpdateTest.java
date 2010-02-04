package fr.zenexity.dbhelper;

import java.util.Arrays;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class SqlQueryUpdateTest {

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
        assertEquals("SET x=?", Sql.update().set("x", 1).toString());
        assertEquals("SET x=?, y=?", Sql.update().set("x", 1).set("y", 2).toString());
    }

    @Test
    public void setParams() {
        assertEquals(Arrays.asList(1), Sql.update().set("x", 1).params());
        assertEquals(Arrays.asList(1, 2), Sql.update().set("x", 1).set("y", 2).params());
    }

    @Test
    public void setExpr() {
        assertEquals("SET x=a", Sql.update().setExpr("x", "a", 1).toString());
        assertEquals("SET x=a, y=b", Sql.update().setExpr("x", "a", 1).setExpr("y", "b", 2).toString());
    }
    @Test
    public void setExprParams() {
        assertEquals(Arrays.asList(1), Sql.update().setExpr("x", "a", 1).params());
        assertEquals(Arrays.asList(1, 2), Sql.update().setExpr("x", "a", 1).setExpr("y", "b", 2).params());
    }

    @Test
    public void setFrom() {
        assertEquals("SET x=?, y=?, z=?", Sql.update().setFrom(new xyz(1,2,3)).toString());
        assertEquals("SET y=?", Sql.update().setFrom(new xyz(1,2,3), "y").toString());
        assertEquals("SET x=?, z=?", Sql.update().setFrom(new xyz(1,2,3), "x", "z").toString());
        assertEquals("SET x=?, y=?, z=?", Sql.update().setFrom(new xyz(1,2,3), "x", "y", "z").toString());
    }

    @Test
    public void setFromParams() {
        assertEquals(Arrays.asList(1, 2, 3), Sql.update().setFrom(new xyz(1,2,3)).params());
        assertEquals(Arrays.asList(2), Sql.update().setFrom(new xyz(1,2,3), "y").params());
        assertEquals(Arrays.asList(1, 3), Sql.update().setFrom(new xyz(1,2,3), "x", "z").params());
        assertEquals(Arrays.asList(1, 2, 3), Sql.update().setFrom(new xyz(1,2,3), "x", "y", "z").params());
    }

    @Test
    public void where() {
        assertEquals("WHERE x", Sql.update().where("x").toString());
        assertEquals("WHERE x AND y", Sql.update().where("x").andWhere("y").toString());
        assertEquals("WHERE x OR y", Sql.update().where("x").orWhere("y").toString());
    }

    @Test
    public void subWhere() {
        assertEquals("WHERE (x)", Sql.update().where(Sql.where("x")).toString());
        assertEquals("WHERE (x) AND (y)", Sql.update().where(Sql.where("x")).andWhere(Sql.where("y")).toString());
        assertEquals("WHERE (x) OR (y)", Sql.update().where(Sql.where("x")).orWhere(Sql.where("y")).toString());
    }

    @Test
    public void whereParams() {
        assertEquals(Arrays.asList(1), Sql.update().where("x", 1).params());
        assertEquals(Arrays.asList(1, 2), Sql.update().where("x", 1).andWhere("y", 2).params());
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
        assertEquals("UPDATE x SET y=? WHERE z ORDER BY 1 LIMIT 2", Sql.update("x").set("y", 3).where("z").orderBy(1).limit(2).toString());
    }

    @Test
    public void fullParams() {
        assertEquals(Arrays.asList(1, 2), Sql.update("x").set("y", 1).where("z", 2).orderBy("a").limit(3).params());
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
