package fr.zenexity.dbhelper;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class SqlDeleteTest {

    @Test
    public void testDelete() {
        assertEquals("DELETE FROM table", new Sql.Delete("table").toString());
        assertEquals("DELETE FROM table", new Sql.Delete("table", false).toString());
        assertEquals("DELETE FROM ONLY table", new Sql.Delete("table", true).toString());
        assertEquals("DELETE FROM Delete", new Sql.Delete(Sql.Delete.class).toString());
        assertEquals("DELETE FROM Delete", new Sql.Delete(Sql.Delete.class, false).toString());
        assertEquals("DELETE FROM ONLY Delete", new Sql.Delete(Sql.Delete.class, true).toString());
    }

    @Test
    public void testUsing() {
        assertEquals("DELETE FROM x USING y", new Sql.Delete("x").using("y").toString());
        assertEquals("DELETE FROM x USING y, z", new Sql.Delete("x").using("y", "z").toString());
        assertEquals("DELETE FROM x USING y, z", new Sql.Delete("x").using("y").using("z").toString());
        assertEquals("DELETE FROM x USING Sql", new Sql.Delete("x").using(Sql.class).toString());
        assertEquals("DELETE FROM x USING Sql, Delete", new Sql.Delete("x").using(Sql.class, Sql.Delete.class).toString());
        assertEquals("DELETE FROM x USING Sql, Delete", new Sql.Delete("x").using(Sql.class).using(Sql.Delete.class).toString());
    }

    @Test
    public void testWhere() {
        SqlTest.assertQuery(new Sql.Delete("x").where("y", 1), "DELETE FROM x WHERE y", 1);
        SqlTest.assertQuery(new Sql.Delete("x").where("y", 1).andWhere("z", 2), "DELETE FROM x WHERE y AND z", 1, 2);
        SqlTest.assertQuery(new Sql.Delete("x").where("y", 1).orWhere("z", 2), "DELETE FROM x WHERE y OR z", 1, 2);
        SqlTest.assertQuery(new Sql.Delete("x").where(Sql.where("y", 1)), "DELETE FROM x WHERE (y)", 1);
        SqlTest.assertQuery(new Sql.Delete("x").where(Sql.where("y", 1)).andWhere(Sql.where("z", 2)), "DELETE FROM x WHERE (y) AND (z)", 1, 2);
        SqlTest.assertQuery(new Sql.Delete("x").where(Sql.where("y", 1)).orWhere(Sql.where("z", 2)), "DELETE FROM x WHERE (y) OR (z)", 1, 2);
    }

    @Test
    public void testToStringParams() {
        SqlTest.assertQuery(new Sql.Delete("x", true).using("y").where("z", 1),
                "DELETE FROM ONLY x USING y WHERE z", 1);
    }

    @Test
    public void testCopyParams() {
        Sql.UpdateQuery delete = new Sql.Delete("x", true).using("y").where("z", 1, 2);
        List<Object> params = delete.copyParams();
        assertEquals(delete.params(), params);
        params.add(3);
        assertEquals(Arrays.asList(1, 2, 3), params);
        assertEquals(Arrays.asList(1, 2), delete.params());
    }

}
