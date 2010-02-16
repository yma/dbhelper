package fr.zenexity.dbhelper;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class SqlQueryUnionTest {

    @Test
    public void unionOneSelect() {
        assertEquals("(SELECT x)", Sql.union(Sql.select("x")).toString());
        assertEquals("(SELECT x)", new Sql.Union().union(Sql.select("x")).toString());
    }

    @Test
    public void unionTwoElements() {
        assertEquals("(SELECT x) UNION (SELECT y)", Sql.union(Sql.select("x")).union(Sql.select("y")).toString());
        assertEquals("(SELECT x) UNION (SELECT y)", new Sql.Union().union(Sql.select("x")).union(Sql.select("y")).toString());
    }

    @Test
    public void unionAllOneElement() {
        assertEquals("(SELECT x)", Sql.unionAll(Sql.select("x")).toString());
        assertEquals("(SELECT x)", new Sql.Union().unionAll(Sql.select("x")).toString());
    }

    @Test
    public void unionAllTwoElements() {
        assertEquals("(SELECT x) UNION ALL (SELECT y)", Sql.unionAll(Sql.select("x")).unionAll(Sql.select("y")).toString());
        assertEquals("(SELECT x) UNION ALL (SELECT y)", new Sql.Union().unionAll(Sql.select("x")).unionAll(Sql.select("y")).toString());
    }

    @Test
    public void orderBy() {
        assertEquals("ORDER BY x", new Sql.Union().orderBy("x").toString());
        assertEquals("ORDER BY x, y", new Sql.Union().orderBy("x").orderBy("y").toString());
        assertEquals("ORDER BY x, y", new Sql.Union().orderBy("x", "y").toString());
        assertEquals("ORDER BY 1, x, 2, y, 3", new Sql.Union().orderBy(1, "x", 2, "y", 3).toString());
    }

    @Test
    public void offset() {
        assertEquals("OFFSET 1", new Sql.Union().offset(1).toString());
    }

    @Test(expected=NullPointerException.class)
    public void offsetError() {
        new Sql.Union().offset(1).offset(2).toString();
    }

    @Test
    public void limit() {
        assertEquals("LIMIT 1", new Sql.Union().limit(1).toString());
    }

    @Test(expected=NullPointerException.class)
    public void limitError() {
        new Sql.Union().limit(1).limit(2).toString();
    }

    @Test
    public void normalUsage() {
        assertEquals("(SELECT x) UNION (SELECT y) ORDER BY z OFFSET 1 LIMIT 2",
                Sql.union(Sql.select("x")).union(Sql.select("y")).orderBy("z").offset(1).limit(2).toString());
    }

    @Test
    public void params() {
        Sql.Select selectX = Sql.select().where("x", 1, 2, 3);
        Sql.Select selectY = Sql.select().where("y", 4, 5, 6);
        SqlQueryTest.assertQuery(Sql.union(selectX).orderBy("8").union(selectY).orderBy("9"),
                "(x) UNION (y) ORDER BY 8, 9",
                1, 2, 3, 4, 5, 6);
    }

}
