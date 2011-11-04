package fr.zenexity.dbhelper;

import static org.junit.Assert.*;

import org.junit.Test;

public class SqlParserTest {

    @Test
    public void testToQuery() {
        assertEquals(new SqlParser("select x").toQuery(Sql.Select.class).getClass(), Sql.Select.class);
    }

    @Test
    public void testToSelect() {
        assertEquals(
                "select x, y, z from a, b join left j1 JOIN j2 " +
                "where c1 and c2 OR c3 GROUP by g1, g2 having h1 AND h2 " +
                "order by o1, o2 offset 1 limit 2",
                new SqlParser(
                        "select x, y from a join left j1 " +
                        "where c1 and c2 GROUP by g1 having h1 " +
                        "order by o1 offset 1 limit 2").toSelect()
                    .select("z").from("b").join("j2").orWhere("c3")
                    .groupBy("g2").having("h2") .orderBy("o2").toString());
        assertEquals("select x, y, z from a, b where c1 and c2 OR c3",
                new SqlParser("select x, y from a where c1 and c2").toSelect()
                    .select("z").from("b").orWhere("c3").toString());
        assertEquals("select x, y, z where c1 and c2 OR c3",
                new SqlParser("select x, y where c1 and c2").toSelect()
                    .select("z").orWhere("c3").toString());
    }

}
