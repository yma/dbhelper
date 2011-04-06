package fr.zenexity.dbhelper;

import static fr.zenexity.dbhelper.SqlTest.assertQuery;

import java.util.Arrays;

import org.junit.Test;

/**
 *
 * @author yma
 */
public class SqlFinalQueryTest {

    @Test
    public void test() {
        assertQuery(new Sql.FinalQuery("SELECT x WHERE y"), "SELECT x WHERE y");
        assertQuery(new Sql.FinalQuery(Sql.select("x").where("y", 1)), "SELECT x WHERE y", 1);
    }

    @Test
    public void testCopy() {
        Sql.FinalQuery query = new Sql.FinalQuery(Sql.select("x").where("y", 1));
        assertQuery(Sql.finalQuery(query), "SELECT x WHERE y", 1);
        assertQuery(Sql.finalQuery(query, 2), "SELECT x WHERE y", 1, 2);
        assertQuery(Sql.finalQuery(query, 3), "SELECT x WHERE y", 1, 3);
    }

    @Test
    public void testWithObject() {
        assertQuery(new Sql.FinalQuery(Sql.select("x").where("y", 1), 2), "SELECT x WHERE y", 1, 2);
    }

    @Test
    public void testWithArray() {
        assertQuery(new Sql.FinalQuery(Sql.select("x").where("y", 1), 2, 3), "SELECT x WHERE y", 1, 2, 3);
    }

    @Test
    public void testWithList() {
        assertQuery(new Sql.FinalQuery(Sql.select("x").where("y", 1), Arrays.<Integer>asList()), "SELECT x WHERE y", 1);
        assertQuery(new Sql.FinalQuery(Sql.select("x").where("y", 1), Arrays.asList(2)), "SELECT x WHERE y", 1, 2);
        assertQuery(new Sql.FinalQuery(Sql.select("x").where("y", 1), Arrays.asList(2, 3)), "SELECT x WHERE y", 1, 2, 3);
    }

}
