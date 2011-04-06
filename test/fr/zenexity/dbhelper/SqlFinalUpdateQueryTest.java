package fr.zenexity.dbhelper;

import static fr.zenexity.dbhelper.SqlTest.assertQuery;

import java.util.Arrays;

import org.junit.Test;

/**
 *
 * @author yma
 */
public class SqlFinalUpdateQueryTest {

    @Test
    public void test() {
        assertQuery(new Sql.FinalUpdateQuery("INSERT INTO x (y) VALUES (1)"), "INSERT INTO x (y) VALUES (1)");
        assertQuery(new Sql.FinalUpdateQuery(Sql.insert("x").set("y", 1)), "INSERT INTO x (y) VALUES (?)", 1);
    }

    @Test
    public void testCopy() {
        Sql.FinalUpdateQuery query = new Sql.FinalUpdateQuery(Sql.insert("x").set("y", 1));
        assertQuery(Sql.finalQuery(query), "INSERT INTO x (y) VALUES (?)", 1);
        assertQuery(Sql.finalQuery(query, 2), "INSERT INTO x (y) VALUES (?)", 1, 2);
        assertQuery(Sql.finalQuery(query, 3), "INSERT INTO x (y) VALUES (?)", 1, 3);
    }

    @Test
    public void testWithObject() {
        assertQuery(new Sql.FinalUpdateQuery(Sql.insert("x").set("y", 1), 2), "INSERT INTO x (y) VALUES (?)", 1, 2);
    }

    @Test
    public void testWithArray() {
        assertQuery(new Sql.FinalUpdateQuery(Sql.insert("x").set("y", 1), 2, 3), "INSERT INTO x (y) VALUES (?)", 1, 2, 3);
    }

    @Test
    public void testWithList() {
        assertQuery(new Sql.FinalUpdateQuery(Sql.insert("x").set("y", 1), Arrays.<Integer>asList()), "INSERT INTO x (y) VALUES (?)", 1);
        assertQuery(new Sql.FinalUpdateQuery(Sql.insert("x").set("y", 1), Arrays.asList(2)), "INSERT INTO x (y) VALUES (?)", 1, 2);
        assertQuery(new Sql.FinalUpdateQuery(Sql.insert("x").set("y", 1), Arrays.asList(2, 3)), "INSERT INTO x (y) VALUES (?)", 1, 2, 3);
    }

}
