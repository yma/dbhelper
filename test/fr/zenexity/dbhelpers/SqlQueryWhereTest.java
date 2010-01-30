package fr.zenexity.dbhelpers;

import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class SqlQueryWhereTest {

    @Test
    public void where() {
        assertEquals("(x)", Sql.where("x").toString());
        assertEquals("(x AND y)", Sql.where("x").and("y").toString());
        assertEquals("(x OR y)", Sql.where("x").or("y").toString());
    }

    @Test
    public void value() {
        assertEquals("x", Sql.where("x").value());
        assertEquals("x AND y", Sql.where("x").and("y").value());
        assertEquals("x OR y", Sql.where("x").or("y").value());
    }

    @Test
    public void and() {
        assertEquals("(x)", new Sql.Where().and("x").toString());
        assertEquals("(x AND y)", new Sql.Where().and("x").and("y").toString());
    }

    @Test
    public void or() {
        assertEquals("(x)", new Sql.Where().and("x").toString());
        assertEquals("(x OR y)", new Sql.Where().or("x").or("y").toString());
    }

    @Test
    public void subWhere() {
        Sql.Where x = Sql.where("x").or("y").and("z");
        Sql.Where y = Sql.where("z").and("y").or("x");
        assertEquals("((x OR y AND z))", Sql.where(x).toString());
        assertEquals("((x OR y AND z) AND (z AND y OR x))", Sql.where(x).and(y).toString());
        assertEquals("((x OR y AND z) OR (z AND y OR x))", Sql.where(x).or(y).toString());
    }

    @Test
    public void andSubWhere() {
        Sql.Where x = new Sql.Where().and("x").or("y").and("z");
        Sql.Where y = new Sql.Where().or("z").and("y").or("x");
        assertEquals("((x OR y AND z))", new Sql.Where().and(x).toString());
        assertEquals("((x OR y AND z) AND (z AND y OR x))", new Sql.Where().and(x).and(y).toString());
        assertEquals("((x OR y AND z) OR (z AND y OR x))", new Sql.Where().and(x).or(y).toString());
    }

    @Test
    public void orSubWhere() {
        Sql.Where x = new Sql.Where().and("x").or("y").and("z");
        Sql.Where y = new Sql.Where().or("z").and("y").or("x");
        assertEquals("((x OR y AND z))", new Sql.Where().or(x).toString());
        assertEquals("((x OR y AND z) AND (z AND y OR x))", new Sql.Where().or(x).and(y).toString());
        assertEquals("((x OR y AND z) OR (z AND y OR x))", new Sql.Where().or(x).or(y).toString());
    }

    @Test
    public void params() {
        assertEquals(
                Arrays.<Object>asList(1, 2, 3, 4, 5, 6),
                Sql.where("x", 1, 2).and("y", 3, 4).or("z", 5, 6).params());
    }

}
