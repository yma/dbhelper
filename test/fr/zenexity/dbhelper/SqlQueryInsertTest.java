package fr.zenexity.dbhelper;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author yma
 */
public class SqlQueryInsertTest {

    @Test
    public void basic() {
        assertEquals("INSERT INTO x (y)", Sql.insert().into("x").column("y").toString());
        assertEquals("INSERT INTO x (y, z)", Sql.insert().into("x").column("y").column("z").toString());
        assertEquals("INSERT INTO x (y, z)", Sql.insert().into("x").columns("y", "z").toString());
    }

    @Test
    public void basicReplace() {
        assertEquals("REPLACE INTO x (y)", Sql.replace().into("x").column("y").toString());
        assertEquals("REPLACE INTO x (y, z)", Sql.replace().into("x").column("y").column("z").toString());
        assertEquals("REPLACE INTO x (y, z)", Sql.replace().into("x").columns("y", "z").toString());
    }

    @Test
    public void defaultValues() {
        assertEquals("INSERT INTO x (y) DEFAULT VALUES", Sql.insert().into("x").column("y").defaultValues().toString());
    }

    @Test
    public void values() {
        SqlQueryTest.assertEquals(Sql.insert().into("x").value("y", 1, 2, 3), "INSERT INTO x VALUES (y)", 1, 2, 3);
        SqlQueryTest.assertEquals(Sql.insert().into("x").value("y", 1, 2).value("z", 3, 4), "INSERT INTO x VALUES (y, z)", 1, 2, 3, 4);
    }

    @Test
    public void select() {
        SqlQueryTest.assertEquals(Sql.insert().into("x").select(Sql.select("y").where("z", 1, 2, 3)), "INSERT INTO x SELECT y WHERE z", 1, 2, 3);
    }

    @Test(expected=NullPointerException.class)
    public void valueError() {
        Sql.insert().into("x").value("ok").select(Sql.select("y").where("z"));
    }

    @Test(expected=NullPointerException.class)
    public void selectError() {
        Sql.insert().into("x").select(Sql.select("y").where("z")).value("error");
    }

}
