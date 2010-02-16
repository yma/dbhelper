package fr.zenexity.dbhelper;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 *
 * @author yma
 */
public class SqlQueryTest {

    public static void assertQuery(Sql.Query query, String sqlString, Object... sqlParams) {
        assertEquals(sqlString, query.toString());
        assertEquals(Arrays.asList(sqlParams), query.params());
    }

    public static void assertQuery(Sql.UpdateQuery query, String sqlString, Object... sqlParams) {
        assertEquals(sqlString, query.toString());
        assertEquals(Arrays.asList(sqlParams), query.params());
    }

    @Test
    public void concatEmpty() {
        assertEquals("", new Sql.Concat("Hello ", "World", ".").toString());
        assertTrue(new Sql.Concat("Hello ", "World", ".").isEmpty());
        assertFalse(new Sql.Concat("Hello ", "World", ".").append("cool").isEmpty());
    }

    @Test
    public void concatNullPrefix() {
        assertEquals("", new Sql.Concat(null, "World", ".").toString());
    }

    @Test(expected=NullPointerException.class)
    public void concatNullPrefixError() {
        new Sql.Concat(null, "World", ".").add("cool").toString();
    }

    @Test
    public void concatNullSeparator() {
        assertEquals("", new Sql.Concat("Hello", null, ".").toString());
        assertEquals("Hello cool.", new Sql.Concat("Hello ", null, ".").add("cool").toString());
    }

    @Test(expected=NullPointerException.class)
    public void concatNullSeparatorError() {
        new Sql.Concat("Hello ", null, ".").add("cool", "giga");
    }

    @Test
    public void concatNullSuffix() {
        assertEquals("", new Sql.Concat("Hello ", "World", null).toString());
    }

    @Test(expected=NullPointerException.class)
    public void concatNullSuffixError() {
        new Sql.Concat("Hello ", "World", null).add("cool").toString();
    }

    @Test
    public void concatOneElement() {
        assertEquals("Hello cool.", new Sql.Concat("Hello ", " World ", ".").append("cool").toString());
    }

    @Test
    public void concatTwoElements() {
        assertEquals("Hello mega World cool.", new Sql.Concat("Hello ", " World ", ".").add("mega", "cool").toString());
    }

    @Test
    public void concatThreeElements() {
        assertEquals("Hello mega World giga World cool.", new Sql.Concat("Hello ", " World ", ".").add("mega", "giga", "cool").toString());
        assertEquals("Hello 1 World 2 World 3.", new Sql.Concat("Hello ", " World ", ".").add(1, "2", 3).toString());
    }

    @Test
    public void concatAppend() {
        assertEquals("Hello mega World giga World 123.", new Sql.Concat("Hello ", " World ", ".").add("mega", "giga").append(123).toString());
    }

    @Test
    public void concatAddSep() {
        assertEquals("Hello 1, 2, 3.", new Sql.Concat("Hello ", ", ", ".").add("1","2","3").toString());
        assertEquals("Hello mega World giga! 1! 2! 3.", new Sql.Concat("Hello ", " World ", ".").add("mega", "giga").separator("! ").add("1","2","3").toString());
    }

    @Test
    public void concatPrefix() {
        assertEquals("yop mega World cool.", new Sql.Concat("Hello ", " World ", ".").prefix("yop ").add("mega", "cool").toString());
        assertEquals("yop mega World cool.", new Sql.Concat(null, " World ", ".").prefix("yop ").add("mega", "cool").toString());
        assertEquals("yop mega World cool.", new Sql.Concat(null, " World ", ".").add("mega").prefix("yop ").add("cool").toString());
        assertEquals("yop mega World cool.", new Sql.Concat(null, " World ", ".").add("mega").add("cool").prefix("yop ").toString());
    }

    @Test
    public void concatLocalPrefix() {
        assertEquals("Hello yop mega World cool.", new Sql.Concat("Hello ", " World ", ".").localPrefix("yop ").add("mega", "cool").toString());
        assertEquals("Hello mega World cool.", new Sql.Concat("Hello ", " World ", ".").add("mega").localPrefix("yop ").add("cool").toString());
        assertEquals("Hello mega World cool.", new Sql.Concat("Hello ", " World ", ".").add("mega").add("cool").localPrefix("yop ").toString());
    }

    @Test
    public void concatSeparator() {
        assertEquals("Hello mega World giga! 123.", new Sql.Concat("Hello ", " World ", ".").add("mega", "giga").separator("! ").append(123).toString());
        assertEquals("Hello 123.", new Sql.Concat("Hello ", " World ", ".").separator("! ").append(123).toString());
        assertEquals("Hello 1! 2! 3.", new Sql.Concat("Hello ", " World ", ".").separator("! ").add("1","2","3").toString());
        assertEquals("Hello mega World giga! 1! 2! 3.", new Sql.Concat("Hello ", " World ", ".").add("mega", "giga").separator("! ").add("1","2","3").toString());
    }

    @Test
    public void concatDefaultValue() {
        assertEquals("", new Sql.Concat("Hello ", " World ", ".").add("", "").toString());
        assertEquals("Hello mega World  World .", new Sql.Concat("Hello ", " World ", ".").add("mega", "", "").toString());
        assertEquals("Hello mega World yop World yop.", new Sql.Concat("Hello ", " World ", ".").defaultValue("yop").add("mega", "", "").toString());
        assertEquals("Hello mega.", new Sql.Concat("Hello ", " World ", ".").defaultValue(null).add("mega", "", "").toString());
        assertEquals("Hello mega World cool.", new Sql.Concat("Hello ", " World ", ".").defaultValue(null).add("mega", "cool", "").toString());
        assertEquals("Hello mega World .", new Sql.Concat("Hello ", " World ", ".").add("mega", "").defaultValue(null).add("").toString());
    }

    @Test
    public void RepeatToString() {
        Sql.Concat c = new Sql.Concat("Hello ", " World ", ".").add("cool", "cool");
        assertEquals("Hello cool World cool.", c.toString());
        assertEquals("Hello cool World cool.", c.toString());
    }

    @Test
    public void quote() {
        assertEquals("'Hello World'", Sql.quote("Hello World"));
        assertEquals("'Hello \\'World\\''", Sql.quote("Hello 'World'"));
    }

    @Test
    public void inlineParam() {
        assertEquals("NULL", Sql.inlineParam(null));
        assertEquals("123", Sql.inlineParam(123));
        assertEquals("'Hello World'", Sql.inlineParam("Hello World"));
        assertEquals("(1, 2, 3)", Sql.inlineParam(new Integer[] {1, 2, 3}));
        assertEquals("('Hello', 'cool', 'World')", Sql.inlineParam(new String[] {"Hello", "cool", "World"}));
        assertEquals("(1)", Sql.inlineParam(new Integer[] {1}));
        assertEquals("", Sql.inlineParam(new Integer[] {}));

        List<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < 3; i++) list.add(i);
        assertEquals("(0, 1, 2)", Sql.inlineParam(list));
    }

}
