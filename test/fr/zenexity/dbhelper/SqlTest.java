package fr.zenexity.dbhelper;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

/**
 *
 * @author yma
 */
public class SqlTest {

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
    public void joinTypeValue() {
        assertEquals("JOIN", Sql.JoinType.JOIN.value);
        assertEquals("INNER JOIN", Sql.JoinType.INNER.value);
        assertEquals("LEFT JOIN", Sql.JoinType.LEFT.value);
        assertEquals("RIGHT JOIN", Sql.JoinType.RIGHT.value);
        assertEquals("FULL JOIN", Sql.JoinType.FULL.value);
        assertEquals("LEFT OUTER JOIN", Sql.JoinType.LEFT_OUTER.value);
        assertEquals("RIGHT OUTER JOIN", Sql.JoinType.RIGHT_OUTER.value);
        assertEquals("FULL OUTER JOIN", Sql.JoinType.FULL_OUTER.value);
        assertEquals("CROSS JOIN", Sql.JoinType.CROSS.value);
        assertEquals("NATURAL JOIN", Sql.JoinType.NATURAL.value);
        assertEquals("NATURAL LEFT JOIN", Sql.JoinType.NATURAL_LEFT.value);
        assertEquals("NATURAL RIGHT JOIN", Sql.JoinType.NATURAL_RIGHT.value);
        assertEquals("NATURAL LEFT OUTER JOIN", Sql.JoinType.NATURAL_LEFT_OUTER.value);
        assertEquals("NATURAL RIGHT OUTER JOIN", Sql.JoinType.NATURAL_RIGHT_OUTER.value);
        assertEquals("STRAIGHT_JOIN", Sql.JoinType.STRAIGHT_JOIN.value);
        assertEquals(15, Sql.JoinType.values().length);
    }

    @Test
    public void RepeatToString() {
        Sql.Concat c = new Sql.Concat("Hello ", " World ", ".").add("cool", "cool");
        assertEquals("Hello cool World cool.", c.toString());
        assertEquals("Hello cool World cool.", c.toString());
    }

    @Test
    public void testClone() {
        Sql.Select select = Sql.select("s").from("f").where("w?", 1).having("h?", 2).groupBy("s");
        Sql.Union union = Sql.union(select, select);
        Sql.Insert insert = Sql.insert("i").set("x", 1).set("y", 2).set("z", 3);
        Sql.Update update = Sql.update("u").set("x", 1).set("y", 2).set("z", 3).where("w?", 4);
        Sql.Delete delete = Sql.delete("d").where("w?", 1);
        Sql.Where where = Sql.where("w?", 1);
        Sql.FinalQuery finalQuery = new Sql.FinalQuery("finalQuery?", 1);
        Sql.FinalUpdateQuery finalUpdateQuery = new Sql.FinalUpdateQuery("finalUpdateQuery?", 1);

        assertEquals(Sql.resolve(select), Sql.resolve(Sql.clone(select)));
        assertEquals(Sql.resolve(union), Sql.resolve(Sql.clone(union)));
        assertEquals(Sql.resolve(insert), Sql.resolve(Sql.clone(insert)));
        assertEquals(Sql.resolve(update), Sql.resolve(Sql.clone(update)));
        assertEquals(Sql.resolve(delete), Sql.resolve(Sql.clone(delete)));
        assertEquals(Sql.resolve(where), Sql.resolve(Sql.clone(where)));
        assertEquals(Sql.resolve(finalQuery), Sql.resolve(Sql.clone(finalQuery)));
        assertEquals(Sql.resolve(finalUpdateQuery), Sql.resolve(Sql.clone(finalUpdateQuery)));

        assertEquals(Sql.resolve(select), Sql.resolve(Sql.clone((Sql.Query)select)));
        assertEquals(Sql.resolve(union), Sql.resolve(Sql.clone((Sql.Query)union)));
        assertEquals(Sql.resolve(insert), Sql.resolve(Sql.clone((Sql.UpdateQuery)insert)));
        assertEquals(Sql.resolve(update), Sql.resolve(Sql.clone((Sql.UpdateQuery)update)));
        assertEquals(Sql.resolve(delete), Sql.resolve(Sql.clone((Sql.UpdateQuery)delete)));

        assertEquals(Sql.Select.class, Sql.clone(select).getClass());
        assertEquals(Sql.Union.class, Sql.clone(union).getClass());
        assertEquals(Sql.Insert.class, Sql.clone(insert).getClass());
        assertEquals(Sql.Update.class, Sql.clone(update).getClass());
        assertEquals(Sql.Delete.class, Sql.clone(delete).getClass());
        assertEquals(Sql.Where.class, Sql.clone(where).getClass());
        assertEquals(Sql.FinalQuery.class, Sql.clone(finalQuery).getClass());
        assertEquals(Sql.FinalUpdateQuery.class, Sql.clone(finalUpdateQuery).getClass());

        assertEquals(Sql.FinalQuery.class, Sql.clone((Sql.Query)select).getClass());
        assertEquals(Sql.FinalQuery.class, Sql.clone((Sql.Query)union).getClass());
        assertEquals(Sql.FinalUpdateQuery.class, Sql.clone((Sql.UpdateQuery)insert).getClass());
        assertEquals(Sql.FinalUpdateQuery.class, Sql.clone((Sql.UpdateQuery)update).getClass());
        assertEquals(Sql.FinalUpdateQuery.class, Sql.clone((Sql.UpdateQuery)delete).getClass());
    }

    @Test
    public void resolve() {
        assertEquals("x=1 AND y='?\"'2 AND z=\"\\\"?\"3",
                Sql.resolve(new Sql.Select()
                        .where("x=?", 1)
                        .andWhere("y='?\"'?", 2)
                        .andWhere("z=\"\\\"?\"?", 3)));
        assertEquals("WHERE x=1 AND y='?\"'2 AND z=\"\\\"?\"3",
                Sql.resolve(new Sql.Update()
                        .where("x=?", 1)
                        .andWhere("y='?\"'?", 2)
                        .andWhere("z=\"\\\"?\"?", 3)));

        assertEquals("SELECT a WHERE x=1 AND y in (SELECT b WHERE x=2) AND z=3",
                Sql.resolve(new Sql.Select()
                        .select("a")
                        .where("x=?", 1)
                        .andWhere("y in ?", new Sql.Select()
                                .select("b")
                                .where("x=?", 2))
                        .andWhere("z=?", 3)));
    }

    @Test
    public void expands() {
        assertQuery(Sql.expands(new Sql.Select()
                .where("x=?", 1)
                .andWhere("y='?\"'?", 2)
                .andWhere("z=\"\\\"?\"?", 3)),
            "x=? AND y='?\"'? AND z=\"\\\"?\"?",
            1, 2, 3);
        assertQuery(Sql.expands(new Sql.Update()
                .where("x=?", 1)
                .andWhere("y='?\"'?", 2)
                .andWhere("z=\"\\\"?\"?", 3)),
            "WHERE x=? AND y='?\"'? AND z=\"\\\"?\"?",
            1, 2, 3);

        assertQuery(Sql.expands(new Sql.Select()
                .select("a")
                .where("x=?", 1)
                .andWhere("y in ?", new Sql.Select()
                        .select("b")
                        .where("x=?", 2))
                .andWhere("z=?", 3)),
            "SELECT a WHERE x=? AND y in (SELECT b WHERE x=?) AND z=?",
            1, 2, 3);
    }

    @Test
    public void table() {
        assertEquals("table", Sql.table("table"));
        assertEquals("table", Sql.table("table", null));
        assertEquals("table AS t", Sql.table("table", "t"));
        assertEquals("Select", Sql.table(Sql.Select.class));
        assertEquals("Select", Sql.table(Sql.Select.class, null));
        assertEquals("Select AS t", Sql.table(Sql.Select.class, "t"));
    }

    @Test
    public void quote() {
        assertEquals("'Hello World'", Sql.quote("Hello World"));
        assertEquals("'Hello \\'World\\''", Sql.quote("Hello 'World'"));
        assertEquals("'Hello\\\\World'", Sql.quote("Hello\\World"));
        assertEquals("'Hello\\\\\\'World\\''", Sql.quote("Hello\\'World'"));
    }

    @Test
    public void likeEscape() {
        assertEquals("Hello World", Sql.likeEscape("Hello World"));
        assertEquals("\\%Hello\\_\\\\\\%World\\_", Sql.likeEscape("%Hello_\\%World_"));
    }

    @Test
    public void inline() {
        assertEquals("'Hello World'", Sql.inline("Hello World"));
    }

    @Test
    public void inlineParamPrimitive() {
        assertEquals("NULL", Sql.inlineParam(null));
        assertEquals("123", Sql.inlineParam(123));
        assertEquals("'Hello World'", Sql.inlineParam("Hello World"));
    }

    @Test
    public void inlineParamList() {
        assertEquals("(1, 2, 3)", Sql.inlineParam(new Integer[] {1, 2, 3}));
        assertEquals("('Hello', 'cool', 'World')", Sql.inlineParam(new String[] {"Hello", "cool", "World"}));
        assertEquals("(1)", Sql.inlineParam(new Integer[] {1}));
        assertEquals("", Sql.inlineParam(new Integer[] {}));
        assertEquals("(0, 1, 2)", Sql.inlineParam(Arrays.asList(0, 1, 2)));
        assertEquals("(1, 2, 3)", Sql.inlineParam(new int[] {1, 2, 3}));
    }

    @Test
    public void inlineParamDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2010, 12-1, 31, 23, 59, 49);
        Date date = calendar.getTime();
        Timestamp timestamp = new Timestamp(date.getTime());
        java.sql.Date sqlDate = new java.sql.Date(date.getTime());
        assertEquals("'2010-12-31 23:59:49'", Sql.inlineParam(date));
        assertEquals("'2010-12-31 23:59:49'", Sql.inlineParam(calendar));
        assertEquals("'2010-12-31 23:59:49'", Sql.inlineParam(timestamp));
        assertEquals("'2010-12-31 23:59:49'", Sql.inlineParam(sqlDate));

        calendar.set(1, 0, 1, 0, 0, 0);
        assertEquals("'0001-01-01 00:00:00'", Sql.inlineParam(calendar));
    }

    @Test
    public void inlineParamInlineableQuery() {
        assertEquals("(SELECT a WHERE x=1)", Sql.inlineParam(Sql.select("a").where("x=?", 1)));
        assertEquals("(x=1)", Sql.inlineParam(Sql.where("x=?", 1)));
        assertEquals("(SELECT a WHERE (x=1) AND y in (SELECT b WHERE z=2))",
                Sql.inlineParam(Sql.select("a").
                        where(Sql.where("x=?", 1)).
                        andWhere("y in ?", Sql.select("b").where("z=?", 2))));
    }

}
