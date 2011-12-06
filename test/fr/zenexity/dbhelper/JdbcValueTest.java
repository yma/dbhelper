package fr.zenexity.dbhelper;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

public class JdbcValueTest extends TestingDatabase {

    private JdbcValue jdbcValue = new JdbcValue();

    @Before
    public void register() {
        jdbcValue.register(new JdbcValue.StandardAdapter());
    }

    @Test
    public void testNormalize() {
        assertEquals(new Integer(213), jdbcValue.normalize(new Integer(213)));
        assertEquals(new Long(213), jdbcValue.normalize(new Long(213)));
        assertEquals(new Long(213), jdbcValue.normalize(new BigDecimal(213)));
    }

    @Test
    public void testCastValue() {
        assertEquals(new Integer(213), jdbcValue.cast(Integer.class, new Integer(213)));
        assertEquals(new BigDecimal(213), jdbcValue.cast(Number.class, new BigDecimal(213)));
    }

    @Test
    public void testNumberConverter() {
        jdbcValue.register(new JdbcValue.NumberConverter());
        assertEquals(new Integer(123), jdbcValue.cast(Integer.class, new Byte((byte)123)));
        assertEquals(new Integer(213), jdbcValue.cast(Integer.class, new Integer(213)));
        assertEquals(new Byte((byte)123), jdbcValue.cast(Byte.class, new Integer(123)));
        assertEquals(new Byte((byte)123), jdbcValue.cast(Byte.class, new BigDecimal(123)));
        assertEquals(new Short((short)213), jdbcValue.cast(Short.class, new BigDecimal(213)));
        assertEquals(new Integer(213), jdbcValue.cast(Integer.class, new BigDecimal(213)));
        assertEquals(new Long(213), jdbcValue.cast(Long.class, new BigDecimal(213)));
    }

    @Test
    public void testEnumCastValue() {
        assertEquals(Entry.DistType.DEBIAN, jdbcValue.cast(Entry.DistType.class, "DEBIAN"));
        assertEquals(Entry.DistType.UBUNTU, jdbcValue.cast(Entry.DistType.class, Entry.DistType.UBUNTU.ordinal()));
        assertEquals(Entry.DistType.FEDORA, jdbcValue.cast(Entry.DistType.class, Entry.DistType.FEDORA));
    }

    @Test
    public void testDateCastDateValue() {
        Date date = new Date();
        assertEquals(date, jdbcValue.cast(Date.class, new Timestamp(date.getTime())));
    }

}
