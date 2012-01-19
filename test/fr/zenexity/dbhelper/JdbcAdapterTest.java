package fr.zenexity.dbhelper;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

public class JdbcAdapterTest extends TestingDatabase {

    private JdbcAdapter adapter;

    @Before
    public void register() {
        adapter = JdbcAdapter.defaultAdapter;
    }

    @Test
    public void testDecodeSqlValue() {
        assertEquals(new Integer(213), adapter.decodeSqlValue(new Integer(213)));
        assertEquals(new Long(213), adapter.decodeSqlValue(new Long(213)));
        assertEquals(new Long(213), adapter.decodeSqlValue(new BigDecimal(213)));
    }

    @Test
    public void testEncodeSqlValue() {
        assertEquals(new Integer(213), adapter.encodeSqlValue(new Integer(213)));
        assertEquals(new Long(213), adapter.encodeSqlValue(new Long(213)));
        assertEquals(new BigDecimal(213), adapter.encodeSqlValue(new BigDecimal(213)));
    }

    @Test
    public void testDate2SqlTimestampEncoder() {
        Date date = new Date();
        assertEquals(date, adapter.encodeSqlValue(date));
        adapter = JdbcAdapter.defaultBuilder()
                .register(new JdbcAdapter.Date2SqlTimestampEncoder())
                .create();
        assertEquals(new Timestamp(date.getTime()), adapter.encodeSqlValue(date));
    }

    @Test
    public void testCastValue() {
        assertEquals(new Integer(213), adapter.cast(Integer.class, new Integer(213)));
        assertEquals(new BigDecimal(213), adapter.cast(Number.class, new BigDecimal(213)));
    }

    @Test
    public void testEnumCastValue() {
        assertEquals(Entry.DistType.DEBIAN, adapter.cast(Entry.DistType.class, "DEBIAN"));
        assertEquals(Entry.DistType.UBUNTU, adapter.cast(Entry.DistType.class, Entry.DistType.UBUNTU.ordinal()));
        assertEquals(Entry.DistType.FEDORA, adapter.cast(Entry.DistType.class, Entry.DistType.FEDORA));
    }

    @Test
    public void testDateCastDateValue() {
        Date date = new Date();
        assertEquals(date, adapter.cast(Date.class, new Timestamp(date.getTime())));
    }

    @Test
    public void testNumberConverter() {
        try {
            assertEquals(new Integer(123), adapter.cast(Integer.class, new Byte((byte)123)));
            fail("JdbcAdapterException excepted");
        } catch (JdbcAdapterException e) {
            assertEquals(ClassCastException.class, e.getCause().getClass());
            assertEquals("123 (java.lang.Byte) to java.lang.Integer", e.getMessage());
        }

        adapter = new JdbcAdapter.Builder()
                .register(new JdbcAdapter.NumberConverter(Number.class))
                .create();
        assertEquals(new Integer(123), adapter.cast(Integer.class, new Byte((byte)123)));
        assertEquals(new Integer(213), adapter.cast(Integer.class, new Integer(213)));
        assertEquals(new Byte((byte)123), adapter.cast(Byte.class, new Integer(123)));
        assertEquals(new Byte((byte)123), adapter.cast(Byte.class, new BigDecimal(123)));
        assertEquals(new Short((short)213), adapter.cast(Short.class, new BigDecimal(213)));
        assertEquals(new Integer(213), adapter.cast(Integer.class, new BigDecimal(213)));
        assertEquals(new Long(213), adapter.cast(Long.class, new BigDecimal(213)));
    }

    @Test
    public void testNumberConverterBigDecimalOnly() {
        adapter = new JdbcAdapter.Builder().create();
        try {
            adapter.cast(Long.class, new BigDecimal(123));
            fail("JdbcAdapterException excepted");
        } catch (JdbcAdapterException e) {
            assertEquals(ClassCastException.class, e.getCause().getClass());
            assertEquals("123 (java.math.BigDecimal) to java.lang.Long", e.getMessage());
        }

        adapter = new JdbcAdapter.Builder()
                .register(new JdbcAdapter.NumberConverter(BigDecimal.class))
                .create();
        try {
            adapter.cast(Integer.class, new Byte((byte)123));
            fail("JdbcAdapterException excepted");
        } catch (JdbcAdapterException e) {
            assertEquals(ClassCastException.class, e.getCause().getClass());
            assertEquals("123 (java.lang.Byte) to java.lang.Integer", e.getMessage());
        }
        assertEquals(new Byte((byte)123), adapter.cast(Byte.class, new BigDecimal(123)));
        assertEquals(new Short((short)213), adapter.cast(Short.class, new BigDecimal(213)));
        assertEquals(new Integer(213), adapter.cast(Integer.class, new BigDecimal(213)));
        assertEquals(new Long(213), adapter.cast(Long.class, new BigDecimal(213)));
    }

}
