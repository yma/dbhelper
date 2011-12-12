package fr.zenexity.dbhelper;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class JdbcValue {

    public interface Priority {
        int priority();
    }

    public interface Adapter extends Priority {
        <T> T cast(Class<T> clazz, Object value) throws Exception;
    }

    public interface Normalizer extends Priority {
        Object convert(Object value) throws Exception;
    }


    private final List<Adapter> adapters = new ArrayList<Adapter>();
    private final List<Normalizer> valueFromSqlNormalizers = new ArrayList<Normalizer>();
    private final List<Normalizer> valueForSqlNormalizers = new ArrayList<Normalizer>();


    static final JdbcValue defaultAdapters = JdbcValue.createDefaultAdapters();

    public static JdbcValue createDefaultAdapters() {
        JdbcValue jdbcValue = new JdbcValue();
        jdbcValue.register(new StandardAdapter());
        jdbcValue.registerValueFromSqlNormalizer(new StandardValueFromSqlNormalizer());
        return jdbcValue;
    }


    public void register(Adapter adapter) {
        adapters.add(adapter);
        Collections.sort(adapters, priorityComparator());
    }

    public void registerValueFromSqlNormalizer(Normalizer normalizer) {
        valueFromSqlNormalizers.add(normalizer);
        Collections.sort(valueFromSqlNormalizers, priorityComparator());
    }

    public void registerValueForSqlNormalizer(Normalizer normalizer) {
        valueForSqlNormalizers.add(normalizer);
        Collections.sort(valueForSqlNormalizers, priorityComparator());
    }

    public static <T extends Priority> Comparator<T> priorityComparator() {
        return new Comparator<T>() {
            public int compare(T o1, T o2) {
                return Integer.valueOf(o1.priority()).compareTo(Integer.valueOf(o2.priority()));
            }
        };
    }


    public <T> T cast(Class<T> clazz, Object value) throws JdbcValueException {
        try {
            for (Adapter valueAdapter : adapters) {
                T result = valueAdapter.cast(clazz, value);
                if (result != null) return result;
            }
            return clazz.cast(value);
        } catch (Exception e) {
            String valueClass = value == null ? null : value.getClass().getName();
            throw new JdbcValueException(value +" ("+ valueClass +") to "+ clazz.getName(), e);
        }
    }

    public static Object normalize(Collection<Normalizer> normalizers, Object value) throws JdbcValueException {
        try {
            for (Normalizer normalizer : normalizers) {
                Object result = normalizer.convert(value);
                if (result != null) return result;
            }
            return value;
        } catch (Exception e) {
            String valueClass = value == null ? null : value.getClass().getName();
            throw new JdbcValueException("normalize "+ value +" ("+ valueClass +")", e);
        }
    }

    public Object normalizeValueFromSql(Object value) throws JdbcValueException {
        return normalize(valueFromSqlNormalizers, value);
    }

    public Object normalizeValueForSql(Object value) throws JdbcValueException {
        return normalize(valueForSqlNormalizers, value);
    }

    public static class StandardAdapter implements Adapter {
        public int priority() { return 1000; }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> T cast(Class<T> clazz, Object value) throws Exception {
            //TODO cast to primitive types
            if (clazz.isPrimitive()) {
                return (T) value;
            }

            if (clazz.isEnum()) {
                if (value instanceof Number) {
                    return clazz.getEnumConstants()[((Number)value).intValue()];
                } else if (value instanceof String) {
                    return (T) Enum.valueOf((Class)clazz, (String)value);
                }
            }

            if (value instanceof Clob) {
                if (clazz == String.class) {
                    return (T) clobToString((Clob) value);
                }
            }

            return null;
        }
    }

    public static class StandardValueFromSqlNormalizer implements Normalizer {
        public int priority() { return 1000; }

        public Object convert(Object value) throws Exception {
            if (value instanceof BigDecimal) return new Long(((BigDecimal)value).longValue());
            if (value instanceof Clob) return clobToString((Clob) value);
            return value;
        }
    }

    public static class NumberConverter implements Adapter {
        public int priority() { return 900; }

        @SuppressWarnings("unchecked")
        public <T> T cast(Class<T> clazz, Object value) throws Exception {
            if (Number.class.isAssignableFrom(clazz) && value instanceof Number) {
                Number numValue = (Number) value;
                if (clazz == Byte.class) {
                    return (T) Byte.valueOf(numValue.byteValue());
                }
                if (clazz == Short.class) {
                    return (T) Short.valueOf(numValue.shortValue());
                }
                if (clazz == Integer.class) {
                    return (T) Integer.valueOf(numValue.intValue());
                }
                if (clazz == Long.class) {
                    return (T) Long.valueOf(numValue.longValue());
                }
                if (clazz == Float.class) {
                    return (T) Float.valueOf(numValue.floatValue());
                }
                if (clazz == Double.class) {
                    return (T) Double.valueOf(numValue.doubleValue());
                }
            }

            //TODO convert primitive types
            return null;
        }
    }

    public static String clobToString(Clob value) throws SQLException, IOException {
        long length =  value.length();
        if (length < Integer.MIN_VALUE || length > Integer.MAX_VALUE) {
            throw new RuntimeException("CLOB too long : length "+ length);
        }

        Reader reader = value.getCharacterStream();
        try {
            StringBuffer str = new StringBuffer((int) length);
            char[] buf = new char[1024];
            for (int n; (n = reader.read(buf)) != -1; ) str.append(buf, 0, n);
            return str.toString();
        } finally {
            reader.close();
        }
    }

}
