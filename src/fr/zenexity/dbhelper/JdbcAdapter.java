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
import java.util.LinkedList;
import java.util.List;

public class JdbcAdapter {

    public interface Priority {
        int priority();
    }

    public interface Caster extends Priority {
        <T> T cast(Class<T> clazz, Object value) throws Exception;
    }

    public interface Normalizer extends Priority {
        Object normalize(Object value) throws Exception;
    }

    public interface SqlEncoder extends Normalizer {}
    public interface SqlDecoder extends Normalizer {}

    public static <T extends Priority> Comparator<T> priorityComparator() {
        return new Comparator<T>() {
            public int compare(T o1, T o2) {
                return Integer.valueOf(o1.priority()).compareTo(Integer.valueOf(o2.priority()));
            }
        };
    }


    public static class Builder {
        private final List<Caster> casters = new LinkedList<Caster>();
        private final List<SqlDecoder> decoders = new LinkedList<SqlDecoder>();
        private final List<SqlEncoder> encoders = new LinkedList<SqlEncoder>();

        public Builder register(Caster caster) {
            casters.add(caster);
            Collections.sort(casters, priorityComparator());
            return this;
        }

        public Builder register(SqlDecoder normalizer) {
            decoders.add(normalizer);
            Collections.sort(decoders, priorityComparator());
            return this;
        }

        public Builder register(SqlEncoder normalizer) {
            encoders.add(normalizer);
            Collections.sort(encoders, priorityComparator());
            return this;
        }

        public JdbcAdapter create() {
            return new JdbcAdapter(casters, decoders, encoders);
        }
    }

    public static final JdbcAdapter defaultAdapter = defaultBuilder().create();

    public static Builder defaultBuilder() {
        return new Builder()
            .register(new StandardCaster())
            .register(new StandardSqlValueDecoder());
    }


    private final List<Caster> casters;
    private final List<SqlDecoder> sqlDecoders;
    private final List<SqlEncoder> sqlEncoders;

    private JdbcAdapter(Collection<Caster> casters, Collection<SqlDecoder> decoders, Collection<SqlEncoder> encoders) {
        this.casters = new ArrayList<Caster>(casters);
        this.sqlDecoders = new ArrayList<SqlDecoder>(decoders);
        this.sqlEncoders = new ArrayList<SqlEncoder>(encoders);
    }

    public <T> T cast(Class<T> clazz, Object value) throws JdbcAdapterException {
        try {
            for (Caster caster : casters) {
                T result = caster.cast(clazz, value);
                if (result != null) return result;
            }
            return clazz.cast(value);
        } catch (Exception e) {
            String valueClass = value == null ? null : value.getClass().getName();
            throw new JdbcAdapterException(value +" ("+ valueClass +") to "+ clazz.getName(), e);
        }
    }

    public static Object normalize(Collection<? extends Normalizer> normalizers, Object value) throws JdbcAdapterException {
        try {
            for (Normalizer normalizer : normalizers) {
                Object result = normalizer.normalize(value);
                if (result != null) return result;
            }
            return value;
        } catch (Exception e) {
            String valueClass = value == null ? null : value.getClass().getName();
            throw new JdbcAdapterException("normalize "+ value +" ("+ valueClass +")", e);
        }
    }

    public Object decodeSqlValue(Object value) throws JdbcAdapterException {
        return normalize(sqlDecoders, value);
    }

    public Object encodeSqlValue(Object value) throws JdbcAdapterException {
        return normalize(sqlEncoders, value);
    }

    public static class StandardCaster implements Caster {
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

    public static class StandardSqlValueDecoder implements SqlDecoder {
        public int priority() { return 1000; }

        public Object normalize(Object value) throws Exception {
            if (value instanceof BigDecimal) return new Long(((BigDecimal)value).longValue());
            if (value instanceof Clob) return clobToString((Clob) value);
            return null;
        }
    }

    public static class NumberConverter implements Caster {
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
