package fr.zenexity.dbhelper;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcResult {

    private JdbcResult() {
    }

    public interface Factory<T> {
        void init(ResultSet result) throws SQLException;
        T create(ResultSet result) throws SQLException;
    }

    public static <T> Factory<T> buildFactory(Class<T> objectClass) {
        return buildFactory(objectClass, (List<String>)null);
    }

    public static <T> Factory<T> buildFactory(Class<T> objectClass, String... fields) {
        return buildFactory(objectClass, Arrays.asList(fields));
    }

    public static <T> Factory<T> buildFactory(Class<T> objectClass, List<String> fields) {
        return objectClass == String.class
            || objectClass == Integer.class
            || objectClass == Float.class
            || objectClass == Double.class
            || objectClass == Long.class
            || objectClass == Short.class
            || objectClass == Byte.class
            || objectClass == Character.class
            || objectClass == Boolean.class
                ? new PrimitiveFactory<T>(objectClass, fields)
                : new ClassFactory<T>(objectClass, fields);
    }



    public static <T> Factory<T> buildPrimitiveFactory(Class<T> objectClass) {
        return buildPrimitiveFactory(objectClass, 1);
    }

    public static <T> Factory<T> buildPrimitiveFactory(Class<T> objectClass, int columnIndex) {
        return new PrimitiveFactory<T>(objectClass, columnIndex);
    }

    public static <T> Factory<T> buildPrimitiveFactory(Class<T> objectClass, String field) {
        return new PrimitiveFactory<T>(objectClass, field);
    }



    public static <T> Factory<T> buildClassFactory(Class<T> objectClass) {
        return buildClassFactory(objectClass, (List<String>)null);
    }

    public static <T> Factory<T> buildClassFactory(Class<T> objectClass, String... fields) {
        return buildClassFactory(objectClass, Arrays.asList(fields));
    }

    public static <T> Factory<T> buildClassFactory(Class<T> objectClass, List<String> fields) {
        return new ClassFactory<T>(objectClass, fields);
    }



    public static class PrimitiveFactory<T> implements Factory<T> {

        private final String field;
        private int columnIndex;

        public PrimitiveFactory(Class<T> objectClass, int columnIndex) {
            this.field = null;
            this.columnIndex = columnIndex;
        }

        public PrimitiveFactory(Class<T> objectClass, String field) {
            this.field = field;
            this.columnIndex = 1;
        }

        public PrimitiveFactory(Class<T> objectClass, List<String> fields) {
            this(objectClass, fields == null || fields.isEmpty() ? null : fields.get(0));
        }

        public void init(ResultSet result) throws SQLException {
            if (field != null) {
                ResultSetMetaData meta = result.getMetaData();
                int count = meta.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    String label = meta.getColumnLabel(i);
                    if (meta.isCaseSensitive(i) ? label.equals(field) : label.equalsIgnoreCase(field)) {
                        columnIndex = i;
                        break;
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")
        public T create(ResultSet result) throws SQLException {
            Object value = result.getObject(columnIndex);
            if (value instanceof BigDecimal) value = new Long(((BigDecimal)value).longValue());
            return (T) value;
        }

    }

    public static class ClassFactory<T> implements Factory<T> {

        private final Class<T> objectClass;
        private List<String> fields;

        public ClassFactory(Class<T> objectClass, List<String> fields) {
            this.objectClass = objectClass;
            this.fields = fields;
        }

        public Map<String, String> labelsToFields() {
            Map<String, String> map = new HashMap<String, String>();
            for (Field objectField : objectClass.getDeclaredFields()) {
                String fieldName = objectField.getName();
                map.put(fieldName.toLowerCase(), fieldName);
            }
            return map;
        }

        public void init(ResultSet result) throws SQLException {
            if (fields == null) {
                Map<String, String> labelsToFields = null;
                fields = new ArrayList<String>();
                ResultSetMetaData meta = result.getMetaData();
                int count = meta.getColumnCount();
                for (int i = 1; i <= count; i++) {
                    String label = meta.getColumnLabel(i);
                    if (label.length() != 0) {
                        if (!meta.isCaseSensitive(i)) {
                            if (labelsToFields == null) labelsToFields = labelsToFields();
                            String name = labelsToFields.get(label.toLowerCase());
                            if (name != null) label = name;
                        }
                        fields.add(label);
                    }
                }
            }
        }

        public T create(ResultSet result) throws SQLException {
            try {
                T obj = objectClass.newInstance();
                for (String field : fields) {
                    Object value = result.getObject(field);
                    if (value instanceof BigDecimal) value = new Long(((BigDecimal)value).longValue());
                    objectClass.getDeclaredField(field).set(obj, value);
                }
                return obj;
            } catch (InstantiationException ex) {
                throw new RuntimeException(ex);
            } catch (NoSuchFieldException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

}
