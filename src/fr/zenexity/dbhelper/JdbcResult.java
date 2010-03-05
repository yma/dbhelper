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
        T create(ResultSet result) throws SQLException, JdbcResultException;
    }


    public static <T> boolean primitive(Class<T> objectClass) {
        return objectClass == String.class
            || objectClass == Integer.class
            || objectClass == Float.class
            || objectClass == Double.class
            || objectClass == Long.class
            || objectClass == Short.class
            || objectClass == Byte.class
            || objectClass == Character.class
            || objectClass == Boolean.class;
    }

    public static <T> Factory<T> buildFactory(Class<T> objectClass) {
        return primitive(objectClass)
                ? primitiveFactory(objectClass)
                : classFactory(objectClass);
    }

    public static <T> Factory<T> buildFactory(Class<T> objectClass, String... fields) {
        return primitive(objectClass)
                ? primitiveFactory(objectClass, fields == null || fields.length == 0 ? null : fields[0])
                : classFactory(objectClass, fields);
    }

    public static <T> Factory<T> buildFactory(Class<T> objectClass, List<String> fields) {
        return primitive(objectClass)
                ? primitiveFactory(objectClass, fields == null || fields.isEmpty() ? null : fields.get(0))
                : classFactory(objectClass, fields);
    }


    public static <T> PrimitiveFactory<T> primitiveFactory(@SuppressWarnings("unused") Class<T> objectClass) {
        return new PrimitiveFactory<T>(null);
    }

    public static <T> PrimitiveFactory<T> primitiveFactory(@SuppressWarnings("unused") Class<T> objectClass, int columnIndex) {
        return new PrimitiveFactory<T>(columnIndex);
    }

    public static <T> PrimitiveFactory<T> primitiveFactory(@SuppressWarnings("unused") Class<T> objectClass, String field) {
        return new PrimitiveFactory<T>(field);
    }


    public static <T> ClassFactory<T> classFactory(Class<T> objectClass) {
        return new ClassFactory<T>(objectClass, null);
    }

    public static <T> ClassFactory<T> classFactory(Class<T> objectClass, String... fields) {
        return new ClassFactory<T>(objectClass, Arrays.asList(fields));
    }

    public static <T> ClassFactory<T> classFactory(Class<T> objectClass, List<String> fields) {
        return new ClassFactory<T>(objectClass, fields);
    }


    public static MapFactory mapFactory() {
        return new MapFactory(null);
    }

    public static MapFactory mapFactory(String... fields) {
        return new MapFactory(Arrays.asList(fields));
    }

    public static MapFactory mapFactory(List<String> fields) {
        return new MapFactory(fields);
    }


    public static class PrimitiveFactory<T> implements Factory<T> {

        private final String field;
        private int columnIndex;

        public PrimitiveFactory(int columnIndex) {
            this.field = null;
            this.columnIndex = columnIndex;
        }

        public PrimitiveFactory(String field) {
            this.field = field;
            this.columnIndex = 1;
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

        public T create(ResultSet result) throws SQLException, JdbcResultException {
            try {
                T obj = objectClass.newInstance();
                for (String field : fields) {
                    Object value = result.getObject(field);
                    if (value instanceof BigDecimal) value = new Long(((BigDecimal)value).longValue());
                    objectClass.getDeclaredField(field).set(obj, value);
                }
                return obj;
            } catch (InstantiationException ex) {
                throw new JdbcResultException(ex);
            } catch (NoSuchFieldException ex) {
                throw new JdbcResultException(ex);
            } catch (IllegalAccessException ex) {
                throw new JdbcResultException(ex);
            }
        }

    }

    public static class MapFactory implements Factory<Map<String, Object>> {

        private List<String> fields;
        private final Map<String, String> columnInsensitiveFields;

        public MapFactory(List<String> fields) {
            this.fields = fields;
            columnInsensitiveFields = new HashMap<String, String>();
        }

        public void init(ResultSet result) throws SQLException {
            List<String> columnFields = new ArrayList<String>();

            ResultSetMetaData meta = result.getMetaData();
            int count = meta.getColumnCount();
            for (int i = 1; i <= count; i++) {
                String label = meta.getColumnLabel(i);
                if (label.length() != 0) {
                    columnFields.add(label);
                    if (!meta.isCaseSensitive(i)) columnInsensitiveFields.put(label.toLowerCase(), label);
                }
            }

            if (fields == null) fields = columnFields;
            else {
                for (int i = 0; i < fields.size(); i++) {
                    String key = fields.get(i).toLowerCase();
                    String label = columnInsensitiveFields.get(key);
                    if (label != null) {
                        columnInsensitiveFields.put(key, fields.get(i));
                        fields.set(i, label);
                    }
                }
            }
        }

        public Map<String, Object> create(ResultSet result) throws SQLException {
            Map<String, Object> map = new FieldHashMap();
            for (String field : fields) {
                Object value = result.getObject(field);
                if (value instanceof BigDecimal) value = new Long(((BigDecimal)value).longValue());
                map.put(field, value);
            }
            return map;
        }

        private class FieldHashMap extends HashMap<String, Object> {
            @Override
            public Object put(String key, Object value) {
                String insensitiveKey = columnInsensitiveFields.get(key.toLowerCase());
                if (insensitiveKey != null) key = insensitiveKey;
                return super.put(key, value);
            }

            @Override
            public Object get(Object key) {
                String insensitiveKey = columnInsensitiveFields.get(key.toString().toLowerCase());
                if (insensitiveKey != null) key = insensitiveKey;
                return super.get(key);
            }
        }

    }

}
