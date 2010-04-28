package fr.zenexity.dbhelper;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcResult {

    private JdbcResult() {
    }

    public interface Factory<T> {
        void init(ResultSet result) throws SQLException, JdbcResultException;
        T create(ResultSet result) throws SQLException, JdbcResultException;
    }


    public static <T> boolean primitive(Class<T> objectClass) {
        return objectClass.isPrimitive()
            || Number.class.isAssignableFrom(objectClass)
            || String.class.isAssignableFrom(objectClass)
            || Character.class.isAssignableFrom(objectClass)
            || Boolean.class.isAssignableFrom(objectClass);
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


    public static <T> PrimitiveFactory<T> primitiveFactory(Class<T> objectClass) {
        return new PrimitiveFactory<T>(objectClass, null);
    }

    public static <T> PrimitiveFactory<T> primitiveFactory(Class<T> objectClass, int columnIndex) {
        return new PrimitiveFactory<T>(objectClass, columnIndex);
    }

    public static <T> PrimitiveFactory<T> primitiveFactory(Class<T> objectClass, String field) {
        return new PrimitiveFactory<T>(objectClass, field);
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
        return new MapFactory();
    }


    public static Object normalizeValue(Object value) {
        if (value instanceof BigDecimal) value = new Long(((BigDecimal)value).longValue());
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <T> T castValue(Class<T> clazz, Object value) {
        value = normalizeValue(value);

        //TODO cast to primitive types
        if (clazz.isPrimitive()) {
            return (T) value;
        }

        if (clazz.isEnum()) {
            if (value instanceof Number) {
                value = clazz.getEnumConstants()[((Number)value).intValue()];
            } else if (value instanceof String) {
                value = Enum.valueOf((Class)clazz, (String)value);
            }
        }

        return clazz.cast(value);
    }

    public static class PrimitiveFactory<T> implements Factory<T> {
        private final Class<T> objectClass;
        private final String field;
        private int columnIndex;

        public PrimitiveFactory(Class<T> objectClass, int columnIndex) {
            this.objectClass = objectClass;
            this.field = null;
            this.columnIndex = columnIndex;
        }

        public PrimitiveFactory(Class<T> objectClass, String field) {
            this.objectClass = objectClass;
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

        public T create(ResultSet result) throws SQLException {
            return castValue(objectClass, result.getObject(columnIndex));
        }
    }

    public static class ClassFactory<T> implements Factory<T> {
        private final Class<T> objectClass;
        private final Set<String> fields;
        private List<ColumnInfo> columns;
        private Map<String, String> labelsToFields;

        public ClassFactory(Class<T> objectClass, Collection<String> fields) {
            this.objectClass = objectClass;
            this.fields = fields == null ? null : new HashSet<String>(fields);
        }

        public Map<String, String> labelsToFields() {
            if (labelsToFields == null) {
                labelsToFields = new HashMap<String, String>();
                for (Field objectField : objectClass.getFields()) {
                    String fieldName = objectField.getName();
                    labelsToFields.put(fieldName.toLowerCase(), fieldName);
                }
            }
            return labelsToFields;
        }

        public void init(ResultSet result) throws SQLException, JdbcResultException {
            Map<String, Integer> fieldsIndexes = new HashMap<String, Integer>();

            ResultSetMetaData meta = result.getMetaData();
            int count = meta.getColumnCount();
            for (int i = 1; i <= count; i++) {
                String label = meta.getColumnLabel(i);
                if (label.length() != 0) {
                    if (!meta.isCaseSensitive(i)) {
                        String name = labelsToFields().get(label.toLowerCase());
                        if (name != null) label = name;
                    }
                    if (fields == null || fields.contains(label))
                        fieldsIndexes.put(label, i);
                }
            }

            if (fields != null) {
                for (String field : fields) {
                    if (!fieldsIndexes.containsKey(field))
                        throw new JdbcResultException(new NoSuchFieldException(field));
                }
            }

            columns = new ArrayList<ColumnInfo>();
            try {
                for (Map.Entry<String, Integer> fieldIndex : fieldsIndexes.entrySet()) {
                    Field objField = objectClass.getField(fieldIndex.getKey());
                    if ((objField.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) != 0)
                        throw new IllegalArgumentException(fieldIndex.getKey());
                    columns.add(new ColumnInfo(fieldIndex.getValue(), objField));
                }
            } catch (Exception e) {
                throw new JdbcResultException(e);
            }
        }

        public T create(ResultSet result) throws SQLException, JdbcResultException {
            try {
                T obj = objectClass.newInstance();
                for (ColumnInfo column : columns) {
                    Object value = result.getObject(column.index);
                    column.field.set(obj, castValue(column.field.getType(), value));
                }
                return obj;
            } catch (SQLException e) {
                throw e;
            } catch (Exception e) {
                throw new JdbcResultException(e);
            }
        }

        private static class ColumnInfo {
            public final int index;
            public final Field field;

            public ColumnInfo(int index, Field field) {
                this.index = index;
                this.field = field;
            }
        }
    }

    public static class MapFactory implements Factory<Map<String, Object>> {
        private Map<String, String> columnInsensitiveFields;
        private List<ColumnInfo> columns;

        public void init(ResultSet result) throws SQLException, JdbcResultException {
            columnInsensitiveFields = new HashMap<String, String>();
            columns = new ArrayList<ColumnInfo>();

            ResultSetMetaData meta = result.getMetaData();
            int count = meta.getColumnCount();
            for (int i = 1; i <= count; i++) {
                String label = meta.getColumnLabel(i);
                if (label.length() != 0) {
                    if (!meta.isCaseSensitive(i))
                        columnInsensitiveFields.put(label.toLowerCase(), label);
                    columns.add(new ColumnInfo(i, label));
                }
            }
        }

        public Map<String, Object> create(ResultSet result) throws SQLException {
            Map<String, Object> map = new FieldHashMap();
            for (ColumnInfo column : columns) {
                Object value = result.getObject(column.index);
                map.put(column.name, normalizeValue(value));
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

        private static class ColumnInfo {
            public final int index;
            public final String name;

            public ColumnInfo(int index, String name) {
                this.index = index;
                this.name = name;
            }
        }
    }

}
