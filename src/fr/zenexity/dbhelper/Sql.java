package fr.zenexity.dbhelper;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class Sql {

    private Sql() {
    }

    public static class Concat {
        private String prefix, localPrefix, separator, suffix;
        private String defaultValue;
        private String value;

        public Concat(String prefix, String separator, String suffix) {
            this.prefix = prefix;
            this.localPrefix = null;
            this.separator = separator;
            this.suffix = suffix;
            this.defaultValue = "";
            this.value = "";
        }

        public Concat(String prefix, String separator) {
            this(prefix, separator, "");
        }

        public Concat(Concat src) {
            this.prefix = src.prefix;
            this.localPrefix = src.localPrefix;
            this.separator = src.separator;
            this.suffix = src.suffix;
            this.defaultValue = src.defaultValue;
            this.value = src.value;
        }

        public Concat defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Concat prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Concat localPrefix(String localPrefix) {
            this.localPrefix = localPrefix;
            return this;
        }

        public Concat separator(String separator) {
            this.separator = separator;
            return this;
        }

        public Concat suffix(String suffix) {
            this.suffix = suffix;
            return this;
        }

        public Concat append(Object obj) {
            final String text;
            if (obj != null) {
                String objStr = obj.toString();
                if (objStr.length() > 0) text = objStr;
                else text = defaultValue;
            } else text = defaultValue;

            if (text != null) {
                if (value.length() > 0) {
                    if (separator == null) throw new NullPointerException();
                    value += separator;
                } else if (localPrefix != null) {
                    value += localPrefix;
                    localPrefix = null;
                }
                value += text;
            }
            return this;
        }

        public Concat add(Object... objs) {
            for (Object obj : objs) append(obj);
            return this;
        }

        public boolean isEmpty() {
            return value.length()<=0;
        }

        @Override
        public String toString() {
            if (isEmpty()) return "";
            if (prefix == null || suffix == null) throw new NullPointerException();
            return prefix + value + suffix;
        }
    }

    public static class ConcatWithParams extends Concat {
        public final List<Object> params;

        public ConcatWithParams(String prefix, String separator, String suffix) {
            super(prefix, separator, suffix);
            params = new ArrayList<Object>();
        }

        public ConcatWithParams(String prefix, String separator) {
            this(prefix, separator, "");
        }

        public ConcatWithParams(ConcatWithParams src) {
            super(src);
            params = new ArrayList<Object>(src.params);
        }

        public ConcatWithParams param(Object obj) {
            params.add(obj);
            return this;
        }

        public ConcatWithParams params(Object... objs) {
            for (Object obj : objs) params.add(obj);
            return this;
        }

        public ConcatWithParams paramsList(Iterable<?> objs) {
            for (Object obj : objs) params.add(obj);
            return this;
        }
    }

    public interface InlineableQuery {
        @Override String toString();
        Iterable<Object> params();
        List<Object> copyParams();
    }

    public static class Where implements InlineableQuery {
        private final ConcatWithParams query;

        public Where() {
            query = new ConcatWithParams("", null);
            query.defaultValue(null);
        }

        public Where(Where src) {
            query = new ConcatWithParams(src.query);
        }

        private void append(String sep, String expr, Object... params) {
            query.separator(sep);
            int index = 0;
            for (Object param : params) {
                if (param instanceof InlineableQuery) {
                    FinalQuery expandedExpr = expands(expr, Arrays.asList(param), index, index+1);
                    expr = expandedExpr.toString();
                    query.params.addAll(expandedExpr.params);
                    index += expandedExpr.params.size();
                } else {
                    query.param(param);
                    index++;
                }
            }
            query.append(expr);
        }

        private void subWhere(String sep, Where where) {
            query.paramsList(where.query.params).separator(sep);
            if (!where.query.isEmpty()) query.append("(" + where.toString() + ")");
        }

        public Where and(String expr, Object... params) { append(" AND ", expr, params); return this; }
        public Where and(Where where) { subWhere(" AND ", where); return this; }
        public Where or(String expr, Object... params) { append(" OR ", expr, params); return this; }
        public Where or(Where where) { subWhere(" OR ", where); return this; }

        public static Where key(Object obj, String... keyFields) throws SqlException {
            Where keyWhere = new Where();
            Class<?> objClass = obj.getClass();
            try {
                for (String field : keyFields) {
                    keyWhere.and(field+"=?", objClass.getDeclaredField(field).get(obj));
                }
            } catch (Exception e) {
                throw new SqlException(e);
            }
            return keyWhere;
        }

        public static Where key(Map<String, ?> map, String... keyFields) {
            Where keyWhere = new Where();
            for (String field : keyFields) keyWhere.and(field+"=?", map.get(field));
            return keyWhere;
        }

        @Override
        public String toString() {
            return query.toString();
        }

        public Iterable<Object> params() {
            return query.params;
        }

        public List<Object> copyParams() {
            return new ArrayList<Object>(query.params);
        }

        public Object[] paramsArray() {
            return query.params.toArray();
        }
    }

    public interface Query extends InlineableQuery {
    }

    public interface UpdateQuery {
        @Override String toString();
        Iterable<Object> params();
        List<Object> copyParams();
    }

    public static final class FinalQuery implements Query {
        public final String query;
        public final List<Object> params;

        public FinalQuery(Query src) {
            query = src.toString();
            params = src.copyParams();
        }

        public FinalQuery(String src) {
            query = src;
            params = new ArrayList<Object>();
        }

        public FinalQuery(String src, Object... params) {
            this(src);
            for (Object param : params) this.params.add(param);
        }

        public FinalQuery(Query src, Object param) {
            this(src);
            this.params.add(param);
        }

        public FinalQuery(Query src, Object... params) {
            this(src);
            for (Object param : params) this.params.add(param);
        }

        public FinalQuery(Query src, Iterable<?> params) {
            this(src);
            for (Object param : params) this.params.add(param);
        }

        @Override
        public String toString() {
            return query;
        }

        public Iterable<Object> params() {
            return params;
        }

        public List<Object> copyParams() {
            return new ArrayList<Object>(params);
        }
    }

    public static final class FinalUpdateQuery implements UpdateQuery {
        public final String query;
        public final List<Object> params;

        public FinalUpdateQuery(UpdateQuery src) {
            query = src.toString();
            params = src.copyParams();
        }

        public FinalUpdateQuery(String src) {
            query = src;
            params = new ArrayList<Object>();
        }

        public FinalUpdateQuery(String src, Object... params) {
            this(src);
            for (Object param : params) this.params.add(param);
        }

        public FinalUpdateQuery(UpdateQuery src, Object param) {
            this(src);
            this.params.add(param);
        }

        public FinalUpdateQuery(UpdateQuery src, Object... params) {
            this(src);
            for (Object param : params) this.params.add(param);
        }

        public FinalUpdateQuery(UpdateQuery src, Iterable<?> params) {
            this(src);
            for (Object param : params) this.params.add(param);
        }

        @Override
        public String toString() {
            return query;
        }

        public Iterable<Object> params() {
            return params;
        }

        public List<Object> copyParams() {
            return new ArrayList<Object>(params);
        }
    }

    public enum JoinType {
        JOIN("JOIN"),
        INNER("INNER JOIN"),
        LEFT("LEFT JOIN"),
        RIGHT("RIGHT JOIN"),
        FULL("FULL JOIN"),
        LEFT_OUTER("LEFT OUTER JOIN"),
        RIGHT_OUTER("RIGHT OUTER JOIN"),
        FULL_OUTER("FULL OUTER JOIN"),
        CROSS("CROSS JOIN"),
        NATURAL("NATURAL JOIN"),
        NATURAL_LEFT("NATURAL LEFT JOIN"),
        NATURAL_RIGHT("NATURAL RIGHT JOIN"),
        NATURAL_LEFT_OUTER("NATURAL LEFT OUTER JOIN"),
        NATURAL_RIGHT_OUTER("NATURAL RIGHT OUTER JOIN"),
        STRAIGHT_JOIN("STRAIGHT_JOIN");

        public final String value;
        private JoinType(String value) { this.value = value; }
    }

    public static final class Select implements Query {
        private final ConcatWithParams with;
        private final Concat select;
        private final ConcatWithParams from;
        public final Where where;
        private final Concat groupBy;
        public final Where having;
        private final Concat orderBy;
        private final Concat offset;
        private final Concat limit;

        public Select() {
            with = new ConcatWithParams("WITH ", ", ");
            select = new Concat("SELECT ", ", ");
            select.defaultValue(null);
            from = new ConcatWithParams("", null);
            from.defaultValue(null);
            where = new Where();
            where.query.prefix("WHERE ").suffix("");
            groupBy = new Concat("GROUP BY ", ", ");
            groupBy.defaultValue(null);
            having = new Where();
            having.query.prefix("HAVING ").suffix("");
            orderBy = new Concat("ORDER BY ", ", ");
            orderBy.defaultValue(null);
            offset = new Concat("OFFSET ", null);
            limit = new Concat("LIMIT ", null);
        }

        public Select(Select src) {
            with = new ConcatWithParams(src.with);
            select = new Concat(src.select);
            from = new ConcatWithParams(src.from);
            where = new Where(src.where);
            groupBy = new Concat(src.groupBy);
            having = new Where(src.having);
            orderBy = new Concat(src.orderBy);
            offset = new Concat(src.offset);
            limit = new Concat(src.limit);
        }

        private ConcatWithParams _from() { from.localPrefix("FROM ").separator(", "); return from; }

        public Select with(String table, Query subquery) { with.paramsList(subquery.params()).append(table + " AS (" + subquery + ")"); return this; }
        public Select with(String table, UpdateQuery subquery) { with.paramsList(subquery.params()).append(table + " AS (" + subquery + ")"); return this; }

        public Select all() { select.prefix = "SELECT ALL "; return this; }
        public Select distinct() { select.prefix = "SELECT DISTINCT "; return this; }

        public Select select(Object expression) { select.append(expression); return this; }
        public Select select(Object... expressions) { select.add(expressions); return this; }

        public Select from(String table) { _from().append(table); return this; }
        public Select from(String... tables) { _from().separator(", ").add((Object[])tables); return this; }
        public Select from(Class<?> clazz) { _from().append(clazz.getSimpleName()); return this; }
        public Select from(Class<?>... classes) { _from(); for (Class<?> clazz : classes) from.append(clazz.getSimpleName()); return this; }
        public Select from(Query subquery, String name) { _from().paramsList(subquery.params()).append("(" + subquery + ") AS " + name); return this; }

        private Select _join(String prefix, String expr, Object... params) { from.params(params).localPrefix(prefix +" ").separator(" "+ prefix +" ").append(expr); return this; }
        private Select _join(String prefix, String table, Where on) { return _join(prefix, table +" ON "+ on.toString(), on.paramsArray()); }
        private Select _join(String prefix, Query subquery, String alias) { from.paramsList(subquery.params()); return _join(prefix, "("+ subquery +") AS "+ alias); }
        private Select _join(String prefix, Query subquery, String alias, Where on) { from.paramsList(subquery.params()); return _join(prefix, "("+ subquery +") AS "+ alias, on); }
        public Select join(JoinType joinType, String expr, Object... params) { return _join(joinType.value, expr, params); }
        public Select join(JoinType joinType, String table, Where on) { return _join(joinType.value, table, on); }
        public Select join(JoinType joinType, Query subquery, String alias) { return _join(joinType.value, subquery, alias); }
        public Select join(JoinType joinType, Query subquery, String alias, Where on) { return _join(joinType.value, subquery, alias, on); }
        public Select freeJoin(String prefix, String expr, Object... params) { return _join(prefix +" "+ JoinType.JOIN.value, expr, params); }
        public Select freeJoin(String prefix, String table, Where on) { return _join(prefix +" "+ JoinType.JOIN.value, table, on ); }
        public Select freeJoin(String prefix, Query subquery, String alias) { return _join(prefix +" "+ JoinType.JOIN.value, subquery, alias); }
        public Select freeJoin(String prefix, Query subquery, String alias, Where on) { return _join(prefix +" "+ JoinType.JOIN.value, subquery, alias, on); }
        public Select join(String expr, Object... params) { return join(JoinType.JOIN, expr, params); }
        public Select join(String table, Where on) { return join(JoinType.JOIN, table, on); }
        public Select join(Query subquery, String alias) { return join(JoinType.JOIN, subquery, alias); }
        public Select join(Query subquery, String alias, Where on) { return join(JoinType.JOIN, subquery, alias, on); }
        public Select innerJoin(String expr, Object... params) { return join(JoinType.INNER, expr, params); }
        public Select innerJoin(String table, Where on) { return join(JoinType.INNER, table, on); }
        public Select innerJoin(Query subquery, String alias) { return join(JoinType.INNER, subquery, alias); }
        public Select innerJoin(Query subquery, String alias, Where on) { return join(JoinType.INNER, subquery, alias, on); }
        public Select leftJoin(String expr, Object... params) { return join(JoinType.LEFT, expr, params); }
        public Select leftJoin(String table, Where on) { return join(JoinType.LEFT, table, on); }
        public Select leftJoin(Query subquery, String alias) { return join(JoinType.LEFT, subquery, alias); }
        public Select leftJoin(Query subquery, String alias, Where on) { return join(JoinType.LEFT, subquery, alias, on); }

        public Select where(String expr, Object... params) { return andWhere(expr, params); }
        public Select where(Where expr) { return andWhere(expr); }
        public Select andWhere(String expr, Object... params) { where.and(expr, params); return this; }
        public Select andWhere(Where expr) { where.and(expr); return this; }
        public Select orWhere(String expr, Object... params) { where.or(expr, params); return this; }
        public Select orWhere(Where expr) { where.or(expr); return this; }

        public Select groupBy(Object column) { groupBy.append(column); return this; }
        public Select groupBy(Object... columns) { groupBy.add(columns); return this; }

        public Select having(String expr, Object... params) { return andHaving(expr, params); }
        public Select having(Where expr) { return andHaving(expr); }
        public Select andHaving(String expr, Object... params) { having.and(expr, params); return this; }
        public Select andHaving(Where expr) { having.and(expr); return this; }
        public Select orHaving(String expr, Object... params) { having.or(expr, params); return this; }
        public Select orHaving(Where expr) { having.or(expr); return this; }

        public Select orderBy(Object column) { orderBy.append(column); return this; }
        public Select orderBy(Object... columns) { orderBy.add(columns); return this; }

        public Select offset(long start) { offset.append(start); return this; }
        public Select limit(long count) { limit.append(count); return this; }
        public Select limit(long offset, long count) { limit.append(offset +", "+ count); return this; }

        @Override
        public String toString() {
            if (select.isEmpty() && from.isEmpty()) where.query.prefix("");
            return new Concat(""," ").defaultValue(null)
                    .append(with)
                    .append(select)
                    .append(from)
                    .append(where)
                    .append(groupBy)
                    .append(having)
                    .append(orderBy)
                    .append(offset)
                    .append(limit)
                    .toString();
        }

        public Iterable<Object> params() {
            return copyParams();
        }

        public List<Object> copyParams() {
            List<Object> list = new ArrayList<Object>(
                    with.params.size() +
                    from.params.size() +
                    where.query.params.size() +
                    having.query.params.size());
            list.addAll(with.params);
            list.addAll(from.params);
            list.addAll(where.query.params);
            list.addAll(having.query.params);
            return list;
        }
    }

    public static final class Union implements Query {
        private final ConcatWithParams union;
        private final Concat orderBy;
        private final Concat offset;
        private final Concat limit;

        public Union() {
            union = new ConcatWithParams("", null);
            orderBy = new Concat("ORDER BY ", ", ");
            orderBy.defaultValue(null);
            offset = new Concat("OFFSET ", null);
            limit = new Concat("LIMIT ", null);
        }

        public Union(Union src) {
            union = new ConcatWithParams(src.union);
            orderBy = new Concat(src.orderBy);
            offset = new Concat(src.offset);
            limit = new Concat(src.limit);
        }

        private void unionSep(String separator, Select query) {
            String sql = query.toString();
            if (sql.length() != 0) sql = "(" + sql + ")";
            union.paramsList(query.params()).separator(separator).append(sql);
        }

        public Union union(Select expr) { unionSep(" UNION ", expr); return this; }
        public Union unionAll(Select expr) { unionSep(" UNION ALL ", expr); return this; }

        public Union union(Select... expr) { for (Select e : expr) union(e); return this; }
        public Union unionAll(Select... expr) { for (Select e : expr) unionAll(e); return this; }

        public Union orderBy(Object column) { orderBy.append(column); return this; }
        public Union orderBy(Object... columns) { orderBy.add(columns); return this; }

        public Union offset(long start) { offset.append(start); return this; }
        public Union limit(long count) { limit.append(count); return this; }

        @Override
        public String toString() {
            return new Concat("", " ").defaultValue(null)
                    .append(union)
                    .append(orderBy)
                    .append(offset)
                    .append(limit)
                    .toString();
        }

        public Iterable<Object> params() {
            return union.params;
        }

        public List<Object> copyParams() {
            return new ArrayList<Object>(union.params);
        }
    }

    public static final class Insert implements UpdateQuery {
        private final Concat into;
        private final Concat columns;
        private final ConcatWithParams values;

        public Insert() {
            into = new Concat("INSERT INTO ", null);
            into.defaultValue(null);
            columns = new Concat("(", ", ", ")");
            values = new ConcatWithParams("VALUES (", ", ", ")");
        }

        public Insert(Insert src) {
            into = new Concat(src.into);
            columns = new Concat(src.columns);
            values = new ConcatWithParams(src.values);
        }

        public Insert replace() { into.prefix = "REPLACE INTO "; return this; }

        public Insert into(String table) { into.append(table); return this; }
        public Insert into(Class<?> clazz) { into.append(clazz.getSimpleName()); return this; }

        public Insert column(Object column) { columns.append(column); return this; }
        public Insert columns(Object... columns) { this.columns.add(columns); return this; }

        public Insert defaultValues() { values.prefix("").suffix("").separator(null).append("DEFAULT VALUES"); return this; }
        public Insert select(Select values) { this.values.paramsList(values.params()).prefix("").suffix("").separator(null).append(values.toString()); return this; }
        public Insert valueExpr(String value, Object... params) { values.params(params).append(value); return this; }
        public Insert value(Object value) { return valueExpr("?", value); }
        public Insert values(Object... values) { for (Object value : values) value(value); return this; }

        public Insert set(String column, Object value) { return column(column).value(value); }
        public Insert setExpr(String column, String value, Object... params) { return column(column).valueExpr(value, params); }

        public Insert object(Object obj, String... exceptFields) throws SqlException {
            Set<String> excepts = new HashSet<String>(exceptFields.length);
            for (String field : exceptFields) excepts.add(field);

            Class<?> objClass = obj.getClass();
            Field[] objFields = objClass.getFields();
            try {
                for (Field objField : objFields) {
                    String fieldName = objField.getName();
                    if (excepts.contains(fieldName)) continue;

                    if ((objField.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0) {
                        set(fieldName, objField.get(obj));
                    }
                }
            } catch (Exception e) {
                throw new SqlException(e);
            }
            return this;
        }

        public Insert objectField(Object obj, String... fields) throws SqlException {
            Class<?> objClass = obj.getClass();
            try {
                for (String field : fields) {
                    Field objField = objClass.getField(field);
                    if ((objField.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) != 0)
                        throw new IllegalArgumentException(field);
                    set(field, objField.get(obj));
                }
            } catch (Exception e) {
                throw new SqlException(e);
            }
            return this;
        }

        public Insert map(Map<String,?> map) {
            for (Map.Entry<String,?> entry : map.entrySet()) set(entry.getKey(), entry.getValue());
            return this;
        }

        public Insert map(Map<String,?> map, String... fields) {
            for (String field : fields) set(field, map.get(field));
            return this;
        }

        public Insert setColumns(String... columns) {
            for (String column : columns) setExpr(column, "?");
            return this;
        }

        public Insert setColumns(Iterable<String> columns) {
            for (String column : columns) setExpr(column, "?");
            return this;
        }

        @Override
        public String toString() {
            return new Concat(""," ").defaultValue(null)
                    .append(into)
                    .append(columns)
                    .append(values)
                    .toString();
        }

        public Iterable<Object> params() {
            return values.params;
        }

        public List<Object> copyParams() {
            return new ArrayList<Object>(values.params);
        }
    }

    public static final class Update implements UpdateQuery {
        private final Concat update;
        private final ConcatWithParams set;
        public final Where where;
        private final Concat orderBy;
        private final Concat limit;

        public Update() {
            update = new Concat("UPDATE ", ", ");
            update.defaultValue(null);
            set = new ConcatWithParams("SET ", ", ");
            where = new Where();
            where.query.prefix("WHERE ").suffix("");
            orderBy = new Concat("ORDER BY ", ", ");
            orderBy.defaultValue(null);
            limit = new Concat("LIMIT ", null);
        }

        public Update(Update src) {
            update = new Concat(src.update);
            set = new ConcatWithParams(src.set);
            where = new Where(src.where);
            orderBy = new Concat(src.orderBy);
            limit = new Concat(src.limit);
        }

        public Update update(String table) { update.append(table); return this; }
        public Update update(String... tables) { update.add((Object[])tables); return this; }
        public Update update(Class<?> clazz) { update.append(clazz.getSimpleName()); return this; }
        public Update update(Class<?>... classes) { for (Class<?> clazz : classes) update.append(clazz.getSimpleName()); return this; }

        public Update set(String name, Object value) { set.param(value).append(name+"=?"); return this; }
        public Update setExpr(String name, String expr, Object... params) { set.params(params).append(name+"="+expr); return this; }

        public Update object(Object obj, String... exceptFields) throws SqlException {
            Set<String> excepts = new HashSet<String>(exceptFields.length);
            for (String field : exceptFields) excepts.add(field);

            Class<?> objClass = obj.getClass();
            Field[] objFields = objClass.getFields();
            try {
                for (Field objField : objFields) {
                    String fieldName = objField.getName();
                    if (excepts.contains(fieldName)) continue;

                    if ((objField.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) == 0) {
                        set(objField.getName(), objField.get(obj));
                    }
                }
            } catch (Exception e) {
                throw new SqlException(e);
            }
            return this;
        }

        public Update objectField(Object obj, String... fields) throws SqlException {
            Class<?> objClass = obj.getClass();
            try {
                for (String field : fields) {
                    Field objField = objClass.getField(field);
                    if ((objField.getModifiers() & (Modifier.STATIC | Modifier.TRANSIENT)) != 0)
                        throw new IllegalArgumentException(field);
                    set(field, objField.get(obj));
                }
            } catch (Exception e) {
                throw new SqlException(e);
            }
            return this;
        }

        public Update map(Map<String,?> map) {
            for (Map.Entry<String,?> entry : map.entrySet()) set(entry.getKey(), entry.getValue());
            return this;
        }

        public Update map(Map<String,?> map, String... fields) {
            for (String field : fields) set(field, map.get(field));
            return this;
        }

        public Update setColumns(String... columns) {
            for (String column : columns) setExpr(column, "?");
            return this;
        }

        public Update setColumns(Iterable<String> columns) {
            for (String column : columns) setExpr(column, "?");
            return this;
        }

        public Update where(String expr, Object... params) { return andWhere(expr, params); }
        public Update where(Where expr) { return andWhere(expr); }
        public Update andWhere(String expr, Object... params) { where.and(expr, params); return this; }
        public Update andWhere(Where expr) { where.and(expr); return this; }
        public Update orWhere(String expr, Object... params) { where.or(expr, params); return this; }
        public Update orWhere(Where expr) { where.or(expr); return this; }

        public Update orderBy(Object column) { orderBy.append(column); return this; }
        public Update orderBy(Object... columns) { orderBy.add(columns); return this; }

        public Update limit(long count) { limit.append(count); return this; }

        @Override
        public String toString() {
            return new Concat(""," ").defaultValue(null)
                    .append(update)
                    .append(set)
                    .append(where)
                    .append(orderBy)
                    .append(limit)
                    .toString();
        }

        public Iterable<Object> params() {
            return copyParams();
        }

        public List<Object> copyParams() {
            List<Object> list = new ArrayList<Object>(
                    set.params.size() +
                    where.query.params.size());
            list.addAll(set.params);
            list.addAll(where.query.params);
            return list;
        }
    }

    public static final class Delete implements UpdateQuery {
        private final String from;
        private final Concat using;
        public final Where where;

        public Delete(String from) {
            this(from, false);
        }

        public Delete(Class<?> from) {
            this(from.getSimpleName(), false);
        }

        public Delete(Class<?> from, boolean only) {
            this(from.getSimpleName(), only);
        }

        public Delete(String from, boolean only) {
            this.from = "DELETE FROM " + (only?"ONLY ":"") + from;
            using = new Concat("USING ", ", ");
            using.defaultValue(null);
            where = new Where();
            where.query.prefix("WHERE ").suffix("");
        }

        public Delete(Delete src) {
            from = new String(src.from);
            using = new Concat(src.using);
            where = new Where(src.where);
        }

        public Delete using(String table) { using.append(table); return this; }
        public Delete using(String... tables) { using.add((Object[])tables); return this; }
        public Delete using(Class<?> clazz) { using.append(clazz.getSimpleName()); return this; }
        public Delete using(Class<?>... classes) { for (Class<?> clazz : classes) using.append(clazz.getSimpleName()); return this; }

        public Delete where(String expr, Object... params) { return andWhere(expr, params); }
        public Delete where(Where expr) { return andWhere(expr); }
        public Delete andWhere(String expr, Object... params) { where.and(expr, params); return this; }
        public Delete andWhere(Where expr) { where.and(expr); return this; }
        public Delete orWhere(String expr, Object... params) { where.or(expr, params); return this; }
        public Delete orWhere(Where expr) { where.or(expr); return this; }

        @Override
        public String toString() {
            return new Concat(""," ").defaultValue(null)
                    .append(from)
                    .append(using)
                    .append(where)
                    .toString();
        }

        public Iterable<Object> params() {
            return where.params();
        }

        public List<Object> copyParams() {
            return where.copyParams();
        }
    }

    public static Select with(String table, Query subquery) { return new Select().with(table, subquery); }
    public static Select with(String table, UpdateQuery subquery) { return new Select().with(table, subquery); }
    public static Select select() { return new Select(); }
    public static Select select(Object column) { return new Select().select(column); }
    public static Select select(Object... columns) { return new Select().select(columns); }
    public static Select selectDistinct(Object column) { return new Select().distinct().select(column); }
    public static Select selectDistinct(Object... columns) { return new Select().distinct().select(columns); }
    public static Select from(String table) { return new Select().from(table); }
    public static Select from(String... tables) { return new Select().from(tables); }
    public static Select from(Class<?> clazz) { return new Select().from(clazz); }
    public static Select from(Class<?>... classes) { return new Select().from(classes); }
    public static Select from(Select subquery, String name) { return new Select().from(subquery, name); }

    public static Union union() { return new Union(); }
    public static Union union(Select expr) { return new Union().union(expr); }
    public static Union union(Select... expr) { return new Union().union(expr); }
    public static Union unionAll(Select expr) { return new Union().unionAll(expr); }
    public static Union unionAll(Select... expr) { return new Union().unionAll(expr); }

    public static Insert insert() { return new Insert(); }
    public static Insert insert(String table) { return new Insert().into(table); }
    public static Insert insert(Class<?> clazz) { return new Insert().into(clazz); }
    public static Insert replace() { return new Insert().replace(); }
    public static Insert replace(String table) { return new Insert().replace().into(table); }
    public static Insert replace(Class<?> clazz) { return new Insert().replace().into(clazz); }

    public static Update update() { return new Update(); }
    public static Update update(String table) { return new Update().update(table); }
    public static Update update(String... tables) { return new Update().update(tables); }
    public static Update update(Class<?> clazz) { return new Update().update(clazz); }
    public static Update update(Class<?>... classes) { return new Update().update(classes); }

    public static Delete delete(String table) { return new Delete(table); }
    public static Delete delete(Class<?> clazz) { return new Delete(clazz); }
    public static Delete deleteOnly(String table) { return new Delete(table, true); }
    public static Delete deleteOnly(Class<?> clazz) { return new Delete(clazz, true); }

    public static Where where() { return new Where(); }
    public static Where where(String expr, Object... params) { return new Where().and(expr, params); }
    public static Where where(Where where) { return new Where().and(where); }

    public static Select clone(Select src) { return new Select(src); }
    public static Union  clone(Union  src) { return new Union (src); }
    public static Insert clone(Insert src) { return new Insert(src); }
    public static Update clone(Update src) { return new Update(src); }
    public static Delete clone(Delete src) { return new Delete(src); }
    public static Where  clone(Where  src) { return new Where (src); }
    public static FinalQuery clone(Query src) { return new FinalQuery(src); }
    public static FinalUpdateQuery clone(UpdateQuery src) { return new FinalUpdateQuery(src); }

    public static FinalQuery finalQuery(Query src) { return new FinalQuery(src); }
    public static FinalQuery finalQuery(Query src, Object param) { return new FinalQuery(src, param); }
    public static FinalQuery finalQuery(Query src, Object... params) { return new FinalQuery(src, params); }
    public static FinalQuery finalQuery(Query src, Iterable<?> params) { return new FinalQuery(src, params); }
    public static FinalUpdateQuery finalQuery(UpdateQuery src) { return new FinalUpdateQuery(src); }
    public static FinalUpdateQuery finalQuery(UpdateQuery src, Object param) { return new FinalUpdateQuery(src, param); }
    public static FinalUpdateQuery finalQuery(UpdateQuery src, Object... params) { return new FinalUpdateQuery(src, params); }
    public static FinalUpdateQuery finalQuery(UpdateQuery src, Iterable<?> params) { return new FinalUpdateQuery(src, params); }

    public static String resolve(InlineableQuery query) { return resolve(query.toString(), query.params()); }
    public static String resolve(UpdateQuery query) { return resolve(query.toString(), query.params()); }
    private static String resolve(String query, Iterable<Object> params) { return resolve(query, params, 0, -1); }
    private static String resolve(String query, Iterable<Object> params, int start, int end) {
        Iterator<Object> paramsIt = params.iterator();
        int index = 0;
        char quote = 0;
        boolean backslash = false;
        for (int ndx = 0; ndx < query.length(); ndx++) {
            if (backslash) {
                backslash = false;
                continue;
            }
            char ch = query.charAt(ndx);
            switch (ch) {
            case '\\':
                backslash = true;
                break;
            case '\'':
            case '"':
                quote = quote == 0 ? ch : quote == ch ? 0 : quote;
                break;
            case '?':
                if (quote == 0) {
                    if (index >= start && (end == -1 || index < end)) {
                        String param = inlineParam(paramsIt.next());
                        query = query.substring(0, ndx) + param + query.substring(ndx+1);
                        ndx += param.length() - 1;
                    }
                    index++;
                }
                break;
            }
        }
        if (paramsIt.hasNext()) throw new IllegalArgumentException("Too many parameters");
        return query;
    }

    public static FinalQuery expands(InlineableQuery query) { return expands(query.toString(), query.params()); }
    public static FinalQuery expands(UpdateQuery query) { return expands(query.toString(), query.params()); }
    private static FinalQuery expands(String query, Iterable<Object> params) { return expands(query, params, 0, -1); }
    private static FinalQuery expands(String query, Iterable<Object> params, int start, int end) {
        List<Object> finalParams = new ArrayList<Object>();
        Iterator<Object> paramsIt = params.iterator();
        int index = 0;
        char quote = 0;
        boolean backslash = false;
        for (int ndx = 0; ndx < query.length(); ndx++) {
            if (backslash) {
                backslash = false;
                continue;
            }
            char ch = query.charAt(ndx);
            switch (ch) {
            case '\\':
                backslash = true;
                break;
            case '\'':
            case '"':
                quote = quote == 0 ? ch : quote == ch ? 0 : quote;
                break;
            case '?':
                if (quote == 0) {
                    final Object param;
                    if (index >= start && (end == -1 || index < end)) {
                        param = paramsIt.next();
                        if (param instanceof InlineableQuery) {
                            InlineableQuery inlineParam = (InlineableQuery) param;
                            FinalQuery finalParam = expands(inlineParam.toString(), inlineParam.params(), 0, -1);
                            query = query.substring(0, ndx) +"("+ finalParam.query +")"+ query.substring(ndx+1);
                            ndx += finalParam.query.length() + 2 - 1;
                            finalParams.addAll(finalParam.params);
                        } else {
                            finalParams.add(param);
                        }
                    }
                    index++;
                }
                break;
            }
        }
        if (paramsIt.hasNext()) throw new IllegalArgumentException("Too many parameters");
        FinalQuery finalQuery = new FinalQuery(query);
        finalQuery.params.addAll(finalParams);
        return finalQuery;
    }

    public static String table(String name) {
        return name;
    }

    public static String table(Class<?> clazz) {
        return clazz.getSimpleName();
    }

    public static String table(String name, String alias) {
        String table = name;
        if (alias != null) table += " AS "+ alias;
        return table;
    }

    public static String table(Class<?> clazz, String alias) {
        return table(clazz.getSimpleName(), alias);
    }

    public static String quote(String str) {
        return "'" + str.replace("\\", "\\\\").replace("'","\\'") + "'";
    }

    public static String likeEscape(String str) {
        return str.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    public static String inline(Object param) {
        return inlineParam(param);
    }

    public static String inlineParam(Object param) {
        final String str;
        if (param == null) str = "NULL";
        else if (param instanceof String) str = quote(param.toString());
        else if (param instanceof Iterable<?>) {
            Concat list = new Concat("(", ", ", ")");
            for (Object p : (Iterable<?>)param) list.append(inlineParam(p));
            str = list.toString();
        } else if (param.getClass().isArray()) {
            Concat list = new Concat("(", ", ", ")");
            int len = Array.getLength(param);
            for (int i = 0; i < len; i++) list.append(inlineParam(Array.get(param, i)));
            str = list.toString();
        } else if (param instanceof Enum<?>) {
            str = quote(param.toString());
        } else if (param instanceof Date) {
            str = inlineParam(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date)param));
        } else if (param instanceof Calendar) {
            str = inlineParam(((Calendar)param).getTime());
        } else if (param instanceof InlineableQuery) {
            str = "(" + Sql.resolve((InlineableQuery)param) + ")";
        } else str = param.toString();
        return str;
    }

    public static String whereIn(String column, Object param) {
        String value = inlineParam(param);
        if (value.length() == 0) return value;

        String operator;
        if (param instanceof Object[]) {
            operator = " in ";
        } else if (param instanceof Iterable<?>) {
            operator = " in ";
        } else {
            operator = "=";
        }

        return column + operator + value;
    }

}
