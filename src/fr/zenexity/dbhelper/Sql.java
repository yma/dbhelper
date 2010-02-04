package fr.zenexity.dbhelper;

import java.util.ArrayList;
import java.util.List;

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

        public String value() {
            return value;
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

        public ConcatWithParams params(Object... objs) {
            for (Object obj : objs) params.add(obj);
            return this;
        }

        public ConcatWithParams paramsList(List<Object> objs) {
            for (Object obj : objs) params.add(obj);
            return this;
        }
    }

    public static class Where {
        private final ConcatWithParams query;

        public Where() {
            query = new ConcatWithParams("(", null, ")");
            query.defaultValue(null);
        }

        public Where(Where src) {
            query = new ConcatWithParams(src.query);
        }

        public Where and(String expr, Object... params) { query.params(params).separator(" AND ").append(expr); return this; }
        public Where and(Where where) { return and(where.toString(), where.query.params); }
        public Where or(String expr, Object... params) { query.params(params).separator(" OR ").append(expr); return this; }
        public Where or(Where where) { return or(where.toString(), where.query.params); }

        @Override
        public String toString() {
            return query.toString();
        }

        public String value() {
            return query.value();
        }

        public List<Object> params() {
            return query.params;
        }
    }

    public interface Query {
        @Override String toString();
        List<Object> params();
    }

    public interface UpdateQuery {
        @Override String toString();
        List<Object> params();
    }

    public static final class Select implements Query {
        private final Concat select;
        private final Concat from;
        private final ConcatWithParams join;
        public final Where where;
        private final Concat groupBy;
        private final ConcatWithParams having;
        private final Concat orderBy;
        private final Concat offset;
        private final Concat limit;

        public Select() {
            select = new Concat("SELECT ", ", ");
            select.defaultValue(null);
            from = new Concat("FROM ", ", ");
            from.defaultValue(null);
            join = new ConcatWithParams("", null);
            join.defaultValue(null);
            where = new Where();
            where.query.prefix("WHERE ").suffix("");
            groupBy = new Concat("GROUP BY ", ", ");
            groupBy.defaultValue(null);
            having = new ConcatWithParams("HAVING ", ", ");
            having.defaultValue(null);
            orderBy = new Concat("ORDER BY ", ", ");
            orderBy.defaultValue(null);
            offset = new Concat("OFFSET ", null);
            limit = new Concat("LIMIT ", null);
        }

        public Select(Select src) {
            select = new Concat(src.select);
            from = new Concat(src.from);
            join = new ConcatWithParams(src.join);
            where = new Where(src.where);
            groupBy = new Concat(src.groupBy);
            having = new ConcatWithParams(src.having);
            orderBy = new Concat(src.orderBy);
            offset = new Concat(src.offset);
            limit = new Concat(src.limit);
        }

        public Select select(Object expression) { select.append(expression); return this; }
        public Select select(Object... expressions) { select.add(expressions); return this; }
        public Select selectAll() { select.append("*"); return this; }

        public Select from(String table) { from.append(table); return this; }
        public Select from(String... tables) { from.add((Object[])tables); return this; }
        public Select from(Class<?> clazz) { from.append(clazz.getSimpleName()); return this; }
        public Select from(Class<?>... classes) { for (Class<?> clazz : classes) from.append(clazz.getSimpleName()); return this; }

        public Select join(String expr, Object... params) { join.params(params).localPrefix("JOIN ").separator(" JOIN ").append(expr); return this; }
        public Select join(String table, Where on) { return join(table +" ON "+ on.value(), on.params()); }
        public Select innerJoin(String expr, Object... params) { join.params(params).localPrefix("INNER JOIN ").separator(" INNER JOIN ").append(expr); return this; }
        public Select innerJoin(String table, Where on) { return innerJoin(table +" ON "+ on.value(), on.params()); }
        public Select leftJoin(String expr, Object... params) { join.params(params).localPrefix("LEFT JOIN ").separator(" LEFT JOIN ").append(expr); return this; }
        public Select leftJoin(String table, Where on) { return leftJoin(table +" ON "+ on.value(), on.params()); }

        public Select where(String expr, Object... params) { return andWhere(expr, params); }
        public Select where(Where expr) { return andWhere(expr); }
        public Select andWhere(String expr, Object... params) { where.and(expr, params); return this; }
        public Select andWhere(Where expr) { where.and(expr); return this; }
        public Select orWhere(String expr, Object... params) { where.or(expr, params); return this; }
        public Select orWhere(Where expr) { where.or(expr); return this; }

        public Select groupBy(Object column) { groupBy.append(column); return this; }
        public Select groupBy(Object... columns) { groupBy.add(columns); return this; }

        public Select having(String expr, Object... params) { having.params(params).append(expr); return this; }
        public Select having(Where expr) { having.paramsList(expr.params()).append(expr.toString()); return this; }

        public Select orderBy(Object column) { orderBy.append(column); return this; }
        public Select orderBy(Object... columns) { orderBy.add(columns); return this; }

        public Select offset(long start) { offset.append(start); return this; }
        public Select limit(long count) { limit.append(count); return this; }

        @Override
        public String toString() {
            if (select.isEmpty() && from.isEmpty() && join.isEmpty()) where.query.prefix("");
            return new Concat(""," ").defaultValue(null)
                    .append(select)
                    .append(from)
                    .append(join)
                    .append(where)
                    .append(groupBy)
                    .append(having)
                    .append(orderBy)
                    .append(offset)
                    .append(limit)
                    .toString();
        }

        public List<Object> params() {
            List<Object> list = new ArrayList<Object>();
            list.addAll(join.params);
            list.addAll(where.params());
            list.addAll(having.params);
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

        public List<Object> params() {
            return union.params;
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

        public Insert into(String table) { into.append(table); return this; }
        public Insert into(Class<?> clazz) { into.append(clazz.getSimpleName()); return this; }

        public Insert column(Object column) { columns.append(column); return this; }
        public Insert columns(Object... columns) { this.columns.add(columns); return this; }

        public Insert defaultValues() { values.prefix("").suffix("").append("DEFAULT VALUES"); return this; }
        public Insert value(String value, Object... params) { values.params(params).append(value); return this; }
        public Insert select(Select values) { this.values.paramsList(values.params()).prefix("").suffix("").separator(null).append(values.toString()); return this; }

        @Override
        public String toString() {
            return new Concat(""," ").defaultValue(null)
                    .append(into)
                    .append(columns)
                    .append(values)
                    .toString();
        }

        public List<Object> params() {
            return values.params;
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

        public Update set(String expr, Object... params) { set.params(params).append(expr); return this; }

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

        public List<Object> params() {
            List<Object> list = new ArrayList<Object>();
            list.addAll(set.params);
            list.addAll(where.params());
            return list;
        }
    }

    public static Select select() { return new Select(); }
    public static Select selectAll() { return new Select().selectAll(); }
    public static Select select(Object column) { return new Select().select(column); }
    public static Select select(Object... columns) { return new Select().select(columns); }
    public static Select from(String table) { return new Select().from(table); }
    public static Select from(String... tables) { return new Select().from(tables); }
    public static Select from(Class<?> clazz) { return new Select().from(clazz); }
    public static Select from(Class<?>... classes) { return new Select().from(classes); }

    public static Union union() { return new Union(); }
    public static Union union(Select expr) { return new Union().union(expr); }
    public static Union unionAll(Select expr) { return new Union().unionAll(expr); }

    public static Insert insert() { return new Insert(); }

    public static Update update() { return new Update(); }
    public static Update update(String table) { return new Update().update(table); }
    public static Update update(String... tables) { return new Update().update(tables); }
    public static Update update(Class<?> clazz) { return new Update().update(clazz); }
    public static Update update(Class<?>... classes) { return new Update().update(classes); }

    public static Where where(String expr, Object... params) { return new Where().and(expr, params); }
    public static Where where(Where where) { return new Where().and(where); }

    public static Select clone(Select src) { return new Select(src); }
    public static Union  clone(Union  src) { return new Union (src); }
    public static Insert clone(Insert src) { return new Insert(src); }
    public static Where  clone(Where  src) { return new Where (src); }

    public static String quote(String str) {
        return "'" + str.replace("'","\\'") + "'";
    }

    public static String inlineParam(Object param) {
        if (param == null) return "NULL";

        String str;
        if (param instanceof String) str = quote(param.toString());
        else if (param instanceof Iterable<?>) {
            Concat list = new Concat("(", ", ", ")");
            for (Object p : (Iterable<?>)param) list.append(inlineParam(p));
            str = list.toString();
        } else if (param instanceof Object[]) {
            Concat list = new Concat("(", ", ", ")");
            for (Object p : (Object[])param) list.append(inlineParam(p));
            str = list.toString();
        } else if (param instanceof Enum<?>) {
            str = quote(param.toString());
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
