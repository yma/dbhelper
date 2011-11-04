package fr.zenexity.dbhelper;

import java.util.LinkedList;
import java.util.List;

public class SqlParser {

    public enum QueryKeyword {
        Select("SELECT"),
        From("FROM"),
        Join("JOIN"),
        Where("WHERE"),
        GroupBy("GROUP BY"),
        Having("HAVING"),
        OrderBy("ORDER BY"),
        Offset("OFFSET"),
        Limit("LIMIT");

        public final String keyword;

        private QueryKeyword(String _keyword) {
            keyword = _keyword;
        }

        public int apply(Sql.Concat concat, List<QueryChunk> chunks, int index) {
            if (index >= chunks.size()) return index;
            QueryChunk chunk = chunks.get(index);
            if (chunk.keyword != this) return index;
            String text = chunk.text();
            int ndx = keyword.length() + 1;
            concat.prefix(text.substring(0, ndx));
            concat.append(text.substring(ndx).trim());
            return index + 1;
        }

        public static QueryKeyword matching(String query, int offset) {
            for (QueryKeyword qk : QueryKeyword.values()) {
                if (query.startsWith(qk.keyword, offset)) {
                    int ndx = offset + qk.keyword.length();
                    if (ndx >= query.length() || isWhitespace(query.charAt(ndx))) {
                        return qk;
                    }
                }
            }
            return null;
        }
    }

    public static boolean isWhitespace(char ch) {
        switch (ch) {
        case ' ':
        case '\t':
        case '\n':
        case '\r':
            return true;
        }
        return false;
    }

    private static int quoteLength(char quote, String text, int start) {
        for (int ndx = start; ndx < text.length(); ndx++) {
            char ch = text.charAt(ndx);
            if (ch == '\\') ndx++;
            else if (ch == quote) return ndx + 1;
        }
        throw new IllegalArgumentException("Not closed : "+ text);
    }

    private static int groupLength(String text, int start) {
        int depth = 1;
        for (int ndx = start; ndx < text.length(); ndx++) {
            char ch = text.charAt(ndx);
            switch (ch) {
            case '(':
                depth++;
                break;
            case ')':
                if (--depth != 0) break;
                return ndx + 1;
            }
        }
        throw new IllegalArgumentException("Not closed : "+ text);
    }

    public class QueryChunk {
        public final QueryKeyword keyword;
        public final int start;
        public final int end;

        public QueryChunk(QueryKeyword _keyword, int _start, int _end) {
            keyword = _keyword;
            start = _start;
            end = _end;
        }

        public String text() {
            return query.substring(start, end);
        }

        public boolean isEmpty() {
            return start == end;
        }

        @Override
        public String toString() {
            return String.format("%s(%d,%d)", keyword, start, end);
        }
    }

    public final String query;
    private final List<QueryChunk> chunks;

    public SqlParser(String _query) {
        query = _query;
        chunks = explode(query);
    }

    private void addIfNotEmpty(List<QueryChunk> chunks, QueryChunk chunk) {
        if (!chunk.isEmpty()) chunks.add(chunk);
    }

    private List<QueryChunk> explode(String query) {
        String upperQuery = query.toUpperCase();
        List<QueryChunk> chunks = new LinkedList<QueryChunk>();
        QueryKeyword qk = null;
        int offset = 0;
        boolean isKeyword = true;
        for (int ndx = offset; ndx < query.length();) {
            if (isKeyword) {
                QueryKeyword nextQk = QueryKeyword.matching(upperQuery, ndx);
                if (nextQk != null) {
                    addIfNotEmpty(chunks, new QueryChunk(qk, offset, ndx));

                    qk = nextQk;
                    offset = ndx;
                    ndx += qk.keyword.length();
                    continue;
                }
            }
            char ch = query.charAt(ndx);
            switch (ch) {
            case '\'':
            case '"':
            case '`':
                ndx = quoteLength(ch, query, ndx);
                break;
            case '(':
                ndx = groupLength(query, ndx);
                break;
            default:
                if (isWhitespace(ch)) isKeyword = true;
                ndx++;
            }
        }
        addIfNotEmpty(chunks, new QueryChunk(qk, offset, query.length()));
        return chunks;
    }

    @SuppressWarnings("unchecked")
    public <T extends Sql.Query> T toQuery(Class<T> clazz) {
        if (clazz == Sql.Select.class) return (T) toSelect();
        throw new IllegalArgumentException("Unknown query type "+ clazz.getName());
    }

    public Sql.Select toSelect() {
        Sql.Select sql = new Sql.Select();
        int index = 0;
        index = QueryKeyword.Select.apply(sql.select, chunks, index);
        index = QueryKeyword.From.apply(sql.from, chunks, index);
        index = QueryKeyword.Join.apply(sql.join, chunks, index);
        index = QueryKeyword.Where.apply(sql.where.query, chunks, index);
        index = QueryKeyword.GroupBy.apply(sql.groupBy, chunks, index);
        index = QueryKeyword.Having.apply(sql.having.query, chunks, index);
        index = QueryKeyword.OrderBy.apply(sql.orderBy, chunks, index);
        index = QueryKeyword.Offset.apply(sql.offset, chunks, index);
        index = QueryKeyword.Limit.apply(sql.limit, chunks, index);
        if (index != chunks.size()) {
            System.err.println(chunks.toString());
            throw new IllegalArgumentException(
                    "Not valid Sql.Select query ("+ index +"/"+ chunks.size() +") : "+ query);
        }
        return sql;
    }

}
