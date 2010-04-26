package fr.zenexity.dbhelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Iterate over a JDBC ResultSet
 * @author yma
 */
public class JdbcIterator<T> implements Iterator<T>, Iterable<T> {

    protected final JdbcResult.Factory<T> factory;
    protected Statement statement;
    protected ResultSet result;
    protected T next;

    /**
     * if statement is not null then it will be closed with the ResultSet
     */
    public JdbcIterator(Statement statement, ResultSet result, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        this.factory = resultFactory;
        this.statement = statement;
        this.result = result;
        next = null;

        if (this.result != null) {
            try {
                this.factory.init(this.result);
            } catch (SQLException e) {
                throw new JdbcIteratorException(e);
            } catch (JdbcResultException e) {
                throw new JdbcIteratorException(e);
            }
        }
    }

    public void close() throws JdbcIteratorException {
        try {
            if (result != null) {
                next = null;
                result.close();
                result = null;
            }
            if (statement != null) {
                statement.close();
                statement = null;
            }
        } catch (SQLException ex) {
            throw new JdbcIteratorException(ex);
        }
    }

    public static void close(Iterator<?> iterator) throws JdbcIteratorException {
        if (iterator instanceof JdbcIterator<?>) ((JdbcIterator<?>)iterator).close();
    }

    protected void load() throws JdbcIteratorException {
        if (next == null && result != null) try {
            if (result.next()) next = factory.create(result);
            else close();
        } catch (SQLException ex) {
            throw new JdbcIteratorException(ex);
        } catch (JdbcResultException ex) {
            throw new JdbcIteratorException(ex);
        }
    }

    public boolean hasNext() throws JdbcIteratorException {
        load();
        return next != null;
    }

    public T next() throws JdbcIteratorException {
        load();
        T e = next;
        next = null;
        return e;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public Iterator<T> iterator() {
        return this;
    }

    public List<T> list() throws JdbcIteratorException {
        List<T> list = new ArrayList<T>();
        for (T row : this) list.add(row);
        return list;
    }

    public T first() throws JdbcIteratorException {
        T e = next();
        close();
        return e;
    }

    public static class Window<T> extends JdbcIterator<T> {
        private int size;

        public Window(Statement statement, ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
            super(statement, result, resultFactory);
            this.size = size;
            try {
                seek(offset);
            } catch (SQLException e) {
                throw new JdbcIteratorException(e);
            }
        }

        private void seek(int offset) throws SQLException {
            if (result != null) {
                if (offset < 0) {
                    size += offset;
                    offset = 0;
                }
                if (size > 0) {
                    if (offset == 0) result.beforeFirst();
                    else result.absolute(offset);
                } else close();
            }
        }

        @Override
        protected void load() throws JdbcIteratorException {
            if (next == null) {
                if (size-- > 0) super.load();
                else close();
            }
        }
    }

}
