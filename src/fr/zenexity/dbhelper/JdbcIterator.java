package fr.zenexity.dbhelper;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Iterate over a JDBC ResultSet
 * @author yma
 */
public class JdbcIterator<T> implements Iterator<T>, Iterable<T>, Closeable {

    protected final JdbcResult.Factory<T> factory;
    protected ResultSet result;
    protected T next;

    public JdbcIterator(ResultSet result, JdbcResult.Factory<T> resultFactory) throws SQLException {
        this.factory = resultFactory;
        this.result = result;
        next = null;

        if (this.result != null) this.factory.init(this.result);
    }

    public void close() {
        if (result != null) {
            try {
                next = null;
                result.close();
                result = null;
            } catch (SQLException ex) {
                result = null;
                throw new RuntimeException(ex);
            }
        }
    }

    public static void close(Iterator<?> iterator) {
        if (iterator instanceof JdbcIterator<?>) ((JdbcIterator<?>)iterator).close();
    }


    protected void load() {
        if (next == null && result != null) try {
            if (result.next()) next = factory.create(result);
            else close();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean hasNext() {
        load();
        return next != null;
    }

    public T next() {
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

    public List<T> list() {
        List<T> list = new ArrayList<T>();
        for (T row : this) list.add(row);
        return list;
    }

    public static class Window<T> extends JdbcIterator<T> {

        private int size;

        public Window(ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws SQLException {
            super(result, resultFactory);
            this.size = size;
            seek(offset);
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
        protected void load() {
            if (next == null) {
                if (size-- > 0) super.load();
                else close();
            }
        }

    }

}
