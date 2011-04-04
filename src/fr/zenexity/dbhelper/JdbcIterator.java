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
    protected boolean loadNext;
    private boolean keepOpen;
    private int limit;

    /**
     * if statement is not null then it will be closed with the ResultSet
     */
    public JdbcIterator(Statement statement, ResultSet result, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
        this.factory = resultFactory;
        this.statement = statement;
        this.result = result;
        next = null;
        loadNext = true;
        keepOpen = false;
        limit = -1;

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

    /**
     * Don't close resources automatically at the end of iteration or after
     * calling first().
     */
    public JdbcIterator<T> keepOpen() throws JdbcIteratorException {
        keepOpen = true;
        return this;
    }

    public ResultSet resultSet() throws JdbcIteratorException {
        return result;
    }

    public void close() throws JdbcIteratorException {
        try {
            if (result != null) {
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

    /**
     * Use negative value to seek from the last row.
     */
    public JdbcIterator<T> seek(int row) throws JdbcIteratorException {
        if (result != null) {
            try {
                if (row == 0) result.beforeFirst();
                else result.absolute(row);
            } catch (SQLException e) {
                throw new JdbcIteratorException(e);
            }
            loadNext = true;
        }
        return this;
    }

    public JdbcIterator<T> offset(int row) throws JdbcIteratorException {
        if (result != null) {
            try {
                result.relative(row);
            } catch (SQLException e) {
                throw new JdbcIteratorException(e);
            }
            loadNext = true;
        }
        return this;
    }

    public int position() throws JdbcIteratorException {
        if (result != null) {
            try {
                return result.getRow();
            } catch (SQLException e) {
                throw new JdbcIteratorException(e);
            }
        }
        return -1;
    }

    /**
     * Limit the number of iteration.
     * Use negative value to disable the limitation.
     */
    public JdbcIterator<T> limit(int rows) throws JdbcIteratorException {
        limit = rows;
        return this;
    }

    protected void load() throws JdbcIteratorException {
        if (loadNext && result != null) try {
            if (limit != 0 && result.next()) {
                if (limit > 0) limit--;
                next = factory.create(result);
                loadNext = false;
            } else if (!keepOpen) close();
        } catch (SQLException ex) {
            throw new JdbcIteratorException(ex);
        } catch (JdbcResultException ex) {
            throw new JdbcIteratorException(ex);
        }
    }

    public boolean hasNext() throws JdbcIteratorException {
        load();
        return ! loadNext;
    }

    public T next() throws JdbcIteratorException {
        load();
        loadNext = true;
        return next;
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
        if (!keepOpen) close();
        return e;
    }

    public static class Window<T> extends JdbcIterator<T> {
        public Window(Statement statement, ResultSet result, int offset, int size, JdbcResult.Factory<T> resultFactory) throws JdbcIteratorException {
            super(statement, result, resultFactory);

            if (offset < 0) {
                size += offset;
                offset = 0;
            }
            if (size > 0) {
                super.seek(offset);
                super.limit(size);
            } else close();
        }

        @Override
        public JdbcIterator<T> keepOpen() throws JdbcIteratorException {
            throw new JdbcIteratorException("Can't use keepOpen with JdbcIterator.Window");
        }

        @Override
        public JdbcIterator<T> seek(int row) throws JdbcIteratorException {
            throw new JdbcIteratorException("Can't use seek with JdbcIterator.Window");
        }

        @Override
        public JdbcIterator<T> offset(int row) throws JdbcIteratorException {
            throw new JdbcIteratorException("Can't use offset with JdbcIterator.Window");
        }

        @Override
        public JdbcIterator<T> limit(int rows) throws JdbcIteratorException {
            throw new JdbcIteratorException("Can't use limit with JdbcIterator.Window");
        }
    }

}
