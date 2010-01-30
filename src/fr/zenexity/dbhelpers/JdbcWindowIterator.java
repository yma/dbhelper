package fr.zenexity.dbhelpers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JdbcWindowIterator<T> extends JdbcIterator<T> {

    private int pageSize;

    public JdbcWindowIterator(ResultSet result, int pageOffset, int pageSize, JdbcResult.Factory<T> resultFactory) throws SQLException {
        super(result, resultFactory);
        this.pageSize = pageSize;
        seek(pageOffset);
    }

    public JdbcWindowIterator(ResultSet result, int pageOffset, int pageSize, Class<T> resultClass) throws SQLException {
        super(result, resultClass);
        this.pageSize = pageSize;
        seek(pageOffset);
    }

    public JdbcWindowIterator(ResultSet result, int pageOffset, int pageSize, Class<T> resultClass, String... fields) throws SQLException {
        super(result, resultClass, fields);
        this.pageSize = pageSize;
        seek(pageOffset);
    }

    public JdbcWindowIterator(ResultSet result, int pageOffset, int pageSize, Class<T> resultClass, List<String> fields) throws SQLException {
        super(result, resultClass, fields);
        this.pageSize = pageSize;
        seek(pageOffset);
    }

    private void seek(int pageOffset) throws SQLException {
        if (result != null) {
            if (pageOffset < 0) {
                pageSize += pageOffset;
                pageOffset = 0;
            }
            if (pageSize > 0) {
                if (pageOffset == 0) result.beforeFirst();
                else result.absolute(pageOffset);
            } else close();
        }
    }

    @Override
    protected void load() {
        if (next == null) {
            if (pageSize-- > 0) super.load();
            else close();
        }
    }

}
