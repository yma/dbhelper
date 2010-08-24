package fr.zenexity.dbhelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlScript implements Iterable<Sql.FinalUpdateQuery> {

    public static final String defaultDelimiter = ";";

    public static SqlScript from(Sql.UpdateQuery... queries) {
        SqlScript script = new SqlScript();
        for (Sql.UpdateQuery query : queries) script.add(query);
        return script;
    }

    public static SqlScript from(Iterable<Sql.UpdateQuery> queries) {
        SqlScript script = new SqlScript();
        for (Sql.UpdateQuery query : queries) script.add(query);
        return script;
    }

    public static SqlScript fromString(String... queries) {
        SqlScript script = new SqlScript();
        for (String query : queries) script.add(query);
        return script;
    }

    public static SqlScript fromString(Iterable<String> queries) {
        SqlScript script = new SqlScript();
        for (String query : queries) script.add(query);
        return script;
    }

    public static SqlScript from(File scriptFile) throws IOException {
        return from(new FileInputStream(scriptFile));
    }

    public static SqlScript from(File scriptFile, String encoding) throws IOException {
        return from(new FileInputStream(scriptFile), encoding);
    }

    public static SqlScript from(InputStream scriptStream) throws IOException {
        return fromString(loadScript(scriptStream));
    }

    public static SqlScript from(InputStream scriptStream, String encoding) throws IOException {
        return fromString(loadScript(scriptStream, encoding));
    }

    public static SqlScript from(InputStream scriptStream, String encoding, String delimiter) throws IOException {
        return fromString(loadScript(scriptStream, encoding, delimiter));
    }

    public static List<String> loadScript(InputStream scriptStream) throws IOException {
        return loadScript(scriptStream, null, null);
    }

    public static List<String> loadScript(InputStream scriptStream, String encoding) throws IOException {
        return loadScript(scriptStream, encoding, null);
    }

    public static List<String> loadScript(InputStream scriptStream, String encoding, String delimiter) throws IOException {
        InputStreamReader scriptReader = encoding == null
            ? new InputStreamReader(scriptStream)
            : new InputStreamReader(scriptStream, encoding);
        StringWriter script = new StringWriter();
        char[] buffer = new char[4096];
        for (int n; (n = scriptReader.read(buffer)) != -1;) script.write(buffer, 0, n);
        return loadScript(script.toString(), delimiter);
    }

    public static List<String> loadScript(String script) {
        return loadScript(script, null);
    }

    public static List<String> loadScript(String script, String delimiter) {
        if (delimiter == null) delimiter = defaultDelimiter;
        List<String> commands = new ArrayList<String>();
        while (script.length() != 0) {
            final String command;
            int ndx = script.indexOf(delimiter);
            if (ndx == -1) {
                command = script.trim();
                script = "";
            } else {
                command = script.substring(0, ndx).trim();
                script = script.substring(ndx+1);
            }
            if (command.length() != 0) commands.add(command);
        }
        return commands;
    }

    private final List<Sql.FinalUpdateQuery> commands;

    public SqlScript() {
        commands = new ArrayList<Sql.FinalUpdateQuery>();
    }

    public void add(Sql.UpdateQuery query) {
        commands.add(Sql.finalQuery(query));
    }

    public void add(String query) {
        commands.add(new Sql.FinalUpdateQuery(query));
    }

    public Iterator<Sql.FinalUpdateQuery> iterator() {
        return commands.iterator();
    }

}
