package fr.zenexity.dbhelper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

public class SqlScriptTest {

    @Test
    public void testLoadScript() {
        assertEquals(Arrays.<String>asList(), SqlScript.loadScript("", ";"));
        assertEquals(Arrays.<String>asList(), SqlScript.loadScript("   ", ";"));
        assertEquals(Arrays.<String>asList(), SqlScript.loadScript(";", ";"));
        assertEquals(Arrays.asList("Hello"), SqlScript.loadScript("Hello", ";"));
        assertEquals(Arrays.asList("Hello"), SqlScript.loadScript("   Hello   ", ";"));
        assertEquals(Arrays.asList("Hello"), SqlScript.loadScript("Hello;", ";"));
        assertEquals(Arrays.asList("Hello", "World"), SqlScript.loadScript("Hello;World", ";"));
        assertEquals(Arrays.asList("Hello", "World"), SqlScript.loadScript(" Hello ; World ;", ";"));
        assertEquals(Arrays.asList("Hello", "World"), SqlScript.loadScript(";Hello;; World;;", ";"));
    }

    @Test
    public void testLoadScriptTinyStream() throws IOException {
        String script = "";
        List<String> commands = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            String command = "** sql command n°"+i;
            commands.add(command);
            script += command+";\n";
        }
        assertEquals(commands, SqlScript.loadScript(new ByteArrayInputStream(script.getBytes())));
    }

    @Test
    public void testLoadScriptBigStream() throws IOException {
        String script = "";
        List<String> commands = new ArrayList<String>();
        for (int i = 0; i < 1000; i++) {
            String command = "** sql command n°"+i;
            commands.add(command);
            script += command+";\n";
        }
        assertEquals(commands, SqlScript.loadScript(new ByteArrayInputStream(script.getBytes())));
    }

    @Test
    public void testLoadScriptEncoding() throws IOException {
        String script = "Hélo";
        assertEquals(Arrays.asList("Hélo"), SqlScript.loadScript(new ByteArrayInputStream(script.getBytes()), null));
        assertEquals(Arrays.asList("Hélo"), SqlScript.loadScript(new ByteArrayInputStream(script.getBytes()), "UTF-8"));
        assertEquals(Arrays.asList("H\303\251lo"), SqlScript.loadScript(new ByteArrayInputStream(script.getBytes()), "ISO-8859-1"));
    }

}
