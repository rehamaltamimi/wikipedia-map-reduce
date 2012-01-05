package wmr.core;

import java.util.Arrays;
import static org.junit.Assert.*;

import org.junit.Test;
import wikiParser.util.Splitter;

public class TestSplitter {

    @Test
    public void testSplitter1() {
        Splitter splitter = new Splitter(true);
        String[] tokens = splitter.split("foo. bar");
//        System.out.println("tokens are " + Arrays.deepToString(tokens));
        assertEquals(3, tokens.length);
        assertEquals("foo", tokens[0]);
        assertEquals(". ", tokens[1]);
        assertEquals("bar", tokens[2]);
    }
    @Test
    public void testSplitter2() {
        Splitter splitter = new Splitter(false);
        String[] tokens = splitter.split("foo. bar");
//        System.out.println("tokens are " + Arrays.deepToString(tokens));
        assertEquals(2, tokens.length);
        assertEquals("foo", tokens[0]);
        assertEquals("bar", tokens[1]);
    }
    @Test
    public void testSplitter3() {
        Splitter splitter = new Splitter(false);
        String[] tokens = splitter.split("  foo. bar$%@\n");
//        System.out.println("tokens are " + Arrays.deepToString(tokens));
        assertEquals(2, tokens.length);
        assertEquals("foo", tokens[0]);
        assertEquals("bar", tokens[1]);
    }
    @Test
    public void testSplitter4() {
        Splitter splitter = new Splitter(true);
        String[] tokens = splitter.split("Foo. bar341-3");
//        System.out.println("tokens are " + Arrays.deepToString(tokens));
        assertEquals(5, tokens.length);
        assertEquals("Foo", tokens[0]);
        assertEquals(". ", tokens[1]);
        assertEquals("bar341", tokens[2]);
        assertEquals("-", tokens[3]);
        assertEquals("3", tokens[4]);
    }
    @Test
    public void testSplitter5() {
        Splitter splitter = new Splitter(true);
        String[] tokens = splitter.split("the quick brown fox jumped over the lazy dog 0123456789");
//        System.out.println("tokens are " + Arrays.deepToString(tokens));
        assertEquals(19, tokens.length);
        assertEquals("the", tokens[0]);
        assertEquals(" ", tokens[1]);
        assertEquals("quick", tokens[2]);
        assertEquals(" ", tokens[3]);
        assertEquals("brown", tokens[4]);
        assertEquals(" ", tokens[5]);
        assertEquals("fox", tokens[6]);
        assertEquals(" ", tokens[7]);
        assertEquals("jumped", tokens[8]);
        assertEquals(" ", tokens[9]);
        assertEquals("over", tokens[10]);
        assertEquals(" ", tokens[11]);
        assertEquals("the", tokens[12]);
        assertEquals(" ", tokens[13]);
        assertEquals("lazy", tokens[14]);
        assertEquals(" ", tokens[15]);
        assertEquals("dog", tokens[16]);
        assertEquals(" ", tokens[17]);
        assertEquals("0123456789", tokens[18]);
    }
}
