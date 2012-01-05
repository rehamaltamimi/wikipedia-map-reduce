package wmr.core;

import wmr.core.Page;
import wmr.core.GraphParser;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class GraphParserTest {
	/**
	 * @uml.property  name="parser"
	 * @uml.associationEnd  
	 */
	GraphParser parser;
	/**
	 * @uml.property  name="reader"
	 */
	BufferedReader reader;
	/**
	 * @uml.property  name="line"
	 */
	String line;

	@Before public void setUp() throws IOException {
//		parser = new ArticleParser(new BufferedInputStream(new FileInputStream("virginislandecon.xml")));
		parser = new GraphParser("");
		reader = new BufferedReader(new FileReader(""));
		line = reader.readLine();
	}

	@Test public void testExtractEntity() {
		String key = line.split("\t")[0];
		assertTrue(parser.extractEntity(key) instanceof Page);
	}
	
	@Test public void testReadGraph() {
		
	}
}
