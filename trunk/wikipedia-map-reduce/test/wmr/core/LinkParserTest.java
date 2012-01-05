package wmr.core;

import wmr.core.Edge;
import wmr.core.PageParser;
import wmr.core.EdgeParser;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class LinkParserTest {

	/**
	 * @uml.property  name="ap"
	 * @uml.associationEnd  
	 */
	PageParser ap;
	/**
	 * @uml.property  name="parser"
	 * @uml.associationEnd  
	 */
	EdgeParser parser;
	
	@Before public void setUp() throws FileNotFoundException, XMLStreamException {
		this.ap = new PageParser(new BufferedInputStream(new FileInputStream("virginislandecon.xml")));
		this.parser = new EdgeParser();
	}
	
	@Test public void findLinks() throws XMLStreamException {
		ArrayList<Edge> links = this.parser.findEdges(ap);
		for (Edge link : links) {
			System.out.println(link.toOutputString());
		}
	}
	
}
