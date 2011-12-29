package wikiParser;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;

import wikiParser.Page;
import wikiParser.PageParser;
import wikiParser.Revision;


public class ArticleParserTest {
	/**
	 * @uml.property  name="parser"
	 * @uml.associationEnd  
	 */
	PageParser parser;
	
	@Before public void setUp() throws FileNotFoundException, XMLStreamException {
		parser = new PageParser(new BufferedInputStream(new FileInputStream("virginislandecon.xml")));
	}

	@Test public void testArticleInformation() throws XMLStreamException {
		Page article = parser.getArticle();
		assertEquals(article.getId(), "32140");
		assertEquals(article.getName(), "Economy of the United States Virgin Islands");

	}
	@Test public void testRevisionCount() throws XMLStreamException {
		int i = 0;
		while (true) {
			Revision rev = parser.getNextRevision();
			if (rev == null) {
				break;
			}
			i++;
		}
		assertEquals(i, 25);
	}
	
	@Test public void testFirstRevision() throws XMLStreamException {
		Revision rev = parser.getNextRevision();
		assertEquals(rev.getId(), "90956");
		assertEquals(rev.getTimestamp(), "2002-06-13T22:45:44Z");
		assertEquals(rev.getContributor().getId(), "Conversion script");
		assertEquals(rev.getComment(), "Automated conversion");

                System.out.println("getText gives " + rev.getText() + " with length " + rev.getText().length());
		assertTrue(rev.getText().trim().startsWith("<b>Economy - overview:</b>"));
		assertTrue(rev.getText().trim().endsWith(":''See also :'' [[U.S. Virgin Islands]]"));
	}
	
	@Test public void testLastRevision() throws XMLStreamException {
		Revision rev = null;
		while (true) {
			Revision nextRev = parser.getNextRevision();
			if (nextRev == null) {
				break;
			}
			rev = nextRev;
		}
		assertEquals(rev.getId(), "169082996");
		assertEquals(rev.getTimestamp(), "2007-11-04T03:15:50Z");
		assertEquals(rev.getContributor().getName(), "Ph89");
		assertEquals(rev.getContributor().getId(), "818695");
		assertEquals(rev.getComment(),
				"[[WP:UNDO|Undid]] revision 166504668 by " + 
				"[[Special:Contributions/66.185.46.66|66.185.46.66]] " +
				"([[User talk:66.185.46.66|talk]])");
		assertTrue(rev.getText().trim().startsWith("{{Cleanup|date=April 2007}}"));
		assertTrue(rev.getText().trim().endsWith("[[Category:Economy of the United States Virgin Islands| ]]"));
	}

        @Test public void testComments() throws XMLStreamException {
            Revision first = parser.getNextRevision();
            assertFalse(first.getText().contains("THIS SHOULD"));
            assertFalse(first.getText().contains("DISAPPEAR"));
        }


	@Test public void testCurrentRevision() throws XMLStreamException, FileNotFoundException {
		parser = new PageParser(new BufferedInputStream(new FileInputStream("accessible_computing.xml")));
		Page article = parser.getArticle();
		assertEquals(article.getId(), "10");
		assertEquals(article.getName(), "AccessibleComputing");
                assertTrue(parser.hasNextRevision());
                assertTrue(parser.getNextRevision() != null);
                assertFalse(parser.hasNextRevision());

	}
}
