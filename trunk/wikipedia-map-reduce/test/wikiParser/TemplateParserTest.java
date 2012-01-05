package wikiParser;

import wikiParser.templates.Template;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertFalse;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;


public class TemplateParserTest {
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
	@Test public void testTemplateParser() throws XMLStreamException {
            Revision rev = null;
            while (true) {
                Revision nextRev = parser.getNextRevision();
                if (nextRev == null) {
                        break;
                }
                rev = nextRev;
                List<Template> templates = rev.getTemplates();
                System.out.println("revision " + rev.getId());
                for (Template t : templates) {
                    System.out.println("\t" + t.toString());
                }
            }
	}

}
