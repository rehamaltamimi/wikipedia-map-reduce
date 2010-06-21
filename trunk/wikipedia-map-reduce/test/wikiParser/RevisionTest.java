package wikiParser;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;
import wikiParser.User;

public class RevisionTest {

	/**
	 * @uml.property  name="user"
	 * @uml.associationEnd  
	 */
	private User user;
	/**
	 * @uml.property  name="article"
	 * @uml.associationEnd  
	 */
	private Page article;
	/**
	 * @uml.property  name="link"
	 * @uml.associationEnd  
	 */
	private Edge link;
	/**
	 * @uml.property  name="revision"
	 * @uml.associationEnd  
	 */
	private Revision revision;
	/**
	 * @uml.property  name="parser"
	 * @uml.associationEnd  
	 */
	private PageParser parser;

	@Before public void setUp() throws FileNotFoundException, XMLStreamException {
		user = new User("Just H", "2033654");
		revision = new Revision("119623749", "2007-04-02T01:52:36Z", user, "#REDIRECT[[Capo di tutti capi]]", "Capo di tutti capi", false, false);
		user.addToRevisions(revision);
		article = new Page("Capo di tutti capo", "10414100");
		article.addToRevisions(revision);
		link = new Edge(article, user, Edge.ART_EDITEDBY_USER);
		user.addToEdges(link);
		article.addToEdges(link);
		link = new Edge(user, article, Edge.USER_EDIT_ART);
		user.addToEdges(link);
		article.addToEdges(link);
		parser = new PageParser(new BufferedInputStream(new FileInputStream("virginislandecon.xml")));
	}
	
	@Test public void testGetAnchorLinks() throws XMLStreamException {
		assertTrue(!revision.getAnchorLinks().isEmpty());
		String tmpLink = revision.getAnchorLinks().get(0);
		assertEquals(tmpLink, "Capo di tutti capi");
		
		Revision rev = parser.getNextRevision();
		ArrayList<String> links = rev.getAnchorLinks();
		for (String l : links) {
			System.out.println(l);
		}
	}
	
}
