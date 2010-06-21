package wikiParser;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;
import wikiParser.User;

public class EntityTest {

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

	@Before public void setUp() {
		user = new User("Just H", "2033654");
		revision = new Revision("119623749", "2007-04-02T01:52:36Z", user, "#REDIRECT[[Capo di tutti capi]]", "Capo di tutti capi", false, false);
		user.addToRevisions(revision);
		article = new Page("Capo di tutti capo", "10414100");
		link = new Edge(article, user, Edge.ART_EDITEDBY_USER);
		user.addToEdges(link);
		article.addToEdges(link);
		link = new Edge(user, article, Edge.USER_EDIT_ART);
		user.addToEdges(link);
	}
	
	@Test public void testAddToRevisions() {
		article.addToRevisions(revision);
		Revision tmpRevision = article.getRevisions().get(0);
		assertEquals(tmpRevision, revision);
	}
	
	@Test public void testAddToLinks() {
		article.addToEdges(link);
		Edge tmpLink = article.getEdges().get(1);
		assertEquals(tmpLink, link);
	}
	
	@Test public void testFindCommonRevisions() {
		article.addToRevisions(revision);
		article.addToEdges(link);
		assertTrue(!article.findCommonRevisions(article).isEmpty());
		Revision tmpRevision = article.findCommonRevisions(article).get(0);
		assertEquals(tmpRevision, revision);
		assertTrue(!article.findCommonRevisions(user).isEmpty());
		tmpRevision = article.findCommonRevisions(user).get(0);
		assertEquals(tmpRevision, revision);
		assertTrue(!user.findCommonRevisions(article).isEmpty());
		tmpRevision = user.findCommonRevisions(article).get(0);
		assertEquals(tmpRevision, revision);
	}
	
	@Test public void testToUnderscoredString() {
		article.addToRevisions(revision);
		article.addToEdges(link);
		String underscoredString = article.toUnderscoredString();
		assertEquals(underscoredString, "Capo_di_tutti_capo");
		assertEquals(underscoredString, article.getName().replaceAll(" ", "_"));
		
	}

}
