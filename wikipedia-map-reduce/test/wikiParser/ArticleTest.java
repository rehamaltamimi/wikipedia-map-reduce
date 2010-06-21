package wikiParser;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;


public class ArticleTest {
	
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
		article.addToRevisions(revision);
		link = new Edge(article, user, Edge.ART_EDITEDBY_USER);
		user.addToEdges(link);
		article.addToEdges(link);
		link = new Edge(user, article, Edge.USER_EDIT_ART);
		user.addToEdges(link);
		article.addToEdges(link);
	}
	
	@Test public void testConstructor() {
		assertEquals(article.getName(), "Capo di tutti capo");
		assertEquals(article.getId(), "10414100");
	}
	
	@Test public void testGetContributors() {
		assertTrue(!article.getContributors().isEmpty());
		User tmpUser = article.getContributors().iterator().next();
		assertEquals(tmpUser, user);
	}
	
	@Test public void testGetAnchorLinks() {
		assertTrue(!article.getAnchorLinks().isEmpty());
		String tmpLink = article.getAnchorLinks().iterator().next();
		assertEquals(tmpLink, "Capo di tutti capi");
	}
	
}
