package wmr.core;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert.*;

import wmr.core.Page;
import wmr.core.Edge;
import wmr.core.Revision;
import wmr.core.User;

public class UserTest {

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
		revision = new Revision("119623749", "2007-04-02T01:52:36Z", user, "#REDIRECT[[Capo di tutti capi]]", "Capo di tutti capi", false);
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
	
	@Test public void testGetRevisionsByArticle() {
		Revision rev = user.getRevisionsByArticle(article).get(0);
		assertEquals(rev, revision);
	}
	
	@Test public void testIsCoAuthor() {
		User newUser = new User("Me", "111");
		Revision newRev = new Revision("112", "time", newUser, "sample edit", "", false);
		newUser.addToRevisions(newRev);
		article.addToRevisions(newRev);
		System.out.println(user.equals(newUser));
		assertTrue(user.isCoAuthor(article, newUser));
	}
	
}
