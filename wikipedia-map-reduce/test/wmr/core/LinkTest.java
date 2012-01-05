package wmr.core;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import wmr.core.Page;
import wmr.core.Edge;
import wmr.core.Revision;
import wmr.core.User;

public class LinkTest {

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
	 * @uml.property  name="link1"
	 * @uml.associationEnd  
	 */
	private Edge link1;
	/**
	 * @uml.property  name="link2"
	 * @uml.associationEnd  
	 */
	private Edge link2;
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
		link1 = new Edge(article, user, Edge.ART_EDITEDBY_USER);
		user.addToEdges(link1);
		article.addToEdges(link1);
		link2 = new Edge(user, article, Edge.USER_EDIT_ART);
		user.addToEdges(link2);
		article.addToEdges(link2);
	}
	
	@Test public void testToOutputString() {
		String tmpString = link1.toOutputString();
		assertEquals(tmpString, "u6Just_H");
		tmpString = link2.toOutputString();
		assertEquals(tmpString, "a3Capo_di_tutti_capo");
	}
	
}
