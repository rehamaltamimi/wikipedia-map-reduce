package wmr.core;


import wmr.core.Edge;
import wmr.core.Page;
import wmr.core.Revision;
import wmr.core.User;
import wmr.core.HashCodeMap;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert.*;


public class HashCodeMapTest {

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
	 * @uml.property  name="hashCodeMap"
	 * @uml.associationEnd  
	 */
	private HashCodeMap hashCodeMap;

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
		hashCodeMap = new HashCodeMap(100);
		for (int i=0;i<50;i++) {
			hashCodeMap.add(new Integer(i), revision);
		}
	}
	
	@Test public void testGet() {
		Revision tmpRev = hashCodeMap.get(0);
		assertEquals(tmpRev, revision);
	}
	
	@Test public void testAddAll() {
		HashCodeMap newHashCodeMap = new HashCodeMap(51);
		newHashCodeMap.add(new Integer(1000), revision);
		newHashCodeMap.addAll(hashCodeMap);
		assertEquals(newHashCodeMap.size(),51);
	}
	
	@Test public void testMaxEntries() {
		for (int i=0;i<500;i++) {
			hashCodeMap.add(new Integer(i), revision);
		}
		assertEquals(hashCodeMap.size(), 100);
	}
	
}
