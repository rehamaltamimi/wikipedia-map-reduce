package wmr.core;

import wmr.core.RevisionFingerprinter;
import wmr.core.Revision;
import wmr.core.User;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

public class FingerprintingParserTest {
	
	/**
	 * @uml.property  name="fParser"
	 * @uml.associationEnd  
	 */
	private RevisionFingerprinter fParser;
	/**
	 * @uml.property  name="user1"
	 * @uml.associationEnd  
	 */
	private User user1;
	/**
	 * @uml.property  name="user2"
	 * @uml.associationEnd  
	 */
	private User user2;
	/**
	 * @uml.property  name="rev1"
	 * @uml.associationEnd  
	 */
	private Revision rev1;
	/**
	 * @uml.property  name="rev2"
	 * @uml.associationEnd  
	 */
	private Revision rev2;
	/**
	 * @uml.property  name="rev3"
	 * @uml.associationEnd  
	 */
	private Revision rev3;

	@Before public void setUp() {
		fParser =  new RevisionFingerprinter();
		user1 = new User("colin");
		rev1 = new Revision("0", "0", user1, "this isn't", "", false, false);
		user2 = new User("shilad");
		rev2 = new Revision("1", "1", user2, "[this | is all] new text, but this isn't", "", false, false);
		rev3 = new Revision("1", "1", user1, "here's some more text, oh, and old stuff: [this | is all] new text, but this isn't", "", false, false);
	}
	
	@Test public void testNeighboringEditors() {
		ArrayList<String> onesUniqueText = fParser.findUniqueText(rev1);
		fParser.update(rev1);
		ArrayList<String> twosUniqueText = fParser.findUniqueText(rev2);
		fParser.update(rev2);
		ArrayList<String> threesUniqueText = fParser.findUniqueText(rev3);
		fParser.update(rev3);
		
		Set<User> neighbors = fParser.neighboringContributors(rev2, twosUniqueText);
		assert(neighbors.contains(user1));
	}
	
	@Test public void testFindUniqueText() {
		ArrayList<String> onesUniqueText = fParser.findUniqueText(rev1);
		assertEquals(onesUniqueText.get(0), "this isn't");
		fParser.update(rev1);
		ArrayList<String> twosUniqueText = fParser.findUniqueText(rev2);
		assertEquals(twosUniqueText.get(0), "[this | is all] new text, but ");
		fParser.update(rev2);
		ArrayList<String> threesUniqueText = fParser.findUniqueText(rev3);
		assertEquals(threesUniqueText.get(0), "here's some more text, oh, and old stuff: [");
		fParser.update(rev3);
	}
	
	@Test public void update() {
		
	}
}
