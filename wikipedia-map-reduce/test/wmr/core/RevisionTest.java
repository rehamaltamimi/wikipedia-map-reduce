package wmr.core;

import wmr.core.Edge;
import wmr.core.PageParser;
import wmr.core.Page;
import wmr.core.Revision;
import wmr.core.User;
import java.io.IOException;
import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;

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
        /**
	 * @uml.property  name="citeRevision"
	 * @uml.associationEnd  
	 */
	private Revision citeRevision;
        /**
	 * @uml.property  name="refRevision"
	 * @uml.associationEnd  
	 */
	private Revision refRevision;
        private Revision refRevision2;

	@Before public void setUp() throws FileNotFoundException, XMLStreamException, IOException {
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
                citeRevision = new Revision("123456", "2011-06-11T14:35:00Z", user,
                        readFile("palmerstonForts.txt"),"Undid revision by VictorianForts",false,false);
                refRevision = new Revision("123456", "2011-06-11T14:35:00Z", user,
                        readFile("mauritiusBroadcastingCorporation.txt"),"Undid revision by VictorianForts",false,false);
                refRevision2 = new Revision("123456", "2011-06-11T14:35:00Z", user,
                        readFile("refTest.txt"),"Undid revision by VictorianForts",false,false);
	}

    private String readFile(String fileName) throws FileNotFoundException, IOException {
        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileName));
        String text = "";
        byte b = (byte)bis.read();
        while (b != -1) {
            text = text+(char)b;
            b = (byte)bis.read();
        }
        return text;
    }
	
    @Test public void testGetAnchorLinks() throws XMLStreamException {
            assertTrue(!revision.getAnchorLinks().isEmpty());
            String tmpLink = revision.getAnchorLinks().get(0);
            assertEquals(tmpLink, "Capo di tutti capi");

            Revision rev = parser.getNextRevision();
            List<String> links = rev.getAnchorLinks();
            for (String l : links) {
                    System.out.println(l);
            }
    }

    @Test public void testRedirects() throws XMLStreamException {
        assertFalse(refRevision.isRedirect());
        assertTrue(revision.isRedirect());
        assertEquals(revision.getRedirectDestination(), "Capo di tutti capi");
        Revision weird = new Revision("123456", "2011-06-11T14:35:00Z", user,
                        "asdfasf asdfas\n #REDIRECT  [[foo\\blah! de  ]]","Undid revision by VictorianForts",false,false);
        assertTrue(weird.isRedirect());
        assertEquals(weird.getRedirectDestination(), "foo\\blah! de");
    }

    @Test public void testDisambiguation() {
        assertFalse(revision.isDisambiguation());
        Revision r1 = new Revision("123456", "2011-06-11T14:35:00Z", user,
                        "boo {{disambig}}","Undid revision by VictorianForts",false,false);
        assertTrue(r1.isDisambiguation());
        Revision r2 = new Revision("123456", "2011-06-11T14:35:00Z", user,
                        "{{disambiguation}}","Undid revision by VictorianForts",false,false);
        assertTrue(r2.isDisambiguation());
        Revision r3 = new Revision("123456", "2011-06-11T14:35:00Z", user,
                        "boo {{hndis | foo bar}}  ","Undid revision by VictorianForts",false,false);
        assertTrue(r3.isDisambiguation());
        Revision r4 = new Revision("123456", "2011-06-11T14:35:00Z", user,
                        "boo {{dab | 1 = 3}}","Undid revision by VictorianForts",false,false);
        assertTrue(r4.isDisambiguation());
    }


    @Test public void testDisambiguationLinks() {
        assertFalse(revision.isDisambiguation());
        List<String> links = makeRevision("*[[Foo bar]]").getDisambiguationLinks();
        assertEquals(links.size(), 1);
        assertTrue(links.contains("Foo bar"));
        links = makeRevision("*\"[[Foo bar]]\"").getDisambiguationLinks();
        assertEquals(links.size(), 1);
        assertTrue(links.contains("Foo bar"));
        links = makeRevision("*z[[Foo bar]]").getDisambiguationLinks();
        assertEquals(links.size(), 0);
        links = makeRevision("*\"[[Foo bar]]\"\n[[Baz]]\n** [[Dah deh | Goz]]").getDisambiguationLinks();
        assertEquals(links.size(), 2);
        assertTrue(links.contains("Foo bar"));
        assertTrue(links.contains("Dah deh "));
        links = makeRevision("*\"[[Foo bar]]\"\n[[Baz]]\n** [[Dah deh#basss | Goz]]").getDisambiguationLinksWithoutFragments();
        assertEquals(links.size(), 2);
        assertTrue(links.contains("Foo bar"));
        assertTrue(links.contains("Dah deh"));
    }


    @Test public void testVandalism() {
        assertTrue(isVandalism("rvv"));
        assertTrue(isVandalism("vandalism"));
        assertTrue(isVandalism("rev spam"));
        assertTrue(isVandalism("foo rvv"));
        assertFalse(isVandalism("rev boo"));
        assertTrue(isVandalism("reverted edits by foo using bar"));
        assertTrue(isVandalism("undo revision by zad"));
        assertTrue(isVandalism("([[WP:HG|HG]])"));
        assertTrue(isVandalism("([[WP:TW|TW]])"));
    }

    private boolean isVandalism(String comment) {
        Revision r = makeRevision("");
        r.setComment(comment);
        return r.isVandalism();
    }
    
    private Revision makeRevision(String text) {
        return new Revision("123456", "2011-06-11T14:35:00Z", user,
                        text,"Undid revision by VictorianForts",false,false);
    }
}
