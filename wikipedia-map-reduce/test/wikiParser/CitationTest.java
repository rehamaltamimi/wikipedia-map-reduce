package wikiParser;

import java.io.IOException;
import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;

import wikiParser.Page;
import wikiParser.Edge;
import wikiParser.Revision;
import wikiParser.User;
import wikiParser.citations.Citation;

public class CitationTest {

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
	
    @Test public void testGetCites() throws XMLStreamException {
        List<Citation> cites = citeRevision.getCitations(article);//should have four (3 cite web, 1 cite book)
        System.out.println("citations:");
        for (Citation c : cites) {
            System.out.println(c);
        }
        assertEquals(cites.size(), 21);
        List<Citation> refs = refRevision.getCitations(article); //should have two (one ref with url in brackets, one just a plain url)
        System.out.println("references :");
        for (Citation c : refs) {
            System.out.println(c);
        }
        assertEquals(refs.size(), 2);
    }
    
    @Test public void testGetUrls() throws XMLStreamException {
        List<Citation> refs = refRevision.getCitations(article);
        System.err.println("found " + refs);
        for (Citation r : refs) {
            assertTrue(r.getUrl().equals("http://mbc.intnet.mu/corporate_info.htm") || 
                       r.getUrl().equals("http://www.gov.mu/portal/goc/pmo/file/mbc.doc"));
        }
    }
        
    @Test public void testLeftBracket() throws FileNotFoundException, IOException {
        Revision weird = new Revision("12", "2011-06-11T14:35:00Z", user, readFile("phillipGogulla.txt"), "comment",false, false);
        List<Citation> refs = weird.getCitations(article);
        System.out.println("references :");
        for (Citation c : refs) {
            System.out.println(c);
        }
        assertEquals(refs.size(), 5);
    }
}
