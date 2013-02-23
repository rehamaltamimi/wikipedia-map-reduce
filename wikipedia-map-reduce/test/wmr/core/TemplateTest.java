/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.core;

import wmr.templates.TemplateParser;
import wmr.templates.Template;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author research
 */
public class TemplateTest {
 
    
    /**
     * @uml.property  name="string"
     * @uml.associationEnd  
     */
    private String citeBookString;
    /**
     * @uml.property  name="String"
     * @uml.associationEnd  
     */
    private String citeWebString;
    private String nestedString;
	private String newLineString;
    
    @Before public void setUp() {
        citeWebString = "{{cite web| last = Lepage | first = Denis | authorlink = | coauthors = | year = | url = http://www.bsc-eoc.org/avibase/avibase.jsp?region=sr&pg=checklist&list=clements | title =  Checklist of birds of Suriname | format = | work = Bird Checklists of the World | publisher = Avibase | accessdate = 26 April 2007 }}";
        citeBookString = "{{cite book | title=Birds of the World: a Checklist | first= James F. | last = Clements | publisher = Cornell University Press | year = 2000 | id = ISBN 0-934797-16-1 | pages = 880 }}";
        nestedString  = (
            "{{WikiProjectBannerShell|collapsed=yes|1=" +
            "{{WikiProject Equine |class=GA |importance=top }}" +
            "{{WikiProject Mammals |importance=high |class=GA }}" +
            "{{WikiProject Agriculture |class=GA |importance=High }}" +
            "{{WP1.0 |class=GA |importance=high |category= |VA=yes }}" +
            "}}"
        );
        newLineString = "{{Cite book\n" +
                "| publisher = Omnibus,\n" +
                "| isbn = 071194301x\n" +
                "| last = Mabbett\n" +
                "| first = Andy\n" +
                "| title = The Complete Guide to the Music of Pink Floyd\n" +
                "| location = London\n" +
                "| date = 1995\n" +
                "}}";
    }
    
    @Test public void testProcessTemplate() {
        Template citeBook = TemplateParser.processTemplate(citeBookString.substring(2, citeBookString.length()-2),0,citeBookString.length());
        Template citeWeb = TemplateParser.processTemplate(citeWebString.substring(2, citeWebString.length()-2),0,citeWebString.length());
        System.out.println("citeBook: " + citeBook);
        System.out.println("citeWeb: " + citeWeb);
        assertTrue(citeBook.getName().equals("cite book"));
        assertTrue(null == citeWeb.getParam("authorlink"));
        assertTrue(citeWeb.getParam("accessdate").equals("26 April 2007"));
        assertTrue(citeBook.getStart() == 0);
        assertTrue(citeWeb.getEnd() == citeWebString.length());
    }

    @Test public void testNestedTemplate() {
        Template t1 = TemplateParser.processTemplate(nestedString.substring(2, nestedString.length()-2),0,nestedString.length());
        assertEquals(3, t1.getAllParams().keySet().size());
        assertTrue(t1.paramContainsTemplate("1"));
        List<Template> children = t1.getParamAsTemplate("1");
        assertEquals(4, children.size());
    }
    
    @Test public void testNewlineString() {
        Template t1 = TemplateParser.processTemplate(newLineString.substring(2, newLineString.length()-2),0,newLineString.length());
        assertEquals(8, t1.getAllParams().keySet().size());
        assertEquals(t1.getName(), "Cite book");
        assertTrue(t1.hasParam("first"));
        assertTrue(t1.hasParam("last"));
    }    
 /*   {{cite web
 | last = Lepage
 | first = Denis
 | authorlink =
 | coauthors =
 | year =
 | url = http://www.bsc-eoc.org/avibase/avibase.jsp?region=sr&pg=checklist&list=clements
 | title =  Checklist of birds of Suriname
 | format =
 | work = Bird Checklists of the World
 | publisher = Avibase
 | accessdate = 26 April 2007
 }}
{{cite book
 | title=Birds of the World: a Checklist
 | first= James F.
 | last = Clements
 | publisher = Cornell University Press
 | year = 2000
 | id = ISBN 0-934797-16-1
 | pages = 880
 }}*/
    
}
