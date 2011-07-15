/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser;

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
    
    @Before public void setUp() {
        citeWebString = "{{cite web| last = Lepage | first = Denis | authorlink = | coauthors = | year = | url = http://www.bsc-eoc.org/avibase/avibase.jsp?region=sr&pg=checklist&list=clements | title =  Checklist of birds of Suriname | format = | work = Bird Checklists of the World | publisher = Avibase | accessdate = 26 April 2007 }}";
        citeBookString = "{{cite book | title=Birds of the World: a Checklist | first= James F. | last = Clements | publisher = Cornell University Press | year = 2000 | id = ISBN 0-934797-16-1 | pages = 880 }}";
    }
    
    @Test public void testProcessTemplate() {
        Template citeBook = Template.processTemplate(citeBookString.substring(2, citeBookString.length()-2),0,citeBookString.length());
        Template citeWeb = Template.processTemplate(citeWebString.substring(2, citeWebString.length()-2),0,citeWebString.length());
        assertTrue(citeBook.getParam("templateName").equals("cite book"));
        assertTrue(null == citeWeb.getParam("authorlink"));
        assertTrue(citeWeb.getParam("accessdate").equals("26 April 2007"));
        assertTrue(citeBook.getStart() == 0);
        assertTrue(citeWeb.getEnd() == citeWebString.length());
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
