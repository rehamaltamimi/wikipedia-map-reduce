/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser;

import wikiParser.templates.TemplateParser;
import wikiParser.templates.Template;
import java.util.ArrayList;
import wikiParser.assessments.Assessment;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author research
 */
public class AssessmentTest {
 
    Page page;
    Revision revision;
    
    @Before public void setUp() {
        page = new Page("41", "Talk: Horses");
        revision = new Revision("41", "200011070832", null, "bar", "foo", false, false);
    }
    
    @Test public void testBasicAssessment() {
        List<Assessment> al1 = get("{{WP1.0 |class=GA |importance=high |category= |VA=yes }}");
        List<Assessment> al2 = get("{{WikiProject Equine |class=GA |importance=top }}");
        assertEquals(al1.size(), 1);
        assertEquals(al2.size(), 1);
        Assessment a1 = al1.get(0);
        Assessment a2 = al2.get(0);
        assertEquals(a1.getTemplateName(), "wp1.0");
        assertEquals(a2.getTemplateName(), "wikiproject equine");
    }
    @Test public void testNestedAssessment() {
        String text = (
            "{{WikiProjectBannerShell|collapsed=yes|1=" +
            "{{WikiProject Equine |class=GA |importance=top }}" +
            "{{WikiProject Mammals |importance=high |class=GA }}" +
            "{{WikiProject Agriculture |class=GA |importance=High }}" +
            "{{WP1.0 |class=GA |importance=high |category= |VA=yes }}" +
            "}}"
        );
        List<Assessment> a1 = get(text);
        assertEquals(4, a1.size());
    }
    
    @Test public void testNestedAssessment2() {
        String text = (
            "{{WikiProjectBannerShell|" +
            "{{WikiProject Greece|class=B|importance=High|B-Class-1=yes|B-Class-2=yes|B-Class-3=yes|B-Class-4=yes|B-Class-5=yes}}" +
            "{{Classical Greece and Rome|class=B|importance=Top}}" +
            "{{maths rating|frequentlyviewed=yes|field=geometry|class=Bplus|importance=Top}}" +
            "{{WP1.0|v0.7=pass|class=B|category=Math}}" +
            "}}"
        );
        List<Assessment> a1 = get(text);
        assertEquals(4, a1.size());
    }

    List<Assessment> get(String text) {
        List<Assessment> assessments = new ArrayList<Assessment>();
        for (Template t : TemplateParser.getOneOrMoreTemplates(text)) {
            assessments.addAll(Assessment.templateToAssessment(page, revision, t));
        }
        return assessments;
    }
}
