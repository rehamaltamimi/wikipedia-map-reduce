/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.assessments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import wmr.core.Page;
import wmr.core.Revision;
import wmr.templates.Template;

/**
 *
 * @author shilad
 */
public class AssessmentParser {

    private static final Map<String, String> RATING_TRANSLATOR = new HashMap<String, String>();
    private static final String[] BASE_RATINGS = new String[] {
        "ffa",
        "ffac",
        "fflc",
        "ffgan",
        "fa",
        "fl",
        "ga",
        "a",
        "bplus",
        "c",
        "start",
        "stub",
        "list",
        "list",
        "sl",
        "dab",
        "cur",
        "future"
    };

    static {
        for (String r : BASE_RATINGS) {
            RATING_TRANSLATOR.put(r, r);
        }
        RATING_TRANSLATOR.put("ffa/ga", "ffa");
        RATING_TRANSLATOR.put("dga", "fga");
        RATING_TRANSLATOR.put("disambig", "dab");
        RATING_TRANSLATOR.put("formerfa2", "ffa");
        RATING_TRANSLATOR.put("formerfa", "ffa");
        RATING_TRANSLATOR.put("formerfa", "ffa");
    }

    private static final Map<String, String> TEMPLATE_RATINGS = new HashMap<String, String>();
    static {
        TEMPLATE_RATINGS.put("featured_article_candidate", "fac");
        TEMPLATE_RATINGS.put("featured article candidate", "fac");
        TEMPLATE_RATINGS.put("featured list candidate", "flc");
        TEMPLATE_RATINGS.put("featured_list_candidate", "flc");
        TEMPLATE_RATINGS.put("ga nominee", "gan");
        TEMPLATE_RATINGS.put("ganominee", "gan");
        TEMPLATE_RATINGS.put("featured article review", "far");
        TEMPLATE_RATINGS.put("featured_article_review", "far");
        TEMPLATE_RATINGS.put("featured list removal candidate", "flrc");
        TEMPLATE_RATINGS.put("featured_list_removal_candidate", "flrc");
        TEMPLATE_RATINGS.put("gar/link", "gar");
    }


    public static List<Assessment> templateToAssessment(Page page, Revision revision, Template template) {
        List<Assessment> result = new ArrayList<Assessment>();
        template.convertMapToLowercase();
        String rating = null;
        if (template.getName().equalsIgnoreCase("class") || template.getName().equalsIgnoreCase("currentstatus")) {
            for (String r : RATING_TRANSLATOR.keySet()) {
                if (template.hasParam(r)) {
                    rating = r;
                    break;
                }
            }
        }
        if (rating == null) {
            rating = template.getParam("class");
        }
        if (rating == null) {
            rating = template.getParam("currentstatus");
        }
        if (rating == null && TEMPLATE_RATINGS.containsKey(template.getName())) {
            rating = TEMPLATE_RATINGS.get(template.getName());
        }
        if (rating != null && RATING_TRANSLATOR.containsKey(rating)) {
            rating = RATING_TRANSLATOR.get(rating);
        }
        if (rating == null) {
            for (String param : template.getAllParams().keySet()) {
                if (template.paramContainsTemplate(param)) {
                    for (Template t : template.getParamAsTemplate(param)) {
                        result.addAll(templateToAssessment(page, revision, t));
                    }
                }
            }
        } else {
            Assessment ass = new Assessment(page, revision, template.getName(), rating);
            if (template.hasParam("importance")) {
                ass.setImportance(template.getParam("importance"));
            }
            if (template.hasParam("auto") && !template.getParam("auto").isEmpty()) {
                ass.setFromBot(true);
            }
            result.add(ass);
        }
        return result;
    }
}
