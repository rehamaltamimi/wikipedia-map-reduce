/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wikiParser.aps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import wikiParser.Page;
import wikiParser.Revision;
import wikiParser.Template;

/**
 *
 * @author shilad
 */
public class Assessment {
    private String articleName;
    private String templateName;
    private String assessment;
    private String importance;
    private String timestamp;

    public Assessment(String articleName, String templateName, String assessment) {
        this.articleName = articleName;
        this.templateName = templateName;
        this.assessment = assessment;
    }

    public String getTemplateName() {
        return templateName;
    }

    public String getArticleName() {
        return articleName;
    }

    public String getAssessment() {
        return assessment;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getImportance() {
        return importance;
    }

    public void setImportance(String importance) {
        this.importance = importance;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Assessment{" + "articleName=" + articleName + " assessment=" + assessment + " templateName=" + templateName + " importance=" + importance + " timestamp=" + timestamp + '}';
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Assessment other = (Assessment) obj;
        if ((this.articleName == null) ? (other.articleName != null) : !this.articleName.equals(other.articleName)) {
            return false;
        }
        if ((this.assessment == null) ? (other.assessment != null) : !this.assessment.equals(other.assessment)) {
            return false;
        }
        if ((this.templateName == null) ? (other.templateName != null) : !this.templateName.equals(other.templateName)) {
            return false;
        }
        if ((this.importance == null) ? (other.importance != null) : !this.importance.equals(other.importance)) {
            return false;
        }
        if (this.timestamp != other.timestamp && (this.timestamp == null || !this.timestamp.equals(other.timestamp))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 67 * hash + (this.articleName != null ? this.articleName.hashCode() : 0);
        hash = 67 * hash + (this.assessment != null ? this.assessment.hashCode() : 0);
        hash = 67 * hash + (this.templateName != null ? this.templateName.hashCode() : 0);
        hash = 67 * hash + (this.importance != null ? this.importance.hashCode() : 0);
        hash = 67 * hash + (this.timestamp != null ? this.timestamp.hashCode() : 0);
        return hash;
    }

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
        RATING_TRANSLATOR.put("ffa/ga", "fa");
        RATING_TRANSLATOR.put("dga", "fa");
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
            Assessment ass = new Assessment(page.getName(), template.getName(), rating);
            if (revision.getTimestamp() != null) {
                ass.setTimestamp(revision.getTimestamp());
            }
            if (template.hasParam("importance")) {
                ass.setImportance(template.getParam("importance"));
            }
            result.add(ass);
        }
        return result;
    }
    
}
