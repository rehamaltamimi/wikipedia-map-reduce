/*
 * Adaptation of Nathaniel's citation parser code.
 */

package wmr.citations;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import wmr.core.Page;
import wmr.core.Revision;
import wmr.templates.Template;
import wmr.templates.TemplateParser;
import wmr.util.Utils;

/**
 *
 * @author shilad
 */
public class CitationParser {
    
    public boolean isCitation(Template t) {
        if (t == null) {
            return false;
        }
        String words [] = t.getName().trim().split(" ");
        if (words.length == 0) {
            return false;
        } else {
            String name = words[0].toLowerCase();
            return name.startsWith("cite") || name.startsWith("citation");
        }
    }

    private static final Pattern REF_START = Pattern.compile("<[\\s]*ref[^/]*?>");
    private static final Pattern REF_END = Pattern.compile("(<[\\s]*/[\\s]*ref[\\s]*>)");


    public List<Citation> extractCitations(Page page, Revision revision) {
        List<Citation> cites = new ArrayList<Citation>();
        
        // First, look for citations in templates
        for (Template t : TemplateParser.getOneOrMoreTemplates(revision.getText())) {
            cites.addAll(templateToCitations(page, revision, t));
        }

        // Look for citations in REF tags
        String content = revision.getText();
        int i = 0;
        while (true) {
            Matcher open = REF_START.matcher(content);
            if (!open.find()) {
                break;
            }
            Matcher close = REF_END.matcher(content);
            if (!close.find() || open.end() >= close.start()) {
                String str = content.substring(open.start());
                str = Utils.cleanupString(str, 47);
                System.err.println("No end of reference found in page " + page.getName() +
                        ", revision " + revision.getId() + " at time " + revision.getTimestamp() +
                        " beginning at '" + str + "'");
                break;
            }
            // we have a beginning and an ending!
            String body = content.substring(open.end(), close.start());
            cites.addAll(processOneRefTag(page, revision, body, i + open.start()));
            content = content.substring(close.end());
            i += close.end();
        }
        return cites;
    }

    private static final Pattern URL_CONTAINER = Pattern.compile(".*?\\[(.*?)\\].*?");

    private List<Citation> processOneRefTag(Page page, Revision rev, String contents, int index) {
        List<Template> templates = TemplateParser.getOneOrMoreTemplates(contents, index);
        List<Citation> cites = new ArrayList<Citation>();
        for (Template t : templates) {
            if (!templateToCitations(page, rev, t).isEmpty()) {
                return cites;  // Already processed enclosed citations.
            }
        }
        String url = null;
        Matcher urlMatcher = URL_CONTAINER.matcher(contents);
        if (urlMatcher.matches()) {
            String inBrackets = urlMatcher.group(1);
            String[] splitInBrack = inBrackets.split(" ");
            if (splitInBrack.length > 0) {
                url = splitInBrack[0].trim();
                if (url.length() > 0 && url.charAt(0) == '[') {
                    String[] tmp = inBrackets.substring(1).split("\\|");
                    if (tmp.length > 0) {
                        url = "wiki:" + tmp[0];
                    }
                }
                cites.add(new Citation(page, rev, url, index));
            }
        } else {
            if (templates.size() > 0) {
                cites.add(new Citation(page, rev, templates.get(0), index));
            } else if (contents.contains("http")) {
                cites.add(new Citation(page, rev, contents.trim(), index));
            } else {
                cites.add(new Citation(page, rev, "noURL", index));
            }
        }
        return cites;
    }

    /**
     * Checks to see if the template, or any of its direct children,
     * are citation templates. Doesn't go any deeper.
     * @param page
     * @param rev
     * @param t
     * @return
     */
    private List<Citation> templateToCitations(Page page, Revision rev, Template t) {
        t.convertMapToLowercase();
        List<Citation> cites = new ArrayList<Citation>();
        if (isCitation(t)) {
            cites.add(new Citation(page, rev, t, getCiteStart(rev, t)));
        } else {
            for (String param : t.getAllParams().keySet()) {
                if (t.paramContainsTemplate(param)) {
                    for (Template t2 : t.getParamAsTemplate(param)) {
                        t2.convertMapToLowercase();
                        if (isCitation(t2)) {
                            cites.add(new Citation(page, rev, t2, getCiteStart(rev, t)));    // location based on outer template
                        }
                    }
                }
            }
        }
        return cites;
    }

    private int getCiteStart(Revision r, Template t) {
        String beforeCite = r.getText().substring(0, t.getStart());
        while (beforeCite.endsWith("{")) {
            beforeCite = beforeCite.substring(0, beforeCite.length()-1);
        }
        int i = beforeCite.lastIndexOf("<ref");
        int j = beforeCite.lastIndexOf("</ref");
        if (i >= 0 && j < 0) {    // "<ref>{{template"
            return i;
        } else if (i >= 0 && j >= 0 && j <= i) { // "</ref>....<ref>{{template"
            return i;
        } else {
            return t.getStart();
        }
    }
}
