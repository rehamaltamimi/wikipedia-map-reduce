/*
 * Adaptation of Nathaniel's citation parser code.
 */

package wikiParser.citations;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import wikiParser.Page;
import wikiParser.Revision;
import wikiParser.Template;

/**
 *
 * @author shilad
 */
public class CitationParser {
    
    public boolean isCitation(Template t) {
        String name = t.getName().trim().split(" ")[0].toLowerCase();
        return name.startsWith("cite") || name.startsWith("citation");
    }

    private static final Pattern REF_START = Pattern.compile("<[\\s]*ref[^/]*?>");
    private static final Pattern REF_END = Pattern.compile("(<[\\s]*/[\\s]*ref[\\s]*>)");

    public List<Citation> extractCitations(Page page, Revision revision) {
        List<Citation> cites = new ArrayList<Citation>();
        
        // First, look for citations in templates
        for (Template t : Template.getOneOrMoreTemplates(revision.getText())) {
            cites.addAll(templateToCitations(page, revision, t));
        }

        // Look for citations in REF tags
        String content = revision.getText();
        while (true) {
            Matcher open = REF_START.matcher(content);
            if (!open.find()) {
                break;
            }
            Matcher close = REF_END.matcher(content);
            if (!close.find() || open.end() >= close.start()) {
                System.err.println("No end of reference found in page " + page.getName() +
                        ", revision " + revision.getId() + " at time " + revision.getTimestamp());
                break;
            }
            // we have a beginning and an ending!
            String body = content.substring(open.end(), close.start());
            System.out.println("scanning for refs in " + body);
            cites.addAll(processOneRefTag(page, revision, body));
            content = content.substring(close.end());
        }
        return cites;
    }

    private static final Pattern URL_CONTAINER = Pattern.compile(".*?\\[(.*?)\\].*?");

    private List<Citation> processOneRefTag(Page page, Revision rev, String contents) {
        List<Template> templates = Template.getOneOrMoreTemplates(contents);
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
            url = splitInBrack[0].trim();
            if (url.length() > 0 && url.charAt(0) == '[') {
                String[] tmp = inBrackets.substring(1).split("\\|");
                url = "wiki:" + tmp[0];
            }
            cites.add(new Citation(page, rev, url));
        } else {
            if (templates.size() > 0) {
                cites.add(new Citation(page, rev, templates.get(0)));
            } else if (contents.contains("http")) {
                cites.add(new Citation(page, rev, contents.trim()));
            } else {
                cites.add(new Citation(page, rev, "noURL"));
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
            cites.add(new Citation(page, rev, t));
        } else {
            for (String param : t.getAllParams().keySet()) {
                if (t.paramContainsTemplate(param)) {
                    for (Template t2 : t.getParamAsTemplate(param)) {
                        t2.convertMapToLowercase();
                        if (isCitation(t2)) {
                            cites.add(new Citation(page, rev, t2));
                        }
                    }
                }
            }
        }
        return cites;

    }

}
