package wmr.core;

import wmr.templates.Template;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import wmr.citations.Citation;
import wmr.citations.CitationParser;
import wmr.templates.TemplateParser;
import wmr.util.Utils;

public class Revision {

    private String id;
    private User contributor;
    private String text;
    private String timestamp;
    private String comment;
    private boolean minor;
    private boolean reverted = false;       // is this revision reverted by a later one
    private boolean revert = false;         // does this revision revert an earlier one
    private boolean vandalism = false;      // does this revision contain vandalism

    public Revision(String id, String timestamp, User contributor, String text, String comment, boolean minor) {
        this.id = id;
        this.contributor = contributor;
        this.text = text;
        this.timestamp = timestamp;
        this.comment = comment;
        this.minor = minor;
    }
    public int hashCode() {
            return id.hashCode();
    }

    public boolean equals(Object o) {
            if (o instanceof Revision) {
                    return ((Revision) o).id.equals(id);
            } else {
                    return false;
            }
    }

    public String getId() {
        return id;
    }

    public User getContributor() {
        return contributor;
    }

    public String getText() {
        return text;
    }

    public long getTextFingerprint() {
        return Utils.longHashCode(text);
    }

    public String getTimestamp() {
        return timestamp;
    }
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");

    public Date getTimestampAsDate() {
        try {
            timestamp = timestamp.replace("T", "");
            timestamp = timestamp.replace("Z", "");
            return formatter.parse(timestamp);
        } catch (ParseException ex) {
            Logger.getLogger(Revision.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public String getComment() {
        return comment;
    }

    public boolean isMinor() {
        return minor;
    }

    /**
     * Regexes taken from:
     *  "creating, destroying, restoring" value... (Preidhorsky et al).
     *
     * As coded in:
     * https://bitbucket.org/halfak/wikimedia-utilities/src/1d499dcf52fe/wmf/util.py
     */
    private static final Pattern VLOOSE_RE = Pattern.compile(
          "(^revert\\ to.+using) " +
          "| (^reverted\\ edits\\ by.+using) " +
          "| (^reverted\\ edits\\ by.+to\\ last\\ version\\ by) " +
          "| (^bot\\ -\\ rv.+to\\ last\\ version\\ by) " +
          "| (-assisted\\ reversion) " +
          "| (^(revert(ed)?|rv).+to\\ last) " +
          "| (^undo\\ revision.+by) " +
          "| (\\(\\[\\[WP\\:HG\\|HG\\]\\]\\)) " +
          "| (\\(\\[\\[WP\\:TW\\|TW\\]\\]\\))",
          Pattern.DOTALL | Pattern.COMMENTS | Pattern.CASE_INSENSITIVE);

    
    private static final Pattern VSTRICT_RE = Pattern.compile(
       "   (\\brvv) " +
       " | (\\brv[/ ]v) " +
       " | (vandal(?!proof|bot)) " +
       " | (\\b(rv|rev(ert)?|rm)\\b.*(blank|spam|nonsense|porn|mass\\sdelet|vand)) ",
          Pattern.DOTALL | Pattern.COMMENTS | Pattern.CASE_INSENSITIVE);

    public boolean isVandalismRevert() {
        return isVandalismLooseRevert() || isVandalismStrictRevert();
    }

    public boolean isVandalismLooseRevert() {
        if (revert) return true;
        if (comment == null) return false;
        return VLOOSE_RE.matcher(comment).find() || VSTRICT_RE.matcher(comment).find();
    }
    
    public boolean isVandalismStrictRevert() {
        if (comment == null) return false;
        return VSTRICT_RE.matcher(comment).find();
    }

    public void setRevert(boolean revert) {
        this.revert = revert;
    }
    
    public boolean isRevert() {
        return isVandalismLooseRevert();
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setContributor(User contributor) {
        this.contributor = contributor;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setMinor(boolean minor) {
        this.minor = minor;
    }

    public boolean isReverted() {
        return reverted;
    }

    public void setReverted(boolean isReverted) {
        this.reverted = isReverted;
    }

    public boolean isVandalism() {
        return vandalism;
    }

    public void setVandalism(boolean vandalism) {
        this.vandalism = vandalism;
    }
    
    public List<String> getAnchorLinks() {
        return getAnchorLinks(this.text);
    }

    private static final Pattern ALL_LINK_PATTERN = Pattern.compile("(http[s]*://[^ \\]}<|]+)");
    public static class Hyperlink {
        private String url;
        private int location;
        public Hyperlink(String url, int location) {
            this.url = url;
            this.location = location;
        }
        public String getUrl() { return url; }
        public int getLocation() { return location; }
    }

    public List<Hyperlink> getHyperlinks() {
        List<Hyperlink> links = new ArrayList<Hyperlink>();
        Matcher linkMatcher = ALL_LINK_PATTERN.matcher(text);
        while (linkMatcher.find()) {
            links.add(new Hyperlink(linkMatcher.group(1), linkMatcher.start()));
        }
        return links;
    }

    private static final Pattern SECTION_PATTERN = Pattern.compile("[^=]==([^=]+)==[^=]");
    public Map<String, Integer> getSections() {
        Map<String, Integer> sections = new HashMap<String, Integer>();
        Matcher matcher = SECTION_PATTERN.matcher(text);
        while (matcher.find()) {
            sections.put(matcher.group(1), matcher.start());
        }
        return sections;
    }

    public static List<String> removeFragments(List<String> links) {
        List<String> result = new ArrayList<String>();
        for (String link : links) {
            int i = link.indexOf("#");
            if (i < 0) {
                result.add(link);
            } else {
                result.add(link.substring(0, i));
            }
        }
        return result;
    }
    public List<String> getAnchorLinksWithoutFragments() {
        return removeFragments(getAnchorLinks(text));
    }

    public List<String> getFileLinks() {
        return getFileLinks(text);
    }
    private static final Pattern LINK_PATTERN = Pattern.compile("\\[\\[([^\\]]+?)\\]\\]");

    public static ArrayList<String> getAnchorLinks(String text) {
        ArrayList<String> anchorLinks = new ArrayList<String>();
        Matcher linkMatcher;
        linkMatcher = LINK_PATTERN.matcher(text);
        while (linkMatcher.find()) {
            String addition = linkMatcher.group(1);
            if (addition.contains("|")) {
                addition = addition.substring(0, addition.indexOf("|"));
            }
            if (!addition.contains("Image:")) {
                anchorLinks.add(addition);
            }
        }
        return anchorLinks;
    }

    public static ArrayList<String> getFileLinks(String text) {
        ArrayList<String> anchorLinks = new ArrayList<String>();
        Matcher linkMatcher;
        linkMatcher = LINK_PATTERN.matcher(text);
        while (linkMatcher.find()) {
            String addition = linkMatcher.group(1);
            if (addition.contains("|")) {
                addition = addition.substring(0, addition.indexOf("|"));
            }
            if (addition.contains("Image:") || addition.contains("File:")) {
                anchorLinks.add(addition);
            }
        }
        return anchorLinks;
    }

    private static final Pattern DAB_LINK_PATTERN = Pattern.compile("\\*(?:\")?\\s*\\[\\[([^\\]]+?)\\]\\](?:\")?");

    public List<String> getDisambiguationLinks() {
        ArrayList<String> anchorLinks = new ArrayList<String>();
        Matcher linkMatcher;
        linkMatcher = DAB_LINK_PATTERN.matcher(text);
        while (linkMatcher.find()) {
            String addition = linkMatcher.group(1);
            if (addition.contains("|")) {
                addition = addition.substring(0, addition.indexOf("|"));
            }
            if (!addition.contains("Image:")) {
                anchorLinks.add(addition);
            }
        }
        return anchorLinks;
    }

    public List<String> getDisambiguationLinksWithoutFragments() {
        return removeFragments(getDisambiguationLinks());
    }

    public List<Citation> getCitations(Page page) {
        CitationParser p = new CitationParser();
        return p.extractCitations(page, this);
    }    

    /**
     * Find all templates in a page.
     */
    public List<Template> getTemplates() {
        return TemplateParser.getOneOrMoreTemplates(text);
    }

    private static final String DAB_BLACKLIST [] = {
        "disambiguation",
        "disambig",
        "dab",
        "disamb",
        "dmbox",
        "geodis",
        "numberdis",
        "dmbox",
        "mathdab",
        "hndis",
        "hospitaldis",
        "mathdab",
        "mountainindex",
        "roaddis",
        "schooldis",
        "shipindex"
    };

    private static final String DAB_WHITELIST [] = {
        "dablink",
        "disambiguation needed",
    };
    
    public boolean isDisambiguation() {
        return hasTemplateWithNameContaining(DAB_WHITELIST, DAB_BLACKLIST);
    }

    private static final Pattern REDIRECT_PATTERN = Pattern.compile("#REDIRECT\\s*\\[\\[([^\\]]+)\\]\\]");
    public boolean isRedirect() {
        return REDIRECT_PATTERN.matcher(text).find(0);
    }

    public String getRedirectDestination() {
        Matcher m = REDIRECT_PATTERN.matcher(text);
        if (!m.find(0)) {
            return null;
        } else {
            return m.group(1).trim();
        }
    }

    private boolean hasTemplateWithNameContaining(String whitelist[], String identifiers[]) {
        for (Template t : getTemplates()) {
            String n = t.getName().toLowerCase();
            if (stringContainsOneOf(n, whitelist)) {
                continue;
            }
            if (stringContainsOneOf(n, identifiers)) {
                return true;
            }
        }
        return false;
    }

    private static boolean stringContainsOneOf(String s, String substrs[]) {
        for (String ss : substrs) {
            if (s.startsWith(ss) || s.contains(" " + ss)) {
                return true;
            }
        }
        return false;
    }
}
