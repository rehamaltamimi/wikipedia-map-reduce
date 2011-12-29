package wikiParser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import wikiParser.citations.Citation;
import wikiParser.citations.CitationParser;

public class Revision {

    private String id;
    private User contributor;
    private String text;
    private String timestamp;
    private String comment;
    private boolean minor;

    public Revision(String id, String timestamp, User contributor, String text, String comment, boolean minor, boolean isVandalism) {
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
        long hash = 0;
        for (int i = 0; i < text.length(); i++) {
            hash = text.charAt(i) + hash * 31;
        }
        return hash;
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

    public boolean getMinor() {
        return minor;
    }

    public boolean isVandalism() {
        if (comment == null) return false;
        String s = comment.toLowerCase();
        return (s.indexOf("rvv") >= 0 || s.indexOf("vandal") >= 0);
    }

    public boolean isRevert() {
        if (comment == null) return false;
        String s = comment.toLowerCase();
        return (s.indexOf("rv") >= 0 || s.indexOf("revert") >= 0);
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
    
    public List<String> getAnchorLinks() {
        return getAnchorLinks(this.text);
    }

    public List<String> getAnchorLinksWithoutFragments() {
        List<String> links = new ArrayList<String>();
        for (String link : getAnchorLinks(this.text)) {
            int i = link.indexOf("#");
            if (i < 0) {
                links.add(link);
            } else {
                links.add(link.substring(0, i));
            }
        }
        return links;
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

    public List<Citation> getCitations(Page page) {
        CitationParser p = new CitationParser();
        return p.extractCitations(page, this);
    }    

    /**
     * Find all templates in a page.
     */
    public List<Template> getTemplates() {
        return Template.getOneOrMoreTemplates(text);
    }

    private static final String DISAMBIGUATION_INDICATORS [] = {
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
    public boolean isDisambiguation() {
        return hasTemplateWithNameContaining(DISAMBIGUATION_INDICATORS);
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

    private boolean hasTemplateWithNameContaining(String identifiers[]) {
        for (Template t : getTemplates()) {
            String n = t.getName().toLowerCase();
            for (String s : identifiers) {
                if (n.contains(s)) {
                    return true;
                }
            }
        }
        return false;
    }
}
