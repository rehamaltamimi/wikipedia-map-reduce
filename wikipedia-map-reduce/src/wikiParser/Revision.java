package wikiParser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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

    

    /**
     * Finds the ending index of a template if one exists within a reference substring. Returns
     * a completed template if one exists within the reference and null if one does not exist.
     * Returns null if there is no template in the reference.
     * @param substring
     * @param beginningIndex
     * @return
     */
    public static Template findTemplateInRef(String substring, int beginningIndex) {
        for (int i = 0; i < substring.length() - 4; i++) {
            if (substring.substring(i, i + 2).equals("{{")) {
                int cnt = 2;
                int j = i + 2;
                while (cnt > 0 && j < substring.length()) {
                    if (substring.charAt(j) == '{') {
                        cnt++;
                    } else if (substring.charAt(j) == '}') {
                        cnt--;
                    }
                    j++;
                }
                String template = substring.substring(i + 2, j - 2);
                try {
                    return Template.processTemplate(template, beginningIndex + i, beginningIndex + j);
                } catch (Exception e) {
                    System.err.println("Incorrectly formatted template caused exception:");
                    e.printStackTrace();
                    return null;
                }
            }
        }
        return null;
    }

    public static boolean isCite(String cite) {
        String name = cite.split("\\|")[0].trim().split(" ")[0].toLowerCase();
        if (name.equals("cite") || name.equals("citation")) {
            return true;
        } else if ((name.length() > 3 && name.substring(0, 4).equals("cite")) || (name.length() > 7 && name.substring(0, 8).equals("citatation"))) {
            return true;
        }
        return false;
    }
    private static final Pattern URL_CONTAINER = Pattern.compile(".*?\\[(.*?)\\].*?");

    public Template processRef(String ref, int start, int end) {
        LinkedHashMap<String, String> params = new LinkedHashMap<String, String>();
        String url;
        Matcher urlMatcher = URL_CONTAINER.matcher(ref);
        boolean matches = urlMatcher.matches();
        Template t = findTemplateInRef(ref, start);
        //Second condition is necessary in case of dead link templates or similar in the citation
        if (t != null && isCite(t.getName())) {
            return t;
        }
        if (matches) {
            String inBrackets = urlMatcher.group(1);
            String[] splitInBrack = inBrackets.split(" ");
            url = splitInBrack[0].trim();
            if (url.length() > 0 && url.charAt(0) == '[') {
                String[] tmp = inBrackets.substring(1).split("\\|");
                url = "wiki:" + tmp[0];
            }
            for (int i = 1; i < splitInBrack.length; i++) {
                params.put("inBrackets" + i, "" + splitInBrack[i].trim());
            }
            if (urlMatcher.start(0) > 0) {
                params.put("otherInfo0", ref.substring(0, urlMatcher.start(0)).trim());
            }
            if (urlMatcher.end(0) < ref.length()) {
                params.put("otherInfo1", ref.substring(urlMatcher.end(0), ref.length()).trim());
            }
        } else {
            if (t != null) {
                return t;
            }
            if (ref.contains("http")) {
                url = ref.trim();
            } else {
                url = "NoURL";
                params.put("otherInfo0", ref);
            }
        }
        params.put("url", url);
        return new Template(start, end, params);
    }
}
