package wikiParser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Revision {

	private String id;
	private User contributor;
	private String text;
	private String timestamp;
	private String comment;
	private boolean minor;
	private boolean isVandalism = false;

	public Revision (String id, String timestamp, User contributor, String text, String comment, boolean minor, boolean isVandalism) {
		this.id = id;
		this.contributor = contributor;
		this.text = text;
		this.timestamp = timestamp;
		this.comment = comment;
		this.minor = minor;
		this.isVandalism = isVandalism;
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

	public boolean isIsVandalism() {
		return isVandalism;
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

	public void setIsVandalism(boolean isVandalism) {
		this.isVandalism = isVandalism;
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
        
        private static final Pattern REF_START = Pattern.compile("^<[\\s]*ref[^/]*?>");
        private static final Pattern REF_END = Pattern.compile("(<[\\s]*/[\\s]*ref[\\s]*>)");
        /**
         * Some information may be omitted or overwritten in the following cases:
         * 1. More than one template is included in a single citation and the template field names overlap
         * 2. If the reference is only in <ref> tags and not a template it is likely that only the top level domain 
         * will be returned in params
         * @return list of citations in the revision as templates
         */
        public List<Template> getCites() {
            List<Template> cites = new LinkedList<Template>();
            for (int i = 0; i < text.length() - 4; i++) {
                if (text.substring(i, i + 2).equals("{{")) {
                    int cnt = 2;
                    int j = i + 2;
                    while (cnt > 0 & j < text.length()) {
                        if (text.charAt(j) == '{') {
                            cnt++;
                        } else if (text.charAt(j) == '}') {
                            cnt--;
                        }
                        j++;
                    }
                    String template = text.substring(i + 2, j - 2);
                    if (isCite(template)) {
                        cites.add(Template.processTemplate(template, i+2, j-2));
                    }
                    i = j;
                } else {
                    Matcher startMatcher = REF_START.matcher(text.substring(i));
                    if (startMatcher.find(0)) {
                        Matcher endMatcher = REF_END.matcher(text.substring(i));
                        if (endMatcher.find()) {
                            //Must call this or matcher will return null for start and end
                            int start = endMatcher.start(1);
                            int end = endMatcher.end(1);
                            String ref = text.substring(i+startMatcher.end(), i+start);//need to do something a little smarter with starting index...
                            cites.add(processRef(ref,i+5,i+start+1));
                            i = i+end;
                        } else {
                            System.err.println("No end of reference found in revision " + this.getId() + " at time " + this.getTimestamp());
                            break;
                        }
                    }
                }
            }
            return cites;
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
                    String template = substring.substring(i+2, j-2);
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
            } else if ((name.length() > 3 && name.substring(0,4).equals("cite"))||(name.length() > 7 && name.substring(0,8).equals("citatation"))) {
                return true;
            }
            return false;
        }
        
        private static final Pattern URL_CONTAINER = Pattern.compile(".*?\\[(.*?)\\].*?");
        public Template processRef(String ref, int start, int end) {
            LinkedHashMap<String,String> params = new LinkedHashMap<String,String>();
            String url;
            Matcher urlMatcher = URL_CONTAINER.matcher(ref);
            boolean matches = urlMatcher.matches(); 
            Template t = findTemplateInRef(ref, start);
            //Second condition is necessary in case of dead link templates or similar in the citation
            if (t != null && isCite(t.getParam("templateName"))) {
                return t;
            }
            if (matches) {
                String inBrackets = urlMatcher.group(1);
                String[] splitInBrack = inBrackets.split(" ");
                url = splitInBrack[0].trim();
                if (url.length() > 0 && url.charAt(0) == '[') {
                    String[] tmp = inBrackets.substring(1).split("\\|");
                    url = "wiki:"+ tmp[0];
                }
                for (int i = 1; i < splitInBrack.length; i++) {
                    params.put("inBrackets" + i, "" + splitInBrack[i].trim());
                }
                if (urlMatcher.start(0) > 0) {
                    params.put("otherInfo0", ref.substring(0,urlMatcher.start(0)).trim());
                }
                if (urlMatcher.end(0) < ref.length()) {
                    params.put("otherInfo1", ref.substring(urlMatcher.end(0),ref.length()).trim());
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
            return new Template (start, end, params);
        }
}
