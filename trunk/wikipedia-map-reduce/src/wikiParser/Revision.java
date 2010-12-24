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
}
