package wikiParser;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Revision {
	/**
	 * @uml.property  name="id"
	 */
	private String id;
	/**
	 * @uml.property  name="contributor"
	 * @uml.associationEnd  multiplicity="(1 1)"
	 */
	private User contributor;
	/**
	 * @uml.property  name="text"
	 */
	private String text;
	/**
	 * @uml.property  name="timestamp"
	 */
	private String timestamp;
	/**
	 * @uml.property  name="comment"
	 */
	private String comment;
	/**
	 * @uml.property  name="minor"
	 */
	private boolean minor;

        /**
         * TODO: setMe properly
         */
        private boolean isVandalism = false;
	
	public Revision (String id, String timestamp, User contributor, String text, String comment, boolean minor) {
		this.id = id;
		this.contributor = contributor;
		this.text = text;
		this.timestamp = timestamp;
		this.comment = comment;
		this.minor = minor;
	}

	/**
	 * @return
	 * @uml.property  name="id"
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * @return
	 * @uml.property  name="contributor"
	 */
	public User getContributor() {
		return contributor;
	}

	/**
	 * @return
	 * @uml.property  name="text"
	 */
	public String getText() {
		return text;
	}

	/**
	 * @return
	 * @uml.property  name="timestamp"
	 */
	public String getTimestamp() {
		return timestamp;
	}
	
	/**
	 * @return
	 * @uml.property  name="comment"
	 */
	public String getComment() {
		return comment;
	}
	
	public boolean getMinor() {
		return minor;
	}

	/**
	 * @param id
	 * @uml.property  name="id"
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * @param contributor
	 * @uml.property  name="contributor"
	 */
	public void setContributor(User contributor) {
		this.contributor = contributor;
	}

	/**
	 * @param text
	 * @uml.property  name="text"
	 */
	public void setText(String text) {
		this.text = text;
	}

	/**
	 * @param timestamp
	 * @uml.property  name="timestamp"
	 */
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	/**
	 * @param comment
	 * @uml.property  name="comment"
	 */
	public void setComment(String comment) {
		this.comment = comment;
	}
	
	/**
	 * @param minor
	 * @uml.property  name="minor"
	 */
	public void setMinor(boolean minor) {
		this.minor = minor;
	}


	public ArrayList<String> getAnchorLinks() {
            return getAnchorLinks(this.text);
        }

        private static final Pattern LINK_PATTERN = Pattern.compile("\\[\\[([^\\]]+?)\\]\\]");
	public static ArrayList<String> getAnchorLinks(String text) {
		ArrayList<String> links = new ArrayList<String>();
		Matcher linkMatcher;
		linkMatcher = LINK_PATTERN.matcher(text);
		while (linkMatcher.find()) {
			String addition = linkMatcher.group(1);
			if (addition.contains("|")) {
				addition = addition.substring(0, addition.indexOf("|"));
			}
			if (!addition.contains("Image:")) {
				links.add(addition);
			}
		}
		return links;
	}

    public boolean isIsVandalism() {
        return isVandalism;
    }

    public void setIsVandalism(boolean isVandalism) {
        this.isVandalism = isVandalism;
    }
}
