package wikiParser;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Revision {

	private String id;
	private User contributor;
	private String text;
	private String timestamp;
	private String comment;
	private boolean minor;

	/**
	 * TODO: set isVandalism properly
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

	public ArrayList<String> getAnchorLinks() {
		return getAnchorLinks(this.text);
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
