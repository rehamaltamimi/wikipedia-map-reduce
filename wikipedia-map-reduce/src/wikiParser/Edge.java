package wikiParser;

public class Edge {

	// Page-Page
	public static final int ART_LINK_ART = 1;

	// User-Page (U to A):
	public static final int USER_EDIT_ART = 3;
	public static final int USER_LINK_ART = 4;
	public static final int USER_MENTION_ART = 5;

        // Page-User (A to U):
	public static final int ART_EDITEDBY_USER = 6;
	public static final int ART_LINKEDBY_USER = 7;
	public static final int ART_MENTIONEDBY_USER = 8;

        // User-User (user A to user B):
	public static final int USER_EDITSTALKOF_USER = 9;
	public static final int USER_LINKSTO_USER = 10;
	public static final int USER_MENTIONS_USER = 11;
	public static final int USER_COEDIT_USER = 12;  // UNUSED!
	public static final int USER_TALKSWITH_USER = 13;
        
	private Vertex one;
	private Vertex two;
	private int type;

	public Edge(Vertex one, Vertex two, int type) {
		this.one = one;
		this.two = two;
		this.type = type;
	}

	public Vertex getOne() {
		return one;
	}

	public Vertex getTwo() {
		return two;
	}

	public int getType() {
		return type;
	}
        
	/**
	 * Returns the edge in the following format:
	 * "destType"+"edgeType"+"dest"
	 * Frex:
	 * "a03Albert_Einstein"
	 * @return coded output string.
	 */
	public String toOutputString() {
		String output = "";
		if (this.two instanceof Page) {
			output += "a";
		}
		else if (this.two instanceof User) {
			output += "u";
		}
		else {
			output += "e";
		}
		if (this.type < 10) {
			output += "0";
		}
		output += this.type;
		output += this.two.toUnderscoredString();
		return output;
	}

}
