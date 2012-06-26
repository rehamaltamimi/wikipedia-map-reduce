package wmr.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Page extends Vertex {
	
	/**
	 * WikiMedia has moved to including namespace codes in their data dumps explicitly, 
	 * so we can now retrieve namespace from a value rather than parsing the name 
	 * like chumps.
	 */
	public static Map<Integer, String> NAMESPACES = new HashMap<Integer, String>();
	
	
	/**
	 * Default mediawiki namespace, holds encyclopedia articles.
	 */
	public static final int NS_MAIN = 0;
	/**
	 * Default mediawiki namespace, holds talk pages for encyclopedia articles.
	 */
	public static final int NS_TALK = 1;
	/**
	 * Default mediawiki namespace, holds user information pages.
	 */
	public static final int NS_USER = 2;
	/**
	 * Default mediawiki namespace, holds talk pages for users.
	 */
	public static final int NS_USER_TALK = 3;
	/**
	 * Canonically called 'project' in mediawiki, is aliased as 'Wikipedia' or 'WP' 
	 * on wikipedia. Default mediawiki namespace is called 'project'. Holds 
	 * information about wikipedia itself.
	 */
	public static final int NS_PROJECT = 4;
	/**
	 * Canonically called 'project talk' in mediawiki, is aliased as 'Wikipedia_Talk' or 
	 * 'WT' in wikipedia. Default mediawiki namespace is called 'project talk'. Holds 
	 * talk pages for information about wikipedia.
	 */
	public static final int NS_PROJECT_TALK = 5;
	/**
	 * Default mediawiki namespace, holds files. Aliased as 'image' as well on wikipedia.
	 */
	public static final int NS_FILE = 6;
	/**
	 * Default mediawiki namespace, holds talk pages for files. Aliased as 'image talk' 
	 * on wikipedia.
	 */
	public static final int NS_FILE_TALK = 7;
	/**
	 * Default mediawiki namespace, holds information about mediawiki.
	 */
	public static final int NS_MEDIAWIKI = 8;
	/**
	 * Default mediawiki namespace, holds talk pages about information about mediawiki.
	 */
	public static final int NS_MEDIAWIKI_TALK = 9;
	/**
	 * Default mediawiki namespace, holds templates for quick page creation.
	 */
	public static final int NS_TEMPLATE = 10;
	/**
	 * Default mediawiki namespace, holds talk pages for templates for quick page 
	 * creation.
	 */
	public static final int NS_TEMPLATE_TALK = 11;
	/**
	 * Default mediawiki namespace, holds help information for mediawiki and wikipedia.
	 */
	public static final int NS_HELP = 12;
	/**
	 * Default mediawiki namespace, holds talk pages about help information for mediawiki 
	 * and wikipedia.
	 */
	public static final int NS_HELP_TALK = 13;
	/**
	 * Default mediawiki namespace, holds information about categories of pages.
	 */
	public static final int NS_CATEGORY = 14;
	/**
	 * Default mediawiki namespace, holds talk pages for information about categories of pages.
	 */
	public static final int NS_CATEGORY_TALK = 15;
	
	
	/**
	 * Custom wikipedia namespace, holds directories for outside reference materials.
	 */
	public static final int NS_PORTAL = 100;
	/**
	 * Custom wikipedia namespace, holds talk pages for directories for outside 
	 * reference materials.
	 */
	public static final int NS_PORTAL_TALK = 101;
	/**
	 * Custom wikipedia namespace, holds wikibooks.
	 */
	public static final int NS_BOOK = 108;
	/**
	 * Custom wikipedia namespace, holds talk pages for wikibooks.
	 */
	public static final int NS_BOOK_TALK = 109;
	/**
	 * Custom wikipedia namespace, holds pages for courses and course information.
	 */
	public static final int NS_COURSE = 442;
	/**
	 * Custom wikipedia namespace, holds talk pages for courses.
	 */
	public static final int NS_COURSE_TALK = 443;
	/**
	 * Custom wikipedia namespace, holds entries for Wikipedia Education Program 
	 * learning institutions
	 */
	public static final int NS_INSTITUTION = 444;
	/**
	 * Custom wikipedia namespace, holds talk pages for WEP institutions.
	 */
	public static final int NS_INSTITUTION_TALK = 445;
	
	
	/**
	 * Special mediawiki namespace for pages created by the software. 
	 */
	public static final int NS_SPECIAL = -1;
	/**
	 * Special mediawiki namespace for direct links to media content.
	 */
	public static final int NS_MEDIA = -2;
	
	
	static {
		NAMESPACES.put(new Integer(NS_MAIN), "main");
		NAMESPACES.put(new Integer(NS_TALK), "talk");
		NAMESPACES.put(new Integer(NS_USER), "user");
		NAMESPACES.put(new Integer(NS_USER_TALK), "user talk");
		NAMESPACES.put(new Integer(NS_PROJECT), "project");
		NAMESPACES.put(new Integer(NS_PROJECT_TALK), "project talk");
		NAMESPACES.put(new Integer(NS_FILE), "file");
		NAMESPACES.put(new Integer(NS_FILE_TALK), "file talk");
		NAMESPACES.put(new Integer(NS_MEDIAWIKI), "mediawiki");
		NAMESPACES.put(new Integer(NS_MEDIAWIKI_TALK), "mediawiki talk");
		NAMESPACES.put(new Integer(NS_TEMPLATE), "template");
		NAMESPACES.put(new Integer(NS_TEMPLATE_TALK), "template talk");
		NAMESPACES.put(new Integer(NS_HELP), "help");
		NAMESPACES.put(new Integer(NS_HELP_TALK), "help talk");
		NAMESPACES.put(new Integer(NS_CATEGORY), "category");
		NAMESPACES.put(new Integer(NS_CATEGORY_TALK), "category talk");
		
		NAMESPACES.put(new Integer(NS_PORTAL), "portal");
		NAMESPACES.put(new Integer(NS_PORTAL_TALK), "portal talk");
		NAMESPACES.put(new Integer(NS_BOOK), "book");
		NAMESPACES.put(new Integer(NS_BOOK_TALK), "book talk");
		NAMESPACES.put(new Integer(NS_COURSE), "course");
		NAMESPACES.put(new Integer(NS_COURSE_TALK), "course talk");
		NAMESPACES.put(new Integer(NS_INSTITUTION), "institution");
		NAMESPACES.put(new Integer(NS_INSTITUTION_TALK), "institution talk");
		
		NAMESPACES.put(new Integer(NS_SPECIAL), "special");
		NAMESPACES.put(new Integer(NS_MEDIA), "media");
		
	}
	
	protected String namespaceName;
	protected int namespaceIndex;
	
    /* We also check for prefix + " talk"
    public static final String[] NAMESPACE_PREFIXES = {
        "book",
        "category",
        "help",
        "file",
        "mediawiki",
        "portal",
        "project",
        "talk",
        "template",
        "user",
        "wikipedia",
        "wp",
    };

    static {
        Arrays.sort(NAMESPACE_PREFIXES);    // just in case
    } */


    public Page(String id) {
        super(id);
        namespaceIndex = Integer.MIN_VALUE;
        namespaceName = "No namespace indicated.";
    }

    public Page(String name, String id) {
        super(name, id);
        namespaceIndex = Integer.MIN_VALUE;
        namespaceName = "No namespace indicated.";
    }
    
    public Page(String name, String id, String ns) {
    	super(name, id);
    	namespaceIndex = Integer.parseInt(ns);
    	namespaceName = Page.NAMESPACES.get(new Integer(namespaceIndex));
    	
    }
    
    private static Pattern userPagePattern = Pattern.compile("User:(.+)");
    private static Pattern userTalkPagePattern = Pattern.compile("User talk:(.+)");

    /**
     * Generates the user who owns the article in the case of User: and
     * User talk: namespace articles.  Returns a User object with just a username
     * unless the user has also edited the article at some point, in which case
     * it returns a User object with both a username and an UID.
     * @param article the article to determine the owner of
     * @return the owner of the article
     */
    public User getUser() {
        //FIXME: This isn't finding user ids.
        User user = null;
        Matcher nameMatcher = userPagePattern.matcher(getName());
        if (nameMatcher.find()) {
            user = new User(nameMatcher.group(1));
        }
        nameMatcher = userTalkPagePattern.matcher(getName());
        if (nameMatcher.find()) {
            user = new User(nameMatcher.group(1));
        }
        // Looks for a revision by them if there are any revisions, for the ID.
        if (!this.getRevisions().isEmpty()) {
            for (Revision rev : this.getRevisions()) {
                if (rev.getContributor().getName().equals(user.getId())) {
                    user = new User(nameMatcher.group(1), rev.getContributor().getId());
                }
            }
        }
        return user;
    }

    public ArrayList<User> getUsers() {
        ArrayList<User> users = new ArrayList<User>();
        for (Revision r : this.getRevisions()) {
            users.add(r.getContributor());
        }
        return users;
    }

    /**
     * Retrieves a set of all unique anchor tag targets originating from this article.
     * @return a set of unique hyperlink targets
     */
    public Set<String> getAnchorLinks() {
        Set<String> anchorLinks = new HashSet<String>();
        for (Revision r : this.getRevisions()) {
            anchorLinks.addAll(r.getAnchorLinks());
        }
        return anchorLinks;
    }

    /**
     * Retrieves a set of all unique contributors to this article.
     * @return a set of unique contributors
     */
    // FIXME: This isn't adding all, even when they are unique.
    public Set<User> getContributors() {
        Set<User> contributors = new HashSet<User>();
        for (User u : this.getUsers()) {
            contributors.add(u);
        }
        return contributors;
    }

    /*
    public boolean isTalk() {
        return getName().startsWith("Talk:");
    }*/
    
    public boolean isTalk() {
    	return getNamespaceIndex() == NS_TALK;
    }

    /*
    public boolean isUser() {
        return getName().startsWith("User:");
    }*/
    
    public boolean isUser() {
    	return getNamespaceIndex() == NS_USER;
    }

    /*
    public boolean isUserTalk() {
        return getName().startsWith("User talk:");
    }*/
    
    public boolean isUserTalk() {
    	return getNamespaceIndex() == NS_USER_TALK;
    }

    /*
    public boolean isProject() {
        return getName().startsWith("Wikipedia:")
                || getName().startsWith("WP:")
                || getName().startsWith("Project:");
    }*/
    
    public boolean isProject() {
    	return getNamespaceIndex() == NS_PROJECT;
    }

    /*
    public boolean isProjectTalk() {
        return getName().startsWith("Wikipedia talk:")
                || getName().startsWith("WP:")
                || getName().startsWith("Project talk:");
    }*/
    
    public boolean isProjectTalk() {
    	return getNamespaceIndex() == NS_PROJECT_TALK;
    }

    /*
    public boolean isPortal() {
        return getName().startsWith("Portal:");
    }*/
    
    public boolean isPortal() {
    	return getNamespaceIndex() == NS_PORTAL;
    }

    /*
    public boolean isPortalTalk() {
        return getName().startsWith("Portal talk:");
    }*/
    
    public boolean isPortalTalk() {
    	return getNamespaceIndex() == NS_PORTAL_TALK;
    }

    /*
    public boolean isFile() {
        return getName().startsWith("File:")
                || getName().startsWith("Image:");
    }*/
    
    public boolean isFile() {
    	return getNamespaceIndex() == NS_FILE;
    }

    /*
    public boolean isFileTalk() {
        return getName().startsWith("File talk:")
                || getName().startsWith("Image talk:");
    }*/
    
    public boolean isFileTalk() {
    	return getNamespaceIndex() == NS_FILE_TALK;
    }

    /*
    public boolean isMediaWiki() {
        return getName().startsWith("MediaWiki:");
    }*/
    
    public boolean isMediaWiki() {
    	return getNamespaceIndex() == NS_MEDIAWIKI;
    }

    /*
    public boolean isMediaWikiTalk() {
        return getName().startsWith("MediaWiki talk:");
    }*/
    
    public boolean isMediaWikiTalk() {
    	return getNamespaceIndex() == NS_MEDIAWIKI_TALK;
    }

    /*
    public boolean isTemplate() {
        return getName().startsWith("Template:");
    }*/
    
    public boolean isTemplate() {
    	return getNamespaceIndex() == NS_TEMPLATE;
    }

    /*
    public boolean isTemplateTalk() {
        return getName().startsWith("Template talk:");
    }*/
    
    public boolean isTemplateTalk() {
    	return getNamespaceIndex() == NS_TEMPLATE_TALK;
    }

    /*
    public boolean isCategory() {
        return getName().startsWith("Category:");
    }*/
    
    public boolean isCategory() {
    	return getNamespaceIndex() == NS_CATEGORY;
    }

    /*
    public boolean isCategoryTalk() {
        return getName().startsWith("Category talk:");
    }*/
    
    public boolean isCategoryTalk() {
    	return getNamespaceIndex() == NS_CATEGORY_TALK;
    }

    /*
    public boolean isBook() {
        return getName().startsWith("Book:");
    }*/
    
    public boolean isBook() {
    	return getNamespaceIndex() == NS_BOOK;
    }

    /*
    public boolean isBookTalk() {
        return getName().startsWith("Book talk:");
    }*/
    
    public boolean isBookTalk() {
    	return getNamespaceIndex() == NS_BOOK_TALK;
    }

    /*
    public boolean isHelp() {
        return getName().startsWith("Help:");
    }*/
    
    public boolean isHelp() {
    	return getNamespaceIndex() == NS_HELP;
    }

    /*
    public boolean isHelpTalk() {
        return getName().startsWith("Help talk:");
    }*/
    
    public boolean isHelpTalk() {
    	return getNamespaceIndex() == NS_HELP_TALK;
    }

    /*
    public boolean isNormalPage() {
        String ln = getName().toLowerCase();
        int i = ln.indexOf(":");
        if (i < 0) {
            return true;
        }
        String prefix = ln.substring(0, i).trim();
        if (prefix.endsWith(" talk")) {
            prefix = prefix.substring(0, prefix.length() - 5).trim();
        }
        if (Arrays.binarySearch(NAMESPACE_PREFIXES, prefix) >= 0) {
            return false;
        }
        return true;
    }*/
    
    public boolean isNormalPage() {
    	return getNamespaceIndex() == NS_MAIN || getNamespaceIndex() == NS_TALK;
    }

    /*
    public String getNamespace() {
        String ln = getName().toLowerCase();
        int i = ln.indexOf(":");
        if (i < 0) {
            return "main";
        }
        String prefix = ln.substring(0, i).trim();
        boolean isTalk = false;
        if (prefix.endsWith(" talk")) {
            isTalk = true;
            prefix = prefix.substring(0, prefix.length() - 5).trim();
        }
        i = Arrays.binarySearch(NAMESPACE_PREFIXES, prefix);
        if (i < 0 && !isTalk) {
            return "main";
        } else if (i < 0 && isTalk) {
            return "main talk";
        } else if (isTalk) {
            return NAMESPACE_PREFIXES[i] + " talk";
        } else {
            return NAMESPACE_PREFIXES[i];
        }
    }*/
    
    public String getNamespace() {
    	return namespaceName;
    }
    
    public int getNamespaceIndex() {
    	return namespaceIndex;
    }
    
    public static String namespaceFromIndex(int index) {
    	return Page.NAMESPACES.get(new Integer(index));
    }
    
    public static int indexFromNameSpace(String ns) {
    	for (Integer index : Page.NAMESPACES.keySet())
    		if (Page.NAMESPACES.get(index).equals(ns))
    			return index;
    	return -1;
    }
    
    

    
    public boolean isAnyTalk() {
    	switch(getNamespaceIndex()) {
    		case NS_TALK: return true; 
    		case NS_USER_TALK: return true;
    		case NS_PROJECT_TALK: return true;
    		case NS_FILE_TALK: return true;
    		case NS_MEDIAWIKI_TALK: return true;
    		case NS_TEMPLATE_TALK: return true;
    		case NS_HELP_TALK: return true;
    		case NS_CATEGORY_TALK: return true;
    		case NS_PORTAL_TALK: return true;
    		case NS_BOOK_TALK: return true;
    		case NS_COURSE_TALK: return true;
    		case NS_INSTITUTION_TALK: return true;
    		default: return false;
    	}
    }
    
    /*
    public boolean isAnyTalk() {
        String ln = getName().toLowerCase();
        if (ln.startsWith("talk:")) {
            return true;
        } 
        int i = ln.indexOf(" talk:");
        if (i > 0) {
            String prefix = ln.substring(0, i);
            return (Arrays.binarySearch(NAMESPACE_PREFIXES, prefix) >= 0);
        }
        return false;
    }*/
}
